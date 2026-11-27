"""Stage appendable BOOM spectroscopy index chunks from a local inventory snapshot."""

import argparse
from contextlib import closing
import gzip
import io
import itertools
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile

OBJECTS = "archival-spectrum-objects"
VISITS = "archival-spectrum-visits"


def read_jsonl(path):
    with (gzip.open if path.suffix == ".gz" else open)(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def apply_selection(db, changed_visits, changed_objects):
    """A change set carries stable IDs; the current snapshot supplies complete rows."""
    db.executescript("""
        CREATE TEMP TABLE selected_object(id TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE selected_visit(id TEXT PRIMARY KEY) WITHOUT ROWID;
    """)
    for file in changed_visits:
        db.executemany("INSERT OR IGNORE INTO selected_visit VALUES (?)",
                       ((row["_id"],) for row in read_jsonl(file)))
    for file in changed_objects:
        db.executemany("INSERT OR IGNORE INTO selected_object VALUES (?)",
                       ((row["object_id"],) for row in read_jsonl(file)))
    if db.execute("""SELECT 1 FROM selected_visit s
                WHERE NOT EXISTS (SELECT 1 FROM observation a WHERE a.id=s.id) LIMIT 1""").fetchone():
        raise ValueError("The snapshot is missing at least one selected visit")
    db.execute("""INSERT OR IGNORE INTO selected_object
                  SELECT a.object_id FROM observation a JOIN selected_visit s ON a.id=s.id""")
    if db.execute("""SELECT 1 FROM selected_object s
                WHERE NOT EXISTS (SELECT 1 FROM object a WHERE a.id=s.id) LIMIT 1""").fetchone():
        raise ValueError("The snapshot is missing at least one selected object")


def gain_summary(path):
    if path is None:
        return {}, {}
    with path.open(encoding="utf-8") as handle:
        report = json.load(handle)
    objects = {
        item["object_id"]: {
            "compared_at_utc": report["database_snapshot_utc"],
            "first_spectrum_id": (item.get("first_linked_spectrum") or {}).get("id"),
            "new_rv_nights": len(item.get("new_usable_rv_nights", [])),
            "new_phase_visits": len(item.get("new_useful_phase_visits", [])),
        }
        for item in report["objects"]
    }
    visits = {}
    for item in report["objects"]:
        if first := item.get("first_linked_spectrum"):
            visits.setdefault(first["id"], {})["first_linked_spectrum"] = True
        for rv in item.get("new_usable_rv_nights", []):
            visits.setdefault(rv["id"], {})["new_rv_night"] = rv["night_mjd"]
        for phase in item.get("new_useful_phase_visits", []):
            visits.setdefault(phase["id"], {})["new_phase"] = {
                key: phase[key] for key in ("phase", "phase_reference", "phase_error_cycles_approximate",
                                           "distance_from_prebackfill_phase")
            }
    return objects, visits


def visit_rows(db, revision, delta, gains):
    selection = "JOIN selected_visit s ON s.id=a.id" if delta else ""
    query = f"""SELECT a.*,o.ra,o.dec FROM observation a JOIN object o ON o.id=a.object_id
                {selection} ORDER BY a.id"""
    for row in db.execute(query):
        result = dict(row)
        result["_id"] = result.pop("id")
        result["revision"] = revision
        for field in ("locator", "details", "quality"):
            result[field] = json.loads(result[field]) if result[field] else None
        result["watchlist_gain"] = gains.get(result["_id"])
        yield {k: v for k, v in result.items() if v is not None}


def object_rows(db, revision, delta, gains):
    selection = "JOIN selected_object s ON s.id=o.id" if delta else ""
    query = f"""SELECT o.*,
        (SELECT count(*) FROM observation a WHERE a.object_id=o.id) AS spectrum_records,
        (SELECT count(DISTINCT a.visit) FROM observation a WHERE a.object_id=o.id) AS independent_visits
        FROM object o {selection} ORDER BY o.id"""
    for row in db.execute(query):
        target = json.loads(row["candidate"]) if row["candidate"] else {}
        result = {"_id": row["id"], "revision": revision, "gaia_dr3_source_id": row["gaia"],
                  "name": row["name"], "ra": row["ra"], "dec": row["dec"],
                  "spectrum_records": row["spectrum_records"], "independent_visits": row["independent_visits"],
                  "lists": target.get("lists", []), "review_status": target.get("review_status"),
                  "next_spectrum_test": target.get("what_next_spectrum_can_test"),
                  "gains": gains.get(row["id"])}
        yield {k: v for k, v in result.items() if v is not None}


def stage(directory, rows, size):
    directory.mkdir(parents=True, exist_ok=True)
    tally = 0
    rows = iter(rows)
    for index in itertools.count():
        batch = list(itertools.islice(rows, size))
        if not batch:
            break
        path = directory / f"part-{index:06d}.jsonl.gz"
        with path.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped:
            with io.TextIOWrapper(zipped, encoding="utf-8") as handle:
                for row in batch:
                    handle.write(json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n")
        tally += len(batch)
    return tally


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("catalog_data_path", type=Path)
    parser.add_argument("--revision", required=True, type=int, help="Increasing signed 64-bit revision")
    parser.add_argument("--rows-per-shard", type=int, default=50000)
    parser.add_argument("--gains", type=Path, help="Optional watchlist gain report for this snapshot")
    parser.add_argument("--changed-visits", action="append", type=Path, default=[],
                        help="Normalized JSONL with observation _id keys")
    parser.add_argument("--changed-objects", action="append", type=Path, default=[],
                        help="JSONL with object_id keys, including zero-spectrum candidates")
    args = parser.parse_args()
    if not 0 <= args.revision < 2**63 or not 1 <= args.rows_per_shard <= 1_000_000:
        parser.error("Use a nonnegative signed 64-bit revision and a positive shard size")
    staged = args.catalog_data_path / "staged" / "archival-spectroscopy"
    staged.mkdir(parents=True, exist_ok=True)
    destination = staged / f"r{args.revision:019d}"
    delta = bool(args.changed_visits or args.changed_objects)
    if destination.exists():
        parser.error(f"Revision {args.revision} has already been staged")
    temporary = Path(tempfile.mkdtemp(prefix=".incoming-", dir=staged))
    try:
        with closing(sqlite3.connect(f"{args.database.resolve().as_uri()}?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            if delta:
                apply_selection(db, args.changed_visits, args.changed_objects)
            db.commit()
            db.execute("BEGIN")
            objects, visits = gain_summary(args.gains)
            counts = {
                VISITS: stage(temporary / VISITS, visit_rows(db, args.revision, delta, visits), args.rows_per_shard),
                OBJECTS: stage(temporary / OBJECTS,
                               object_rows(db, args.revision, delta, objects), args.rows_per_shard),
            }
        if any(counts.values()):
            temporary.rename(destination)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    print(json.dumps(counts))


if __name__ == "__main__":
    main()
