import gzip
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

EXPORT = Path(__file__).resolve().parents[1] / "downloaders" / "export_archival_indices.py"


def records(root, revision, catalog):
    base = root / "staged" / "archival-spectroscopy" / f"r{revision:019d}" / catalog
    return [json.loads(row) for part in sorted(base.glob("part-*.jsonl.gz"))
            for row in gzip.open(part, "rt", encoding="utf-8")]


def test_a_second_release_updates_a_waiting_object(tmp_path):
    db_path = tmp_path / "index.sqlite"
    out = tmp_path / "data"
    with sqlite3.connect(db_path) as db:
        db.executescript("""
            CREATE TABLE object(id TEXT PRIMARY KEY, gaia TEXT, name TEXT, ra REAL, dec REAL, candidate TEXT);
            CREATE TABLE observation(id TEXT PRIMARY KEY, object_id TEXT, visit TEXT, mjd REAL,
                survey TEXT, release TEXT, product TEXT, locator TEXT, details TEXT, quality TEXT);
            INSERT INTO object VALUES('gaia-dr3:123','123','Known candidate',25.,30.,'{"lists":["eclipsing"]}');
            INSERT INTO object VALUES('gaia-dr3:456','456','Other candidate',45.,50.,NULL);
        """)
        subprocess.run([sys.executable, str(EXPORT), str(db_path), str(out), "--revision", "1"], check=True)
        before = records(out, 1, "archival-spectrum-objects")
        assert [r["spectrum_records"] for r in before] == [0, 0]

        db.execute("INSERT INTO observation VALUES(?,?,?,?,?,?,?,?,?,?)", (
            "desi:dr1-iron:exposure:1", "gaia-dr3:123", "desi:exposure:96", 60600.,
            "desi", "dr1-iron", "exposure", '{"format":"desi","url":"https://archive.example/one.fits"}',
            None, None))
        db.commit()
        changed = tmp_path / "changed.jsonl"
        changed.write_text('{"_id":"desi:dr1-iron:exposure:1"}\n')
        gains = tmp_path / "gains.json"
        gains.write_text(json.dumps({"database_snapshot_utc": "2026-11-27T23:00:00Z", "objects": [{
            "object_id": "gaia-dr3:123",
            "first_linked_spectrum": {"id": "desi:dr1-iron:exposure:1"},
            "new_usable_rv_nights": [{"id": "desi:dr1-iron:exposure:1", "night_mjd": 60600}],
            "new_useful_phase_visits": [],
        }]}))
        subprocess.run([sys.executable, str(EXPORT), str(db_path), str(out), "--revision", "2",
                        "--changed-visits", str(changed), "--gains", str(gains)], check=True)
    [updated] = records(out, 2, "archival-spectrum-objects")
    [visit] = records(out, 2, "archival-spectrum-visits")
    assert updated["_id"] == before[0]["_id"]
    assert updated["independent_visits"] == 1
    assert updated["gains"]["new_rv_nights"] == 1
    assert visit["object_id"] == updated["_id"]
    assert visit["watchlist_gain"]["first_linked_spectrum"]
    assert visit["locator"]["format"] == "desi"
