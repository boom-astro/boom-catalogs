"""Rank repeat radial-velocity candidates from a completed archive backfill."""

import argparse
import gzip
import itertools
import json
import math
import sqlite3
import tempfile
from collections import Counter
from contextlib import closing
from dataclasses import asdict
from pathlib import Path

if __package__:
    from .cross_pipeline_rv import best_cross_pair, calibration_id, fit_calibration, load_calibrations
    from .fourmost_rv import PRODUCTS as FOURMOST_FLOORS, read_visits as fourmost_visits
else:
    from cross_pipeline_rv import best_cross_pair, calibration_id, fit_calibration, load_calibrations
    from fourmost_rv import PRODUCTS as FOURMOST_FLOORS, read_visits as fourmost_visits

# Conservative per-visit floors in km/s for discovery ranking. Different
# pipelines are compared separately until their zero points are calibrated.
FLOORS = {
    ("lamost", "lrs", "LASP"): 10.0,
    ("lamost", "mrs", "LAMOST_MRS_BR1"): 3.0,
    ("sdss", "boss", "BOSSNet"): 10.0,
    ("sdss", "apogee", "APOGEE_VRAD_VRELERR"): 1.0,
    ("sdss", "apogee", "APOGEE_VHELIO_VRELERR"): 1.0,
    ("desi", "exposure", "RVSpecFit"): 5.0,
}


def flag(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_clear(value):
    return str(value).lower() in {"false", "0"}


def quality_passes(row):
    quality = row.get("quality") or {}
    match row["survey"], row["product"]:
        case "fourmost", _:
            return quality.get("accepted") is True and row["rv"].get("frame") == "barycentric"
        case "desi", "exposure":
            return flag(quality.get("rvs_warn")) == flag(quality.get("fiberstatus")) == 0
        case "lamost", "lrs":
            return flag(quality.get("fibermask")) == 0
        case "lamost", "mrs":
            return (flag(quality.get("rv_br_flag")) == 0
                    and flag(quality.get("fibermask")) in {0, -9999}
                    and flag(quality.get("bad_b")) == flag(quality.get("bad_r")) == 0)
        case "sdss", "boss":
            return is_clear(quality.get("flag_bad")) and is_clear(quality.get("flag_warn"))
        case "sdss", "apogee":
            starflag = flag(quality.get("starflag"))
            rvflag = flag(quality.get("rv_flag"))
            star_bad = sum(1 << bit for bit in (0, 3, 4, 18, 19, 22))
            rv_bad = sum(1 << bit for bit in (4, 5, 6, 10, 11))
            return (starflag is not None and starflag >= 0 and not starflag & star_bad
                    and (rvflag is None or rvflag >= 0 and not rvflag & rv_bad))
        case _:
            return False


def number(value):
    return isinstance(value, (int, float)) and math.isfinite(value)


def identifier(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value and value not in {"0", "-1", "-999", "-9999", "None"} else None


def normalize(row, min_snr):
    velocity = row.get("rv")
    if not velocity:
        return None
    survey, product, method = row["survey"], row["product"], velocity["method"]
    floor = FOURMOST_FLOORS.get(product) if survey == "fourmost" else FLOORS.get((survey, product, method))
    if floor is None or not quality_passes(row):
        return None
    mjd, snr = row.get("mjd"), row.get("snr")
    value, error = velocity["value_km_s"], velocity["error_km_s"]
    if not all(map(number, (mjd, snr, value, error))) or error <= 0 or snr < min_snr:
        return None
    if not (0 <= row["ra"] < 360 and -90 <= row["dec"] <= 90):
        return None

    native_id = identifier(row.get("archive_source_id"))
    native = f"{survey}:{product}:{native_id}" if native_id else None
    gaia_id = identifier(row.get("gaia_dr3_source_id"))
    gaia = f"gaia-dr3:{gaia_id}" if gaia_id and gaia_id.isdecimal() else None
    if native is None and gaia is None:
        return None

    epoch = row["_id"]
    family = epoch
    coadd = False
    if survey == "lamost" and product == "mrs":
        epoch, family = row["epoch_id"], row["visit_id"]
        coadd = row["granularity"] == "visit_coadd"
        if not coadd:
            # LAMOST's LMJM is in local time (UTC+8), with one-minute resolution.
            mjd = int(epoch.rsplit(":", 1)[-1]) / 1440 - 8 / 24
    if survey in {"desi", "fourmost"}:
        epoch = str(row["exposure_id"])
        family = epoch
    pipeline = ":".join((survey, row["release"], product, method))
    visit = {
        "id": row["_id"], "pipeline": pipeline, "ra": row["ra"], "dec": row["dec"],
        "mjd": mjd, "snr": snr, "rv_km_s": value, "rv_error_km_s": error,
        "exptime_s": row.get("exptime_s"), "rv_frame": velocity.get("frame"),
        "effective_error_km_s": math.hypot(error, floor), "spectrum": row.get("spectrum"),
    }
    return (row["_id"], native, gaia, pipeline, family, epoch, int(coadd),
            visit["effective_error_km_s"], snr, json.dumps(visit, allow_nan=False))


def metadata_rows(manifest_path, footprint):
    if footprint:
        with gzip.open(footprint, "rt") as stream:
            yield from map(json.loads, stream)
        return
    if manifest_path is None:
        return
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("format_version") != 1 or "jobs" not in manifest:
        raise ValueError("Expected a completed archive import manifest")
    root = manifest_path.parent.resolve()
    expected = 0
    for job in manifest["jobs"]:
        for shard in job["shards"]:
            path = (root / shard["path"]).resolve()
            if not path.is_relative_to(root) or path.stat().st_size != shard["bytes"]:
                raise ValueError(f"Missing or changed archive shard {path}")
            with gzip.open(path, "rt") as stream:
                count = 0
                for count, line in enumerate(stream, start=1):
                    yield json.loads(line)
            if count != shard["records"]:
                raise ValueError(f"Record count changed in archive shard {path}")
            expected += count
    if expected != manifest["records"]:
        raise ValueError("Archive manifest total does not match its shards")


def ranked_pair(visits, min_delta, min_sigma, min_baseline):
    best = None
    for first, second in itertools.combinations(visits, 2):
        baseline = abs(second["mjd"] - first["mjd"])
        delta = abs(second["rv_km_s"] - first["rv_km_s"])
        if baseline < min_baseline or delta < min_delta:
            continue
        sigma = delta / math.hypot(first["effective_error_km_s"], second["effective_error_km_s"])
        rank = (sigma, delta, baseline)
        if sigma >= min_sigma and (best is None or rank > best[0]):
            best = rank, tuple(sorted((first, second), key=lambda visit: (visit["mjd"], visit["id"])))
    return best


def independent_visits(rows):
    for _, family in itertools.groupby(rows, key=lambda row: row[2]):
        exposures = []
        coadd = None
        for _, equivalents in itertools.groupby(family, key=lambda row: row[3]):
            # SQL sorts the best-error measurement first within an epoch.
            _, _, _, _, is_coadd, payload = next(equivalents)
            if is_coadd:
                coadd = json.loads(payload)
            else:
                exposures.append(json.loads(payload))
        yield from exposures or ([coadd] if coadd else [])


def visit_groups(db):
    db.executescript("""
        CREATE TABLE aliases AS
            SELECT native, MIN(gaia) AS gaia, COUNT(DISTINCT gaia) AS n
            FROM visits WHERE native IS NOT NULL AND gaia IS NOT NULL GROUP BY native;
        CREATE UNIQUE INDEX aliases_native ON aliases(native);
    """)
    rows = db.execute("""
        SELECT COALESCE(v.gaia, a.gaia, v.native) AS identity, v.pipeline, v.family,
               v.epoch, v.coadd, v.payload
        FROM visits v LEFT JOIN aliases a ON v.native = a.native
        WHERE a.n IS NULL OR a.n = 1
        ORDER BY identity, v.pipeline, v.family, v.epoch, v.error, v.snr DESC, v.id
    """)
    for identity, source in itertools.groupby(rows, key=lambda row: row[0]):
        yield identity, {
            pipeline: list(independent_visits(group))
            for pipeline, group in itertools.groupby(source, key=lambda row: row[1])
        }


def candidates(groups, args, stats, calibrations):
    thresholds = args.min_delta_km_s, args.min_sigma, args.min_baseline_days
    for identity, pipelines in groups:
        for pipeline, visits in pipelines.items():
            if len(visits) < 2:
                continue
            stats["source_pipelines_with_repeats"] += 1
            pair = ranked_pair(visits, *thresholds)
            if pair is None:
                continue
            (sigma, delta, baseline), (first, second) = pair
            stats["candidates"] += 1
            yield candidate(identity, pipeline, len(visits), sigma, delta, baseline, first, second)
        if not calibrations or not identity.startswith("gaia-dr3:"):
            continue
        for (pipeline_a, pipeline_b), (calibration, fit_id, min_snr) in calibrations.items():
            if pipeline_a not in pipelines or pipeline_b not in pipelines:
                continue
            a, b = pipelines[pipeline_a], pipelines[pipeline_b]
            a = [visit for visit in a if visit["rv_frame"] == calibration.frame_a and visit["snr"] >= min_snr]
            b = [visit for visit in b if visit["rv_frame"] == calibration.frame_b and visit["snr"] >= min_snr]
            if not a or not b:
                continue
            stats["cross_pipeline_source_pairs"] += 1
            best = best_cross_pair(a, b, calibration, *thresholds)
            if best is None:
                continue
            (sigma, delta, baseline), first, second, residual, uncertainty = best
            stats["cross_pipeline_candidates"] += 1
            row = candidate(identity, pipeline_a, len(a) + len(b), sigma, delta, baseline, first, second)
            row.update(
                _id=f"{identity}:cross:{pipeline_a}:{pipeline_b}:{fit_id}",
                comparison="cross_pipeline",
                cross_pipeline={
                    "calibration_id": fit_id,
                    "pipeline_b": pipeline_b,
                    "raw_b_minus_a_km_s": second["rv_km_s"] - first["rv_km_s"],
                    "corrected_b_minus_a_km_s": residual,
                    "corrected_error_km_s": uncertainty,
                    "offset_b_minus_a_km_s": calibration.offset_b_minus_a_km_s,
                    "offset_error_km_s": calibration.offset_error_km_s,
                    "scatter_km_s": calibration.scatter_km_s,
                },
            )
            yield row


def candidate(identity, pipeline, count, sigma, delta, baseline, first, second):
    survey, release, product, method = pipeline.split(":")
    return {
        "_id": f"{identity}:{pipeline}", "source_id": identity, "survey": survey,
        "release": release, "product": product, "method": method,
        "ra": first["ra"], "dec": first["dec"], "n_visits": count,
        "delta_km_s": delta, "pair_sigma": sigma, "baseline_days": baseline,
        "comparison": "same_pipeline", "best_pair": [first, second],
    }


def search(args):
    stats = Counter(input_rows=0, usable_rv_rows=0, candidates=0, source_pipelines_with_repeats=0)
    calibrations = load_calibrations(getattr(args, "cross_pipeline_calibration", None))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=args.scratch_dir) as directory, closing(
        sqlite3.connect(Path(directory) / "search.sqlite")
    ) as db:
        db.executescript("""
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=FILE;
            CREATE TABLE visits (
                id TEXT PRIMARY KEY, native TEXT, gaia TEXT, pipeline TEXT, family TEXT,
                epoch TEXT, coadd INTEGER, error REAL, snr REAL, payload TEXT
            );
        """)

        def usable():
            visits = itertools.chain(metadata_rows(args.manifest, args.footprint),
                                     *(fourmost_visits(path) for path in getattr(args, "fourmost_visits", [])))
            for row in visits:
                stats["input_rows"] += 1
                if args.survey and row["survey"] not in args.survey:
                    continue
                visit = normalize(row, args.min_snr)
                if visit is not None:
                    yield visit

        db.executemany("INSERT OR IGNORE INTO visits VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", usable())
        stats["usable_rv_rows"] = db.total_changes
        groups = visit_groups(db)
        if pair := getattr(args, "fit_cross_pipeline", None):
            control_ids = (set(args.control_ids.read_text().split()) if args.control_ids else None)
            calibration, validation = fit_calibration(
                groups, pair, args.rv_frames, args.min_training_controls,
                args.min_holdout_controls, args.control_max_span_km_s, control_ids,
            )
            validation["min_snr"] = args.min_snr
            payload = {"calibration": asdict(calibration), "validation": validation}
            payload["id"] = calibration_id(payload)
            with tempfile.NamedTemporaryFile(mode="w", dir=args.output.parent, delete=False) as stream:
                json.dump({"version": 1, "pairs": [payload]}, stream, indent=2)
                stream.write("\n")
                stream.close()
                Path(stream.name).replace(args.output)
            return validation
        if calibrations:
            stats.update(cross_pipeline_source_pairs=0, cross_pipeline_candidates=0)
        db.execute("CREATE TABLE ranked (id TEXT, grouping TEXT, sigma REAL, delta REAL, payload TEXT)")
        db.executemany("INSERT INTO ranked VALUES (?, ?, ?, ?, ?)", (
            (row["_id"], row["comparison"], row["pair_sigma"], row["delta_km_s"],
             json.dumps(row, allow_nan=False))
            for row in candidates(groups, args, stats, calibrations)
        ))
        ordered = db.execute("SELECT payload FROM ranked ORDER BY grouping DESC, sigma DESC, delta DESC, id")
        with tempfile.NamedTemporaryFile(dir=args.output.parent, delete=False) as stream:
            partial = Path(stream.name)
            try:
                with gzip.GzipFile(fileobj=stream, filename="", mode="wb", mtime=0) as output:
                    for (payload,) in ordered:
                        output.write((payload + "\n").encode())
                stream.close()
                partial.replace(args.output)
            finally:
                partial.unlink(missing_ok=True)
    return dict(stats)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--manifest", type=Path, help="Completed --all importer manifest")
    inputs.add_argument("--footprint", type=Path, help="Compressed test footprint metadata")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scratch-dir", type=Path)
    parser.add_argument("--survey", choices=("lamost", "sdss", "desi", "fourmost"), action="append")
    parser.add_argument("--fourmost-visits", type=Path, action="append", default=[],
                        help="Normalized public 4MOST visit CSV, standalone or alongside an archive input")
    cross = parser.add_mutually_exclusive_group()
    cross.add_argument("--cross-pipeline-calibration", type=Path, help="Apply a versioned calibration artifact")
    cross.add_argument("--fit-cross-pipeline", nargs=2, metavar=("PIPELINE_A", "PIPELINE_B"),
                       help="Fit a calibration to quiet stars and write JSON instead of candidates")
    parser.add_argument("--rv-frames", nargs=2, metavar=("FRAME_A", "FRAME_B"))
    parser.add_argument("--control-ids", type=Path, help="Optional list of Gaia DR3 IDs to use for calibration")
    parser.add_argument("--min-training-controls", type=int, default=30)
    parser.add_argument("--min-holdout-controls", type=int, default=10)
    parser.add_argument("--control-max-span-km-s", type=float, default=20.0)
    parser.add_argument("--min-delta-km-s", type=float, default=30.0)
    parser.add_argument("--min-sigma", type=float, default=5.0)
    parser.add_argument("--min-snr", type=float, default=5.0)
    parser.add_argument("--min-baseline-days", type=float, default=0.0)
    args = parser.parse_args()
    if not (args.manifest or args.footprint or args.fourmost_visits):
        parser.error("Provide --manifest, --footprint, or --fourmost-visits")
    thresholds = (args.min_delta_km_s, args.min_sigma, args.min_snr, args.min_baseline_days)
    if not all(math.isfinite(value) and value >= 0 for value in thresholds):
        parser.error("Thresholds must be finite and non-negative")
    if args.fit_cross_pipeline:
        if not args.rv_frames:
            parser.error("--fit-cross-pipeline requires --rv-frames")
        if (args.min_training_controls < 3 or args.min_holdout_controls < 3
                or not math.isfinite(args.control_max_span_km_s) or args.control_max_span_km_s <= 0):
            parser.error("Calibration needs at least three stars in each split and a positive finite control span")
    elif args.rv_frames or args.control_ids:
        parser.error("--rv-frames and --control-ids require --fit-cross-pipeline")
    if args.scratch_dir:
        args.scratch_dir.mkdir(parents=True, exist_ok=True)
    print(json.dumps(search(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
