"""Bulk input adapters and resumable output for the archival spectra importer."""

import csv
import fcntl
import gzip
import itertools
import json
import os
import re
import shutil
import sqlite3
import tempfile
import sys
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import closing, contextmanager
from pathlib import Path

from astropy.io import fits

if __package__:
    from . import import_archival_spectra as spectra
else:
    import import_archival_spectra as spectra


LAMOST = f"{spectra.LAMOST}/dr12/v2.0"
SDSS = spectra.SDSS
SDSS17 = "https://dr17.sdss.org/sas/dr17"
INPUTS = {
    "lamost/lrs": f"{LAMOST}/catdl?name=dr12_v2.0_LRS_catalogue.csv.gz",
    "lamost/stellar": f"{LAMOST}/catdl?name=dr12_v2.0_LRS_stellar.csv.gz",
    "lamost/mrs": f"{LAMOST}/medcas/catdl?name=dr12_v2.0_MRS_catalogue.csv.gz",
    "sdss/boss": f"{SDSS}/spectro/boss/redux/v6_2_1/summary/daily/spAll-v6_2_1.fits.gz",
    "sdss/bossnet": f"{SDSS}/spectro/astra/0.8.1/summary/astraAllVisitBossNet-0.8.1.fits.gz",
    "sdss/legacy": f"{SDSS17}/sdss/spectro/redux/specObj-dr17.fits",
    "sdss/apogee": f"{SDSS}/spectro/apogee/redux/1.3/summary/allVisit-1.3-apo25m.fits",
    "sdss/apogee17": f"{SDSS17}/apogee/spectro/aspcap/dr17/synspec_rev1/allVisit-dr17-synspec_rev1.fits",
}
DESI_INDEX = f"{spectra.DESI}/healpix/tilepix.fits"
SHARD_SIZE = 50000
COPY_BUFFER = 1024 * 1024
FORMAT_VERSION = 1


def filename(url):
    return url.rsplit("name=", 1)[-1].rsplit("/", 1)[-1]


def download(client, url, directory):
    path = directory / filename(url)
    if path.exists():
        return path
    directory.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    offset = partial.stat().st_size if partial.exists() else 0
    headers = {"Accept-Encoding": "identity"}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    with client.get(url, stream=True, headers=headers, timeout=(15, 90)) as response:
        if response.status_code == 416 and response.headers.get("Content-Range") == f"bytes */{offset}" and offset:
            partial.replace(path)
            return path
        response.raise_for_status()
        if response.status_code == 206:
            interval = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
            if not interval or int(interval[1]) != offset or int(interval[2]) + 1 != int(interval[3]):
                raise ValueError(f"Unexpected incomplete range for {url}")
            total = int(interval[3])
            mode = "ab"
        else:
            total = int(response.headers["Content-Length"]) if "Content-Length" in response.headers else None
            mode = "wb"
        with partial.open(mode) as output:
            for block in response.iter_content(COPY_BUFFER):
                output.write(block)
    if total is not None and partial.stat().st_size != total:
        raise IOError(f"Incomplete download of {url}")
    partial.replace(path)
    return path


def csv_records(path, columns):
    with gzip.open(path, "rt", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or not set(columns) <= set(reader.fieldnames):
            raise ValueError(f"{path.name} lacks required columns {sorted(set(columns) - set(reader.fieldnames or []))}")
        for row in reader:
            yield {key: (row[key] or "").strip() for key in columns}


@contextmanager
def fits_table(path):
    # A compressed FITS file cannot be memory-mapped. Unpack to a reusable file
    # on disk, then read each row's selected fields from the mmap.
    if path.suffix == ".gz":
        unpacked = path.with_suffix("")
        if not unpacked.exists():
            temporary = unpacked.with_suffix(".fits.part")
            with gzip.open(path, "rb") as source, temporary.open("wb") as target:
                shutil.copyfileobj(source, target, COPY_BUFFER)
            temporary.replace(unpacked)
        path = unpacked
    with fits.open(path, memmap=True) as hdus:
        yield hdus[1].data


def string(value):
    return value.decode("ascii").strip() if isinstance(value, bytes) else str(value).strip()


def fits_records(path, columns):
    with fits_table(path) as table:
        names = {key.lower(): key for key in table.names}
        missing = set(columns) - names.keys()
        if missing:
            raise ValueError(f"{path.name} lacks required columns {sorted(missing)}")
        # Access selected columns in blocks, avoiding a full-column conversion
        # for scaled unsigned FITS types or string columns.
        keys = [(key, names[key]) for key in columns]
        for start in range(0, len(table), 10000):
            block = table[start:start + 10000]
            values = [block[name] for _, name in keys]
            for row in zip(*values, strict=True):
                yield dict(zip(columns, map(string, row), strict=True))


def lookup(path, pairs):
    if not path.exists():
        temporary = path.with_suffix(".sqlite.part")
        temporary.unlink(missing_ok=True)
        with closing(sqlite3.connect(temporary)) as db, db:
            db.execute("CREATE TABLE records (id TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID")
            db.executemany("INSERT OR REPLACE INTO records VALUES (?, ?)",
                           ((key, json.dumps(value, allow_nan=False)) for key, value in pairs))
        temporary.replace(path)
    return sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)


def get(db, key):
    match = db.execute("SELECT value FROM records WHERE id = ?", (key,)).fetchone()
    return json.loads(match[0]) if match else None


def valid_position(row):
    ra, dec = spectra.floating(row["ra"]), spectra.floating(row["dec"])
    return ra is not None and dec is not None and 0 <= ra < 360 and -90 <= dec <= 90


def lamost_lrs(path, stellar, cache):
    columns = ("obsid", "ra", "dec", "mjd", "snrr", "class", "fibermask", "gaia_source_id")
    estimates = ((row["obsid"], spectra.rv(row["rv"], row["rv_err"], "LASP", "survey_native"))
                 for row in csv_records(stellar, ("obsid", "rv", "rv_err")))
    with closing(lookup(cache / "lamost-lrs-stellar.sqlite", estimates)) as db:
        for row in csv_records(path, columns):
            if valid_position(row):
                yield spectra.lamost_lrs_record({**row, "snr": row["snrr"]}, get(db, row["obsid"]))


def lamost_mrs(path):
    columns = ("obsid", "ra", "dec", "mjd", "lmjm", "band", "coadd", "snr", "rv_br1",
               "rv_br1_err", "rv_br_flag", "fibermask", "bad_b", "bad_r", "gaia_source_id")
    for row in csv_records(path, columns):
        if valid_position(row):
            yield spectra.lamost_mrs_record(row)


def source_id(value):
    return value if value and value.isdecimal() and int(value) > 0 else None


def boss(path, net_path, cache):
    columns = ("spec_file", "ra", "dec", "fieldid", "mjd", "snr", "exptime", "sdss_id",
               "gaia_dr3_source_id", "flag_bad", "flag_warn", "bn_v_r", "e_bn_v_r")
    matches = ((row["spec_file"], row) for row in fits_records(net_path, columns)
               if row["spec_file"].startswith("spec-") and valid_position(row))
    columns = ("plug_ra", "plug_dec", "specobjid", "field", "mjd", "catalogid", "run2d", "sdss_id",
               "mjd_list", "exptime", "zwarning", "sn_median_all", "class")
    with closing(lookup(cache / "sdss-bossnet.sqlite", matches)) as db:
        db.execute("PRAGMA temp_store=FILE")
        db.execute("CREATE TEMP TABLE seen (id TEXT PRIMARY KEY) WITHOUT ROWID")
        for row in fits_records(path, columns):
            row.update(ra=row["plug_ra"], dec=row["plug_dec"], snr=row["sn_median_all"])
            if not valid_position(row):
                continue
            archive = spectra.boss_locator(row)
            name = archive["url"].rsplit("/", 1)[-1] if archive else None
            match = get(db, name) if name else None
            if match:
                db.execute("INSERT OR IGNORE INTO seen VALUES (?)", (name,))
            yield boss_record(name or row["specobjid"], row, archive, match or {})
        for (payload,) in db.execute("SELECT value FROM records WHERE id NOT IN (SELECT id FROM seen) ORDER BY id"):
            row = json.loads(payload)
            field, mjd, name = int(row["fieldid"]), row["mjd"], row["spec_file"]
            archive = spectra.spectrum("boss", f"{SDSS}/spectro/boss/redux/v6_2_1/spectra/daily/full/"
                                       f"{field // 1000:03d}XXX/{field:06d}/{mjd}/{name}")
            yield boss_record(name, row, archive, row)


def boss_record(name, row, archive, match):
    return spectra.record(
        "sdss", "dr20", "boss", name, row, "visit_coadd",
        archive_spectrum_id=row.get("specobjid"),
        archive_source_id=source_id(row["sdss_id"]) or source_id(match.get("sdss_id")),
        gaia_dr3_source_id=source_id(match.get("gaia_dr3_source_id")),
        spectral_class=row.get("class"), exptime_s=spectra.floating(row["exptime"]),
        contributing_mjds=row.get("mjd_list"),
        quality={"zwarning": row.get("zwarning"), "flag_bad": match.get("flag_bad"),
                 "flag_warn": match.get("flag_warn")},
        rv=spectra.rv(match.get("bn_v_r"), match.get("e_bn_v_r"), "BOSSNet", "survey_native"),
        spectrum=archive,
    )


def legacy(path):
    columns = ("plug_ra", "plug_dec", "specobjid", "plate", "mjd", "fiberid",
               "run2d", "class", "sn_median_all", "zwarning", "thing_id")
    for row in fits_records(path, columns):
        row.update(ra=row["plug_ra"], dec=row["plug_dec"], snr=row["sn_median_all"])
        if not valid_position(row):
            continue
        reduction = spectra.LEGACY_REDUCTIONS.get(row["run2d"])
        plate, fiber, mjd = int(row["plate"]), int(row["fiberid"]), row["mjd"]
        archive = (spectra.spectrum("boss", f"{SDSS}/prior-surveys/{reduction}/spectra/lite/"
                                    f"{plate:04d}/spec-{plate:04d}-{mjd}-{fiber:04d}.fits")
                   if reduction else None)
        yield spectra.record("sdss", "dr20", "optical_legacy", row["specobjid"], row, "visit_coadd",
                             sdss_thing_id=source_id(row["thing_id"]), spectral_class=row["class"],
                             quality={"zwarning": row["zwarning"]}, spectrum=archive)


def apogee(path, prior_release=False):
    columns = ("file", "ra", "dec", "mjd", "jd", "starflag", "snr", "apogee_id")
    if prior_release:
        columns += ("plate", "telescope", "field", "vhelio", "vrelerr", "rv_flag")
    else:
        columns += ("uri", "apred_vers", "vrad", "vrelerr")
    for row in fits_records(path, columns):
        if not valid_position(row):
            continue
        if prior_release:
            release = "dr17"
            plate = row["plate"].zfill(4) if row["plate"].isdecimal() else row["plate"]
            url = f"{SDSS17}/apogee/spectro/redux/dr17/visit/{row['telescope']}/{row['field']}/{plate}/{row['mjd']}/{row['file']}"
            archive = spectra.spectrum("apogee", url)
            velocity = spectra.rv(row["vhelio"], row["vrelerr"], "APOGEE_VHELIO_VRELERR", "survey_native")
        else:
            release = row["apred_vers"]
            archive = spectra.apogee_locator(row["uri"])
            velocity = spectra.rv(row["vrad"], row["vrelerr"], "APOGEE_VRAD_VRELERR", "survey_native")
        yield spectra.record("sdss", "dr20", "apogee", f"{release}:{row['file']}", row, "visit_coadd",
                             archive_source_id=row["apogee_id"], jd=spectra.floating(row["jd"]),
                             quality={"starflag": row["starflag"], "rv_flag": row.get("rv_flag")}, rv=velocity,
                             spectrum=archive)


def desi_groups(path):
    groups = defaultdict(set)
    with fits_table(path) as table:
        for row in table:
            pixel = int(row["HEALPIX"])
            groups[f"desi/{pixel // 100:03d}"].add((pixel, string(row["SURVEY"]), string(row["PROGRAM"])))
    return {key: sorted(groups[key]) for key in sorted(groups)}


def desi_records(pixels):
    with spectra.session() as client:
        for pixel, survey, program in pixels:
            yield from spectra.desi_pixel(client, None, survey, program, pixel)


def atomic_json(path, value):
    with tempfile.NamedTemporaryFile("w", dir=path.parent, prefix=path.name + ".", delete=False) as temp:
        partial = Path(temp.name)
        try:
            json.dump(value, temp, indent=2, sort_keys=True)
            temp.write("\n")
            temp.close()
            partial.replace(path)
        finally:
            partial.unlink(missing_ok=True)


def write_job(directory, key, records):
    job_path = directory / key
    job_path.mkdir(parents=True, exist_ok=True)
    status = job_path / "done.json"
    if status.is_file():
        previous = json.loads(status.read_text())
        if all((directory / part["path"]).is_file()
               and (directory / part["path"]).stat().st_size == part["bytes"]
               for part in previous["shards"]):
            print(f"{key}: {previous['records']} records (checkpoint)", file=sys.stderr)
            return previous
    status.unlink(missing_ok=True)
    for path in job_path.glob("*.jsonl.gz"):
        path.unlink()

    parts = []
    iterator = iter(records)
    try:
        for index in itertools.count():
            batch = list(itertools.islice(iterator, SHARD_SIZE))
            if not batch:
                break
            path = job_path / f"{index:06d}.jsonl.gz"
            rows = {row["_id"]: row for row in batch}
            if len(rows) != len(batch):
                raise ValueError(f"Duplicate observation IDs in {key} shard {index}")
            spectra.write_catalog(path, rows)
            parts.append({"path": str(path.relative_to(directory)), "records": len(rows),
                          "bytes": path.stat().st_size})
    finally:
        if close := getattr(iterator, "close", None):
            close()
    result = {"job": key, "shards": parts, "records": sum(part["records"] for part in parts)}
    atomic_json(status, result)
    print(f"{key}: {result['records']} records", file=sys.stderr)
    return result


def run_bulk(directory, surveys, workers, plan_only):
    surveys = sorted(set(surveys))
    if plan_only:
        groups = {}
        if "desi" in surveys:
            with tempfile.TemporaryDirectory() as temp, spectra.session() as client:
                groups = desi_groups(download(client, DESI_INDEX, Path(temp)))
        print(json.dumps({"surveys": surveys,
                          "inputs": {key: url for key, url in INPUTS.items() if key.split("/")[0] in surveys},
                          "desi_index": DESI_INDEX if "desi" in surveys else None,
                          "desi_partitions": len(groups),
                          "desi_pixel_program_products": sum(map(len, groups.values()))}, indent=2))
        return

    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".import.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        bulk_run(directory, surveys, workers)



def desi_jobs(directory, groups, workers):
    def run_partition(key, pixels):
        return write_job(directory, key, desi_records(pixels))

    items = iter(groups.items())
    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {pool.submit(run_partition, key, pixels)
                   for key, pixels in itertools.islice(items, workers)}
        while pending:
            completed, pending = wait(pending, return_when=FIRST_COMPLETED)
            results.extend(job.result() for job in completed)
            pending.update(pool.submit(run_partition, key, pixels)
                           for key, pixels in itertools.islice(items, len(completed)))
    return sorted(results, key=lambda result: result["job"])


def bulk_run(directory, surveys, workers):
    configuration = {"format_version": FORMAT_VERSION, "surveys": surveys}
    config_path = directory / "configuration.json"
    if config_path.exists() and json.loads(config_path.read_text()) != configuration:
        raise ValueError("This output directory contains a different release import configuration")
    atomic_json(config_path, configuration)
    (directory / "manifest.json").unlink(missing_ok=True)
    cache = directory / ".downloads"
    jobs = []

    def input_for(client, name):
        return download(client, INPUTS[name], cache)

    def lamost_job(client, name):
        if name == "lamost/lrs":
            yield from lamost_lrs(input_for(client, name), input_for(client, "lamost/stellar"), cache)
        else:
            yield from lamost_mrs(input_for(client, name))

    def sdss_job(client, name):
        path = input_for(client, name)
        if name == "sdss/legacy":
            yield from legacy(path)
        elif name == "sdss/boss":
            yield from boss(path, input_for(client, "sdss/bossnet"), cache)
        else:
            yield from apogee(path, prior_release=name == "sdss/apogee17")

    with spectra.session() as client:
        inputs = {"lamost": ("lamost/lrs", "lamost/mrs"),
                  "sdss": ("sdss/legacy", "sdss/boss", "sdss/apogee", "sdss/apogee17")}
        for survey, importer in (("lamost", lamost_job), ("sdss", sdss_job)):
            if survey in surveys:
                for name in inputs[survey]:
                    jobs.append(write_job(directory, name, importer(client, name)))

        if "desi" in surveys:
            groups = desi_groups(download(client, DESI_INDEX, cache))

            jobs.extend(desi_jobs(directory, groups, workers))

    manifest = {**configuration, "jobs": jobs, "records": sum(job["records"] for job in jobs)}
    atomic_json(directory / "manifest.json", manifest)
    print(f"{manifest['records']} records across {len(jobs)} completed partitions. See {directory / 'manifest.json'}")
