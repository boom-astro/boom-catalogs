"""Build a versioned, per-spectrum metadata index from public archives."""

import argparse
import csv
import gzip
import io
import json
import math
import os
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LAMOST = "https://www.lamost.org"
SDSS = "https://dr20.sdss.org/sas/dr20"
SKYSERVER = "https://skyserver.sdss.org/dr20/SkyServerWS/SearchTools/SqlSearch"
DESI = "https://data.desi.lbl.gov/public/dr1/spectro/redux/iron"
DESI_MWS = "https://data.desi.lbl.gov/public/dr1/vac/dr1/mws/iron/v1.0/rv_output/240521/healpix"
LIMIT = 20000
LEGACY_REDUCTIONS = {
    "26": "sdss2-dr8-sdss/spectro/redux/26",
    "103": "sdss2-dr8-sdss/spectro/redux/103",
    "104": "sdss2-dr8-sdss/spectro/redux/104",
    "v5_13_2": "sdss4-dr17-eboss/spectro/redux/v5_13_2",
}


@dataclass(frozen=True)
class Cone:
    ra: float
    dec: float
    radius: float

    def __post_init__(self):
        if not (0 <= self.ra < 360 and -90 <= self.dec <= 90 and 0 < self.radius <= 1):
            raise ValueError("Expected RA in [0, 360), Dec in [-90, 90], and radius in (0, 1] degrees")

    def adql(self, ra, dec):
        return f"1=CONTAINS(POINT({ra},{dec}),CIRCLE({self.ra},{self.dec},{self.radius}))"

    def box(self, ra, dec):
        south, north = max(-90, self.dec - self.radius), min(90, self.dec + self.radius)
        latitude = f"{dec} BETWEEN {south - 1e-12} AND {north + 1e-12}"
        if south == -90 or north == 90:
            return latitude
        width = math.degrees(math.asin(math.sin(math.radians(self.radius))
                                      / math.cos(math.radians(self.dec)))) + 1e-12
        west, east = (self.ra - width) % 360, (self.ra + width) % 360
        longitude = (f"{ra} BETWEEN {west} AND {east}" if west <= east
                     else f"{ra} >= {west} OR {ra} <= {east}")
        return f"{latitude} AND ({longitude})"

    def contains(self, ra, dec):
        ra_delta = math.radians((ra - self.ra + 180) % 360 - 180)
        dec_delta = math.radians(dec - self.dec)
        haversine = math.sin(dec_delta / 2) ** 2 + (
            math.cos(math.radians(dec)) * math.cos(math.radians(self.dec))
            * math.sin(ra_delta / 2) ** 2
        )
        return haversine <= math.sin(math.radians(self.radius) / 2) ** 2 + 1e-16


def session():
    client = requests.Session()
    client.mount("https://", HTTPAdapter(max_retries=Retry(
        total=2, backoff_factor=1, status_forcelist=(429, 502, 503, 504)
    )))
    return client


def request(client, url, params=None):
    response = client.get(url, params=params, timeout=(10, 75))
    if not response.ok:
        raise RuntimeError(f"{url}: HTTP {response.status_code}: {response.text[:400]}")
    return response


def csv_rows(body):
    reader = csv.DictReader(line for line in body.splitlines() if line and not line.startswith("#"))
    if not reader.fieldnames or reader.fieldnames[0].startswith(("<", "ERROR")):
        raise ValueError(f"Invalid archive CSV: {body[:300]}")
    return [{key.lower(): value.strip() if value else "" for key, value in row.items()}
            for row in reader]


def votable_rows(body):
    root = ET.fromstring(body)
    statuses = root.findall(".//{*}INFO[@name='QUERY_STATUS']")
    for info in statuses:
        if info.get("value") != "OK":
            raise ValueError(f"TAP {info.get('value')}: {info.text}")
    fields = [field.attrib["name"].lower() for field in root.findall(".//{*}FIELD")]
    if not statuses or not fields:
        raise ValueError("TAP response has no query status or fields")
    if root.find(".//{*}TABLEDATA") is None:
        raise ValueError("TAP response did not return TABLEDATA")
    return [dict(zip(fields, ((cell.text or "").strip() for cell in row), strict=True))
            for row in root.findall(".//{*}TR")]


def bounded(rows):
    if len(rows) >= LIMIT:
        raise ValueError(f"Archive row limit ({LIMIT}) reached. Split the footprint and rerun.")
    return rows


def tap(client, url, query):
    response = request(client, url, {
        "REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "votable", "MAXREC": LIMIT, "QUERY": query,
    })
    return bounded(votable_rows(response.content))


def skyserver(client, query):
    return bounded(csv_rows(request(client, SKYSERVER, {"cmd": query, "format": "csv"}).text))


def floating(value):
    try:
        value = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return value if math.isfinite(value) and value not in {-999, -9999, -99999} else None


def rv(value, error, method, frame):
    value, error = floating(value), floating(error)
    if value is None or error is None or error <= 0:
        return None
    return {"value_km_s": value, "error_km_s": error, "method": method, "frame": frame}


def spectrum(format_name, url=None, **fields):
    return {"format": format_name, **({"url": url} if url else {}), **fields}


def record(survey, release, product, key, row, granularity, **metadata):
    ra, dec = float(row["ra"]), float(row["dec"])
    if not (0 <= ra < 360 and -90 <= dec <= 90):
        raise ValueError(f"Invalid sky position for {survey} {product} {key}: {ra}, {dec}")
    return {
        "_id": f"{survey}:{release}:{product}:{key}",
        "survey": survey, "release": release, "product": product,
        "ra": ra, "dec": dec, "granularity": granularity,
        "coordinates": {"radec_geojson": {"type": "Point", "coordinates": [ra - 180, dec]}},
        "mjd": floating(row.get("mjd")), "snr": floating(row.get("snr")),
        **metadata,
    }


def lamost(client, cone):
    url = f"{LAMOST}/dr12/v2.0/voservice/tap/sync"
    where = cone.adql("ra", "dec")
    lrs = tap(client, url, f"""SELECT TOP {LIMIT} obsid,ra,dec,mjd,snrr AS snr,class,
        fibermask,gaia_source_id FROM catalogue WHERE {where}""")
    stellar = tap(client, url, f"""SELECT TOP {LIMIT} obsid,rv,rv_err
        FROM stellar WHERE {where}""")
    estimates = {row["obsid"]: rv(row["rv"], row["rv_err"], "LASP", "survey_native")
                 for row in stellar}
    for row in lrs:
        obs = row["obsid"]
        yield record("lamost", "dr12-v2.0", "lrs", obs, row, "visit_coadd",
                     gaia_dr3_source_id=row["gaia_source_id"] or None, spectral_class=row["class"],
                     quality={"fibermask": row["fibermask"]}, rv=estimates.get(obs),
                     spectrum=spectrum("lamost", f"{LAMOST}/dr12/v2.0/spectrum/fits/{obs}",
                                       extensions=["COADD"]))
    mrs = tap(client, url, f"""SELECT TOP {LIMIT} obsid,ra,dec,mjd,lmjm,band,coadd,
        snr,rv_br1,rv_br1_err,rv_br_flag,fibermask,bad_b,bad_r,gaia_source_id
        FROM med_catalogue WHERE {where}""")
    for row in mrs:
        obs, band, minute = row["obsid"], row["band"], row["lmjm"]
        coadd = row["coadd"] == "1"
        if band not in {"B", "R"} or not coadd and not minute:
            raise ValueError(f"Unrecognized LAMOST MRS spectrum row {row}")
        extension = f"COADD_{band}" if coadd else f"{band}-{minute}"
        yield record("lamost", "dr12-v2.0", "mrs", f"{obs}:{extension}", row,
                     "visit_coadd" if coadd else "exposure",
                     gaia_dr3_source_id=row["gaia_source_id"] or None, band=band,
                     visit_id=obs, epoch_id=f"{obs}:coadd" if coadd else f"{obs}:{minute}",
                     quality={key: row[key] for key in ("rv_br_flag", "fibermask", "bad_b", "bad_r")},
                     rv=rv(row["rv_br1"], row["rv_br1_err"], "LAMOST_MRS_BR1", "survey_native"),
                     spectrum=spectrum("lamost", f"{LAMOST}/openapi/dr12/v2.0/mrs/spectrum/fits?obsid={obs}",
                                       extensions=[extension]))


def boss_locator(row):
    reduction, field = row["run2d"], row["field"]
    if reduction != "v6_2_1" or not field.isdecimal() or not row["catalogid"].isdecimal():
        return None
    field, mjd, catalog = int(field), row["mjd"], row["catalogid"]
    return spectrum("boss", f"{SDSS}/spectro/boss/redux/{reduction}/spectra/daily/full/"
                    f"{field // 1000:03d}XXX/{field:06d}/{mjd}/spec-{field:06d}-{mjd}-{catalog}.fits")


def sdss(client, cone):
    nearby = f"dbo.fGetNearbyAllspecEq({cone.ra},{cone.dec},{cone.radius * 60})"


    def query(columns, table, key, valid_key=""):
        return skyserver(client, f"""SELECT TOP {LIMIT} {columns} FROM {table} s
            WHERE s.{key} IN (SELECT f.{key} FROM {nearby} f {valid_key})""")

    identities = defaultdict(set)
    for row in skyserver(client, f"SELECT TOP {LIMIT} specobjid,sdss_id FROM {nearby} WHERE sdss_id > 0"):
        if row["specobjid"]:
            identities[row["specobjid"]].add(row["sdss_id"])

    def sdss_id(row):
        matches = identities[row["specobjid"]]
        return next(iter(matches)) if len(matches) == 1 else None


    bossnet = query("""s.ra,s.dec,s.fieldid,s.mjd,s.spec_file,s.bn_v_r,s.e_bn_v_r,
        s.flag_bad,s.flag_warn,s.gaia_dr3_source_id,s.sdss_id,s.snr,s.exptime""", "boss_net_boss_visit", "sdss_id", "WHERE f.sdss_id > 0")
    net = {row["spec_file"]: row for row in bossnet if cone.contains(float(row["ra"]), float(row["dec"]))}
    daily = skyserver(client, f"""SELECT TOP {LIMIT} s.plug_ra AS ra,s.plug_dec AS dec,
        s.specobjid,s.field,s.mjd,s.catalogid,s.run2d,s.programname,s.mjd_list,s.exptime,
        s.zwarning,s.sn_median_all AS snr,s.class FROM mos_sdssv_boss_spall s
        WHERE {cone.box('s.plug_ra', 's.plug_dec')}""")
    for row in daily:
        if not cone.contains(float(row["ra"]), float(row["dec"])):
            continue
        archive = boss_locator(row)
        filename = archive["url"].rsplit("/", 1)[-1] if archive else None
        match = net.pop(filename, {})
        yield record("sdss", "dr20", "boss", filename or row["specobjid"], row, "visit_coadd",
                     archive_spectrum_id=row["specobjid"],
                     archive_source_id=sdss_id(row) or match.get("sdss_id"),
                     gaia_dr3_source_id=match.get("gaia_dr3_source_id") or None, spectral_class=row["class"],
                     quality={"zwarning": row["zwarning"], "flag_bad": match.get("flag_bad"),
                              "flag_warn": match.get("flag_warn")},
                     exptime_s=floating(row["exptime"]), contributing_mjds=row["mjd_list"],
                     rv=rv(match.get("bn_v_r"), match.get("e_bn_v_r"), "BOSSNet", "survey_native"),
                     spectrum=archive)
    for filename, row in net.items():
        field, mjd = int(row["fieldid"]), row["mjd"]
        archive = spectrum("boss", f"{SDSS}/spectro/boss/redux/v6_2_1/spectra/daily/full/"
                           f"{field // 1000:03d}XXX/{field:06d}/{mjd}/{filename}")
        yield record("sdss", "dr20", "boss", filename, row, "visit_coadd",
                     archive_source_id=row["sdss_id"],
                     gaia_dr3_source_id=row["gaia_dr3_source_id"] or None,
                     quality={"flag_bad": row["flag_bad"], "flag_warn": row["flag_warn"]},
                     exptime_s=floating(row["exptime"]),
                     rv=rv(row["bn_v_r"], row["e_bn_v_r"], "BOSSNet", "survey_native"),
                     spectrum=archive)
    legacy = query("""s.ra,s.dec,s.specobjid,s.plate,s.mjd,s.fiberid,s.run2d,s.class,
        s.snmedian AS snr,s.zwarning,s.elodieZ,s.elodieZErr""", "SpecObjAll", "specobjid")
    for row in legacy:
        reduction = LEGACY_REDUCTIONS.get(row["run2d"])
        plate, fiber, mjd = int(row["plate"]), int(row["fiberid"]), row["mjd"]
        archive = spectrum("boss", f"{SDSS}/prior-surveys/{reduction}/spectra/lite/"
                           f"{plate:04d}/spec-{plate:04d}-{mjd}-{fiber:04d}.fits") if reduction else None
        yield record("sdss", "dr20", "optical_legacy", row["specobjid"], row, "visit_coadd",
                     archive_source_id=sdss_id(row), spectral_class=row["class"],
                     quality={"zwarning": row["zwarning"]},
                     spectrum=archive)
    visits = skyserver(client, f"""SELECT TOP {LIMIT} s.[FILE] AS filename,s.URI,s.ra,s.dec,
        s.mjd,s.jd,s.vrad,s.vrelerr,s.starflag,s.snr,s.apogee_id,s.apred_vers
        FROM apogee_drp_allvisit s WHERE {cone.box('s.ra', 's.dec')}""")
    for row in visits:
        uri = row["uri"].lstrip("/")
        archive = spectrum("apogee", f"{SDSS}/{uri}") if uri.startswith("spectro/apogee/") else None
        yield record("sdss", "dr20", "apogee", f"{row['apred_vers']}:{row['filename']}", row,
                     "visit_coadd", archive_source_id=row["apogee_id"], jd=floating(row["jd"]),
                     quality={"starflag": row["starflag"]},
                     rv=rv(row["vrad"], row["vrelerr"], "APOGEE_VRAD_VRELERR", "survey_native"),
                     spectrum=archive)


@lru_cache(maxsize=1)
def desi_tilepix(client):
    from astropy.io import fits

    response = request(client, f"{DESI}/healpix/tilepix.fits")
    groups = defaultdict(set)
    with fits.open(io.BytesIO(response.content), memmap=False) as hdus:
        for row in hdus["TILEPIX"].data:
            groups[int(row["HEALPIX"])].add((str(row["SURVEY"]), str(row["PROGRAM"])))
    return groups


def desi_mws(client, group, filename, targets):
    from astropy.io import fits

    url = f"{DESI_MWS}/{group}/rvtab_spectra-{filename}.fits"
    response = client.get(url, timeout=(10, 75))
    if response.status_code == 404:
        return {}
    response.raise_for_status()
    measurements = {}
    with fits.open(io.BytesIO(response.content), memmap=False) as hdus:
        visits, fibers = hdus["RVTAB"].data, hdus["FIBERMAP"].data
        for fit, fiber in zip(visits, fibers, strict=True):
            target = int(fit["TARGETID"])
            if target != int(fiber["TARGETID"]):
                raise ValueError(f"DESI RV and FIBERMAP rows are misaligned in {url}")
            if target not in targets:
                continue
            key = target, int(fiber["EXPID"]), int(fiber["FIBER"])
            measurements[key] = {
                "rv": rv(fit["VRAD"], fit["VRAD_ERR"], "RVSpecFit", "survey_native"),
                "snr": max(float(fit[key]) for key in ("SN_B", "SN_R", "SN_Z")),
                "rvs_warn": int(fit["RVS_WARN"]),
                "rv_metadata_url": url,
            }
    return measurements


def desi_pixel(client, cone, survey, program, pixel):
    from astropy.io import fits

    group = f"{survey}/{program}/{pixel // 100}/{pixel}"
    filename = f"{survey}-{program}-{pixel}"
    url = f"{DESI}/healpix/{group}/redrock-{filename}.fits"
    targets = {}
    exposures = defaultdict(list)
    with fits.open(io.BytesIO(request(client, url).content), memmap=False) as hdus:
        redshifts, fibers = hdus["REDSHIFTS"].data, hdus["FIBERMAP"].data
        for redshift, fiber in zip(redshifts, fibers, strict=True):
            target = int(fiber["TARGETID"])
            if target != int(redshift["TARGETID"]):
                raise ValueError(f"DESI redshift and FIBERMAP rows are misaligned in {url}")
            ra, dec = float(fiber["MEAN_FIBER_RA"]), float(fiber["MEAN_FIBER_DEC"])
            if fiber["OBJTYPE"] == "SKY" or not cone.contains(ra, dec):
                continue
            targets[target] = (redshift, fiber, {"ra": ra, "dec": dec})
        for row in hdus["EXP_FIBERMAP"].data:
            target = int(row["TARGETID"])
            if target in targets:
                exposures[target].append(row)
        if not targets:
            return

        measured = desi_mws(client, group, filename, targets)
        for target, (redshift, fiber, position) in targets.items():
            inputs = exposures[target]
            times = [float(row["MJD"]) for row in inputs if math.isfinite(row["MJD"])]
            source_id = str(target)
            coadd_id = f"desi:dr1-iron:healpix_coadd:{filename}:{target}"
            yield record("desi", "dr1-iron", "healpix_coadd", f"{filename}:{target}", position,
                         "coadd", archive_source_id=source_id,
                         spectral_class=str(redshift["SPECTYPE"]).strip(),
                         exposure_count=int(fiber["COADD_NUMEXP"]),
                         night_count=int(fiber["COADD_NUMNIGHT"]),
                         first_mjd=min(times, default=None), last_mjd=max(times, default=None),
                         listed_exposure_ids=sorted({int(row["EXPID"]) for row in inputs}),
                         exptime_s=floating(fiber["COADD_EXPTIME"]), metadata_url=url,
                         quality={"zwarn": int(redshift["ZWARN"]),
                                  "fiberstatus": int(fiber["COADD_FIBERSTATUS"])},
                         redshift=floating(redshift["Z"]), redshift_error=floating(redshift["ZERR"]),
                         spectrum=spectrum("desi", f"{DESI}/healpix/{group}/coadd-{filename}.fits",
                                           target_id=source_id, layout="coadd"))
            for row in inputs:
                expid, night, petal, fiber_id = (
                    int(row[key]) for key in ("EXPID", "NIGHT", "PETAL_LOC", "FIBER")
                )
                fit = measured.get((target, expid, fiber_id), {})
                frame = f"{DESI}/exposures/{night}/{expid:08d}"
                yield record("desi", "dr1-iron", "exposure", f"{target}:{expid}:{fiber_id}", {
                    **position, "mjd": row["MJD"], "snr": fit.get("snr"),
                }, "exposure", archive_source_id=source_id, exposure_id=expid, fiber=fiber_id,
                             related_coadd_id=coadd_id, exptime_s=floating(row["EXPTIME"]),
                             rv=fit.get("rv"), metadata_url=url, rv_metadata_url=fit.get("rv_metadata_url"),
                             quality={"rvs_warn": fit.get("rvs_warn"), "fiberstatus": int(row["FIBERSTATUS"])},
                             spectrum=spectrum("desi", target_id=source_id, layout="cframe",
                                               urls=[f"{frame}/cframe-{arm}{petal}-{expid:08d}.fits.gz?download=1"
                                                     for arm in "brz"]))


def desi(client, cone):
    from astropy import units as u
    from astropy_healpix import HEALPix

    pixels = HEALPix(nside=64, order="nested").cone_search_lonlat(
        cone.ra * u.deg, cone.dec * u.deg, cone.radius * u.deg
    )
    tilepix = desi_tilepix(client)
    for pixel in sorted(int(pixel) for pixel in pixels):
        for survey, program in sorted(tilepix.get(pixel, ())):
            yield from desi_pixel(client, cone, survey, program, pixel)


IMPORTERS = {"lamost": lamost, "sdss": sdss, "desi": desi}


def collect(importer, cones):
    rows = {}
    with session() as client:
        for cone in cones:
            before = len(rows)
            for row in importer(client, cone):
                if not cone.contains(row["ra"], row["dec"]):
                    continue
                existing = rows.setdefault(row["_id"], row)
                if existing != row:
                    raise ValueError(f"Conflicting metadata for {row['_id']}")
            print(f"{importer.__name__} {cone}: {len(rows) - before} new records", file=sys.stderr)
    return rows


def write_catalog(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as temp:
        staging = Path(temp.name)
        try:
            with gzip.GzipFile(filename="", fileobj=temp, mode="wb", mtime=0) as stream:
                for key in sorted(rows):
                    stream.write((json.dumps(rows[key], allow_nan=False, sort_keys=True) + "\n").encode())
            temp.close()
            os.replace(staging, path)
        finally:
            staging.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cone", nargs=3, type=float, action="append", required=True,
                        metavar=("RA", "DEC", "RADIUS_DEG"))
    parser.add_argument("--survey", choices=IMPORTERS, action="append",
                        help="Repeat to select multiple surveys. The default is all three.")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        cones = [Cone(*values) for values in args.cone]
    except ValueError as exc:
        parser.error(str(exc))
    selected = {key: IMPORTERS[key] for key in (args.survey or IMPORTERS)}
    rows = {}
    with ThreadPoolExecutor(max_workers=len(selected)) as pool:
        jobs = {key: pool.submit(collect, importer, cones) for key, importer in selected.items()}
        for key, job in jobs.items():
            try:
                rows.update(job.result())
            except Exception as exc:
                raise RuntimeError(f"{key} import failed") from exc
    write_catalog(args.output, rows)
    print(json.dumps({
        "records": len(rows),
        "by_product": dict(Counter(f"{row['survey']}/{row['product']}" for row in rows.values())),
        "with_rv": sum(bool(row.get("rv")) for row in rows.values()),
        "with_spectrum_locator": sum(bool(row.get("spectrum")) for row in rows.values()),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
