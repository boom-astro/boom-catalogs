"""Read the normalized public 4MOST visit export used by BOOM's RV search."""

import csv
import math
import re
from urllib.parse import urlsplit

PRODUCTS = {"lrs-a": 3.0, "lrs-b": 3.0, "hrs": 1.0}
REQUIRED = {
    "release", "pipeline", "spectrograph", "target_id", "exposure_id",
    "ra", "dec", "mjd_utc_mid", "exptime_s", "snr", "rv_barycentric_km_s",
    "rv_error_km_s", "quality_ok", "public", "spectrum_url",
}


def token(value):
    return bool(re.fullmatch(r"[A-Za-z0-9_.+-]+", value or ""))


def read_visits(path):
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if missing := REQUIRED - set(reader.fieldnames or []):
            raise ValueError(f"4MOST export lacks columns {sorted(missing)}")
        for line, raw in enumerate(reader, 2):
            if raw["public"] != "1":
                raise ValueError(f"{path.name}:{line}: only public 4MOST visits can enter this catalog")
            try:
                release, pipeline, mode = (raw[key] for key in ("release", "pipeline", "spectrograph"))
                target, exposure = raw["target_id"], raw["exposure_id"]
                if mode not in PRODUCTS or not all(map(token, (release, pipeline, target, exposure))):
                    raise ValueError("invalid release, pipeline, spectrograph or identifier")
                gaia = raw.get("gaia_dr3_source_id") or None
                if gaia and not (gaia.isascii() and gaia.isdecimal() and int(gaia) > 0):
                    raise ValueError("invalid Gaia DR3 source ID")
                ra, dec, mjd, duration, snr, velocity, error = (
                    float(raw[key]) for key in ("ra", "dec", "mjd_utc_mid", "exptime_s", "snr",
                                                "rv_barycentric_km_s", "rv_error_km_s")
                )
                if not (all(map(math.isfinite, (ra, dec, mjd, duration, snr, velocity, error)))
                        and 0 <= ra < 360 and -90 <= dec <= 90 and mjd > 0
                        and duration > 0 and snr >= 0 and error > 0):
                    raise ValueError("invalid position, epoch, exposure time, S/N or RV")
                if raw["quality_ok"] not in {"0", "1"}:
                    raise ValueError("quality_ok must be 0 or 1")
                url = urlsplit(raw["spectrum_url"])
                if url.scheme != "https" or not url.hostname or url.username or url.password:
                    raise ValueError("expected a public HTTPS FITS locator")
                row_id = f"fourmost:{release}:{mode}:{pipeline}:{target}:{exposure}"
            except (TypeError, ValueError) as error_detail:
                raise ValueError(f"{path.name}:{line}: {error_detail}") from error_detail
            yield {
                "_id": row_id, "survey": "fourmost", "release": release, "product": mode,
                "archive_source_id": target, "gaia_dr3_source_id": gaia,
                "exposure_id": exposure, "granularity": "exposure",
                "ra": ra, "dec": dec, "mjd": mjd, "exptime_s": duration, "snr": snr,
                "rv": {"method": pipeline, "frame": "barycentric",
                       "value_km_s": velocity, "error_km_s": error},
                "quality": {"accepted": raw["quality_ok"] == "1"},
                "spectrum": {"format": "fourmost", "url": raw["spectrum_url"]},
            }
