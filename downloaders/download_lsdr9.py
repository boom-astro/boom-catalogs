"""Script to download Legacy Survey DR9 sweep + photo-z catalogs and minify them in place.

DR9 is not available as HATS parquet on data.lsdb.io (only DR10.1 is), so unlike
download_lsdr10.py this cannot pull a column subset over HTTP range requests: FITS binary
tables are row-major, so the whole file has to cross the wire to get the ~21 columns we keep.
That is ~0.2 TB for dr9/north and ~1.0 TB for dr9/south.

To avoid needing that much scratch space at once, each sweep is downloaded together with its
photo-z counterpart, minified to parquet, and the two FITS files are deleted before moving on
(--keep-fits turns that off). Peak disk is then a few GB per worker rather than the full
catalog. Work is resumable: a pair whose parquet already exists is skipped, so a job that hits
a wall-clock limit can simply be resubmitted.
"""
import argparse
import importlib.util
import os
import re
import shutil
import sys
import threading
import time

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/ls_dr9/"

BASE_URL = "https://portal.nersc.gov/cfs/cosmo/data/legacysurvey/dr9"
# 9.1-photo-z is the Zhou et al. (2023) rerun; 9.0-photo-z is the older Zhou et al. (2021)
# one. Both are computed against the same 9.0 sweeps, so either pairs row-for-row with them.
SWEEP_VERSION = "9.0"
PHOTOZ_VERSION = "9.1-photo-z"

# The minifier is a sibling script rather than a package, and its filename has hyphens, so it
# cannot be imported by name.
_MINIFIER_PATH = os.path.join(
	os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "minifiers", "ls-dr9-minify.py"
)
_spec = importlib.util.spec_from_file_location("ls_dr9_minify", _MINIFIER_PATH)
ls_dr9_minify = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ls_dr9_minify)

# Set on Ctrl-C. Worker threads poll it so an in-flight download gives up inside a chunk
# instead of running to completion: a sweep is most of a GB, and ThreadPoolExecutor's shutdown
# waits for running tasks no matter what the main thread does.
STOP = threading.Event()


class Aborted(BaseException):
    """Raised in a worker when the run is interrupted.

    Deliberately not an `Exception`, so the `except Exception` that turns a real failure into a
    logged error lets this pass through untouched. An aborted pair is not a failed pair.
    """


parser = argparse.ArgumentParser(
    description="Download and minify Legacy Survey DR9 sweep + photo-z catalogs."
)
parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR,
                    help="Directory to save downloaded and minified files")
parser.add_argument("--region", choices=["north", "south", "both"], default="north",
                    help="DR9 reduction to ingest. 'north' (BASS+MzLS) is the sky DR10's DECam "
                         "footprint does not reach; 'south' overlaps DR10 heavily.")
parser.add_argument("--photoz-version", type=str, default=PHOTOZ_VERSION,
                    choices=["9.0-photo-z", "9.1-photo-z"],
                    help="Which photo-z sweep release to pair with the 9.0 sweeps")
parser.add_argument("--processes", type=int, default=8,
                    help="Number of pairs to download and minify concurrently")
parser.add_argument("--resolve", choices=["dr9", "dec-only", "none"], default="dr9",
                    help="Row cut applied by the minifier for the north/south overlap; see "
                         "minifiers/ls-dr9-minify.py")
parser.add_argument("--keep-fits", action="store_true",
                    help="Keep the downloaded FITS files instead of deleting them after minifying")
parser.add_argument("--no-minify", action="store_true",
                    help="Only download; implies --keep-fits. Run the minifier separately after.")
parser.add_argument("--limit", type=int, default=None,
                    help="Only process the first N pairs. Useful for a smoke test.")


def list_fits(url):
    """List the .fits filenames in a NERSC Apache directory listing."""
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    names = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and href.startswith("sweep-") and href.endswith(".fits"):
            names.append(href)
    return sorted(set(names))


# sweep-<RAmin><p|m><Decmin>-<RAmax><p|m><Decmax>.fits, e.g. sweep-000m005-010p000.fits is
# RA 0..10, Dec -5..0.
SWEEP_NAME_RE = re.compile(r"sweep-(\d{3})([pm])(\d{3})-(\d{3})([pm])(\d{3})\.fits$")


def sweep_dec_max(name):
    """Upper Dec bound of a sweep file's sky box, from its filename. None if unparseable."""
    match = SWEEP_NAME_RE.match(name)
    if not match:
        return None
    return int(match.group(6)) * (1 if match.group(5) == "p" else -1)


def skip_by_dec_box(names, resolve):
    """Drop sweeps whose entire Dec box falls below the resolve boundary.

    dr9/north reduces equatorial sky that the resolve hands to dr9/south, so those files
    would be downloaded in full only to minify down to zero rows. The filename gives the Dec
    box, so that is decidable before any bytes move. It only saves ~3% of the north volume
    (the high-Dec files are much the largest) but it avoids ~47 pointless downloads.
    """
    if resolve == "none":
        return names, 0
    kept = []
    skipped = 0
    for name in names:
        dec_max = sweep_dec_max(name)
        # An unrecognised name is kept; the minifier will cut its rows correctly anyway.
        if dec_max is not None and dec_max <= ls_dr9_minify.RESOLVE_DEC:
            skipped += 1
            continue
        kept.append(name)
    return kept, skipped


def download_file(url, output_path, retries=5):
    """Download `url` to `output_path`, skipping it if a complete copy is already there."""
    response = requests.head(url, timeout=60)
    response.raise_for_status()
    expected = int(response.headers.get("content-length", 0))

    if os.path.exists(output_path) and expected and os.path.getsize(output_path) == expected:
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    tmp_path = output_path + ".part"
    for attempt in range(retries):
        try:
            if STOP.is_set():
                raise Aborted()
            with requests.get(url, stream=True, timeout=(30, 120)) as response:
                response.raise_for_status()
                with open(tmp_path, "wb") as handle:
                    for chunk in response.iter_content(chunk_size=1 << 20):
                        if STOP.is_set():
                            raise Aborted()
                        handle.write(chunk)
            # A sweep that stops early still looks like a valid FITS file to astropy for the
            # rows it does contain, so verify the length here rather than letting a short
            # read turn into a silent row-count mismatch at minify time.
            if expected and os.path.getsize(tmp_path) != expected:
                raise IOError(
                    f"short read: got {os.path.getsize(tmp_path)} bytes, expected {expected}"
                )
            os.replace(tmp_path, output_path)
            return
        # BaseException rather than Exception so an abort also drops its partial file. A
        # leftover .part is never mistaken for a complete download, but it still wastes disk.
        except BaseException as e:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            if isinstance(e, Aborted) or not isinstance(e, Exception):
                raise
            if attempt == retries - 1:
                raise RuntimeError(f"Failed to download {url} after {retries} attempts: {e}") from e
            # Interruptible backoff; time.sleep would keep a cancelled run alive for seconds.
            if STOP.wait(2 ** attempt):
                raise Aborted() from e


def process_pair(task):
    """Download one sweep/photo-z pair, minify it, and drop the FITS files."""
    region, name, args = task
    sweep_url = f"{BASE_URL}/{region}/sweep/{SWEEP_VERSION}/{name}"
    photoz_name = name.replace(".fits", "-pz.fits")
    photoz_url = f"{BASE_URL}/{region}/sweep/{args.photoz_version}/{photoz_name}"

    region_dir = os.path.join(args.output_dir, region)
    sweep_path = os.path.join(region_dir, "sweep", name)
    photoz_path = os.path.join(region_dir, "photo-z", photoz_name)
    output_path = os.path.join(
        args.output_dir, "minified", region, name.replace(".fits", ".parquet")
    )

    if os.path.exists(output_path) or STOP.is_set():
        return (name, 0, None)

    try:
        download_file(sweep_url, sweep_path)
        download_file(photoz_url, photoz_path)
        if args.no_minify:
            return (name, 0, None)
        rows = ls_dr9_minify.minify_pair(
            sweep_path, photoz_path, output_path, resolve=args.resolve
        )
        if not args.keep_fits:
            os.remove(sweep_path)
            os.remove(photoz_path)
        return (name, rows, None)
    except Exception as e:
        return (name, 0, f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    args = parser.parse_args()
    if args.no_minify:
        args.keep_fits = True

    regions = ["north", "south"] if args.region == "both" else [args.region]

    tasks = []
    for region in regions:
        listing_url = f"{BASE_URL}/{region}/sweep/{SWEEP_VERSION}/"
        print(f"Listing {listing_url} ...")
        names = list_fits(listing_url)
        names, skipped = skip_by_dec_box(names, args.resolve)
        print(f"  {len(names)} sweep files in dr9/{region}"
              f"{f' ({skipped} skipped: Dec box entirely below the resolve boundary)' if skipped else ''}")
        tasks.extend((region, name, args) for name in names)

    if args.limit:
        tasks = tasks[: args.limit]

    free_gb = shutil.disk_usage(os.path.dirname(os.path.abspath(args.output_dir)) or ".").free / 1e9
    print(f"\n{len(tasks)} pairs to process, {args.processes} at a time "
          f"(photo-z: {args.photoz_version}, resolve: {args.resolve}).")
    print(f"Free space at destination: {free_gb:,.0f} GB"
          f"{'' if not args.keep_fits else '  (--keep-fits: the full FITS set is retained)'}")

    os.makedirs(args.output_dir, exist_ok=True)
    total_rows = 0
    failures = []
    # Deliberately not `with ThreadPoolExecutor(...)`: its __exit__ calls shutdown(wait=True),
    # which drains every queued task. Since map() submits all of them up front, Ctrl-C would
    # otherwise keep downloading the whole catalog before exiting.
    pool = ThreadPoolExecutor(max_workers=args.processes)
    interrupted = False
    done = 0
    with tqdm(total=len(tasks), desc="DR9 sweeps") as pbar:
        futures = [pool.submit(process_pair, task) for task in tasks]
        try:
            for future in as_completed(futures):
                try:
                    name, rows, error = future.result()
                except Aborted:
                    continue
                if error is not None:
                    failures.append((name, error))
                    pbar.write(f"FAILED {name}: {error}")
                total_rows += rows
                done += 1
                pbar.update()
        except KeyboardInterrupt:
            interrupted = True
            STOP.set()
            pbar.write("\nInterrupted: dropping queued pairs and stopping in-flight downloads.")
        finally:
            # Queued pairs are cancelled outright; the few already running see STOP inside
            # their next chunk and unwind, so this returns in about a second rather than
            # after the remaining hours of downloading.
            pool.shutdown(wait=True, cancel_futures=True)

    verb = "Stopped" if interrupted else "Done"
    print(f"\n{verb}: {done}/{len(tasks)} pairs completed, "
          f"{len(failures)} failed, {total_rows:,} rows written.")
    if interrupted:
        print("Partial downloads were removed. Completed pairs are kept, so re-running "
              "resumes from here.")
    if failures:
        fail_log = os.path.join(args.output_dir, "failed_pairs.txt")
        with open(fail_log, "w") as f:
            for name, error in failures:
                f.write(f"{name}\t{error}\n")
        print(f"Failed pairs written to {fail_log}; re-run to retry them.")

    if interrupted:
        # 130 is the conventional "killed by SIGINT" status, so a wrapping shell script or
        # Slurm job sees this as an interruption rather than a clean finish.
        sys.exit(130)
