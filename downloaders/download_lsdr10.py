"""Script to download Legacy Survey DR10.1 catalog parquet files in parallel."""
import os
import time
import argparse
import threading
import lsdb

from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from dotenv import load_dotenv

load_dotenv()
LSDR10_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/ls_dr10/"

LSDR10_COLUMNS = [
    "OBJID",
    "SHAPE_R",
    "SHAPE_R_IVAR",
    "SHAPE_E1",
    "SHAPE_E1_IVAR",
    "SHAPE_E2",
    "SHAPE_E2_IVAR",
    "RA",
    "TYPE",
    "DEC",
    "FLUX_R",
    "NOBS_G",
    "NOBS_R",
    "NOBS_Z",
    "FITBITS",
    "RA_IVAR",
    "DEC_IVAR",
    "MW_TRANSMISSION_R",
    "Z_PHOT_MEAN",
    "Z_PHOT_STD",
    "Z_SPEC",
]

CATALOG_URL = "https://data.lsdb.io/hats/legacysurvey_dr10.1"

parser = argparse.ArgumentParser(description="Download Legacy Survey DR10.1 catalog parquet files in parallel.")
parser.add_argument("--output-dir", type=str, default=LSDR10_OUTPUT_DIR, help="Directory to save downloaded files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")

# Thread-local storage so each thread opens the catalog once
_thread_local = threading.local()

def _get_catalog():
    if not hasattr(_thread_local, "catalog"):
        _thread_local.catalog = lsdb.open_catalog(
            CATALOG_URL,
            columns=LSDR10_COLUMNS,
        )
    return _thread_local.catalog

def download_partition(arguments):
    pixel_order, pixel_pixel, output_dir = arguments
    out_path = os.path.join(output_dir, f"batch_order{pixel_order}_pix{pixel_pixel}.parquet")
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return
    for attempt in range(5):
        try:
            catalog = _get_catalog()
            partition_df = catalog.get_partition(pixel_order, pixel_pixel).compute()
            # rename columns to lowercase
            partition_df.columns = [c.lower() for c in partition_df.columns]
            partition_df.to_parquet(out_path)
            del partition_df
            return
        except Exception as e:
            # reset catalog on failure so next attempt gets a fresh one
            _thread_local.catalog = None
            if attempt < 4:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(
                    f"Failed partition order={pixel_order} pixel={pixel_pixel} after 5 attempts: {e}"
                ) from e

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    nb_processes = min(args.processes, cpu_count() - 2)

    os.makedirs(output_dir, exist_ok=True)

    catalog = lsdb.open_catalog(
        CATALOG_URL,
        columns=LSDR10_COLUMNS,
    )
    pixels = catalog.get_healpix_pixels()
    print(f"Found {len(pixels)} partitions to download.")

    with tqdm(total=len(pixels)) as pbar:
        with ThreadPoolExecutor(max_workers=nb_processes) as pool:
            futures = [
                pool.submit(download_partition, (pixel.order, pixel.pixel, output_dir))
                for pixel in pixels
            ]
            for future in futures:
                future.result()
                pbar.update()
