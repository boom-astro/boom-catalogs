"""Script to download Legacy Survey DR10.1 catalog parquet files in parallel via direct HTTP."""
import os
import time
import argparse
import requests
import fsspec
import aiohttp
import pyarrow.parquet as pq

from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from dotenv import load_dotenv

load_dotenv()
LSDR10_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/ls_dr10/"

LSDR10_COLUMNS = [
    # key columns: OBJID is only unique within a brick, so RELEASE+BRICKID
    # are required to build a unique _id
    "RELEASE",
    "BRICKID",
    "OBJID",
    "TYPE",
    "RA",
    "DEC",
    "EBV",
    "Z_SPEC",
    "SURVEY",
    "Z_PHOT_MEAN",
    "Z_PHOT_MEDIAN",
    "Z_PHOT_STD",
    "Z_PHOT_L95",
    "Z_PHOT_U95",
    "FLUX_G",
    "FLUX_R",
    "FLUX_I",
    "FLUX_Z",
    "FLUX_W1",
    "FLUX_W2",
    "FLUX_W3",
    "FLUX_W4",
]

CATALOG_BASE = "https://data.lsdb.io/hats/legacysurvey_dr10.1/legacysurvey"
DATASET_URL = f"{CATALOG_BASE}/dataset"

parser = argparse.ArgumentParser(description="Download Legacy Survey DR10.1 catalog parquet files in parallel.")
parser.add_argument("--output-dir", type=str, default=LSDR10_OUTPUT_DIR, help="Directory to save downloaded files")
parser.add_argument("--processes", type=int, default=32, help="Number of parallel download processes")


def get_partitions():
    """Fetch partition list from HATS partition_info.csv."""
    url = f"{CATALOG_BASE}/partition_info.csv"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    lines = resp.text.strip().split("\n")
    header = lines[0].split(",")
    order_idx = header.index("Norder")
    pixel_idx = header.index("Npix")
    partitions = []
    for line in lines[1:]:
        fields = line.split(",")
        partitions.append((int(fields[order_idx]), int(fields[pixel_idx])))
    return partitions


# Timeouts matter here: without them a stalled connection parks a worker thread
# forever instead of falling through to the retry loop below.
HTTP_FS = fsspec.filesystem(
    "http",
    client_kwargs={
        "timeout": aiohttp.ClientTimeout(total=600, sock_connect=30, sock_read=60)
    },
)


def download_partition(arguments):
    order, pixel, output_dir = arguments
    out_path = os.path.join(output_dir, f"batch_order{order}_pix{pixel}.parquet")
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return
    # HATS URL pattern: Norder=N/Dir=D/Npix=P.parquet
    dir_val = (pixel // 10000) * 10000
    url = f"{DATASET_URL}/Norder={order}/Dir={dir_val}/Npix={pixel}.parquet"
    for attempt in range(5):
        try:
            # Read over HTTP range requests so only the columns we need cross the
            # wire; the full partition is ~20x larger than the subset we keep.
            with HTTP_FS.open(url, "rb") as handle:
                table = pq.read_table(handle, columns=LSDR10_COLUMNS)
            table = table.rename_columns([c.lower() for c in table.column_names])
            tmp_path = out_path + ".part"
            pq.write_table(table, tmp_path)
            os.replace(tmp_path, out_path)
            del table
            return
        except Exception as e:
            if attempt < 4:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(
                    f"Failed partition order={order} pixel={pixel} after 5 attempts: {e}"
                ) from e


if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    nb_processes = min(args.processes, cpu_count() - 2)

    os.makedirs(output_dir, exist_ok=True)

    print("Fetching partition list...")
    partitions = get_partitions()
    print(f"Found {len(partitions)} partitions to download.")

    with tqdm(total=len(partitions)) as pbar:
        with ThreadPoolExecutor(max_workers=nb_processes) as pool:
            futures = [
                pool.submit(download_partition, (order, pixel, output_dir))
                for order, pixel in partitions
            ]
            for future in futures:
                future.result()
                pbar.update()
