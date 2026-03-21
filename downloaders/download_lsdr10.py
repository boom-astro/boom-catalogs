"""Script to download Legacy Survey DR10.1 catalog parquet files in parallel via direct HTTP."""
import os
import time
import argparse
import requests
import pyarrow.parquet as pq
from io import BytesIO

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

CATALOG_BASE = "https://data.lsdb.io/hats/legacysurvey_dr10.1/legacysurvey_dr10.1"
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
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            # Read only the columns we need, rename to lowercase
            table = pq.read_table(BytesIO(resp.content), columns=LSDR10_COLUMNS)
            table = table.rename_columns([c.lower() for c in table.column_names])
            pq.write_table(table, out_path)
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
