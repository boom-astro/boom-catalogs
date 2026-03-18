"""Script to download all AllWISE source parquet files in parallel."""
import os
import time
import argparse
import lsdb

from tqdm import tqdm
# multiprocessing pool doesn't work well with lsdb
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./allwise_files/
ALLWISE_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/allwise_files/"

ALLWISE_COLUMNS = [
    "source_id",
    "ra",
    "dec",
    "sigra",
    "sigdec",
    "w1mpro",
    "w2mpro",
    "w3mpro",
    "w4mpro",
    "w1sigmpro",
    "w2sigmpro",
    "w3sigmpro",
    "w4sigmpro",
    "w1rchi2",
    "w2rchi2",
    "pmra",
    "pmdec",
    "sigpmra",
    "sigpmdec",
]

parser = argparse.ArgumentParser(description="Download all AllWISE source parquet files in parallel.")
parser.add_argument("--output-dir", type=str, default=ALLWISE_OUTPUT_DIR, help="Directory to save downloaded files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")

def download_partition(arguments):
    pixel_order, pixel_pixel, output_dir = arguments
    out_path = os.path.join(output_dir, f"batch_order{pixel_order}_pix{pixel_pixel}.parquet")
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return
    for attempt in range(5):
        try:
            catalog = lsdb.open_catalog(
                'https://data.lsdb.io/hats/wise/allwise',
                columns=ALLWISE_COLUMNS,
            )
            partition_df = catalog.get_partition(pixel_order, pixel_pixel).compute()
            partition_df.to_parquet(out_path)
            del partition_df
            return
        except Exception as e:
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
        'https://data.lsdb.io/hats/wise/allwise',
        columns=ALLWISE_COLUMNS,
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
