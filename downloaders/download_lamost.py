"""Script to download LAMOST http://www.lamost.org/dr11/v2.0/catalogue."""
import os
import gzip
import shutil
import argparse
import requests

from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./lamost/
OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/lamost/"

parser = argparse.ArgumentParser(description="Download LAMOST DR10 stellar parameter catalog.")
parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="Directory to save downloaded files")

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir

    os.makedirs(output_dir, exist_ok=True)

    url_low_resolution = "https://www.lamost.org/dr11/v2.0/catdl?name=dr11_v2.0_LRS_catalogue.csv.gz"
    url_medium_resolution = "https://www.lamost.org/dr11/v2.0/medcas/catdl?name=dr11_v2.0_MRS_catalogue.csv.gz"
    url = url_low_resolution
    gz_path = os.path.join(output_dir, "lamost.csv.gz")
    output_path = os.path.join(output_dir, "lamost.csv")

    # Skip if already decompressed
    if os.path.exists(output_path):
        print(f"File already exists: {output_path}")
        exit(0)

    # Check if gz file already exist
    headers = {}
    mode = 'wb'
    initial_size = 0
    if os.path.exists(gz_path):
        initial_size = os.path.getsize(gz_path)
        headers['Range'] = f'bytes={initial_size}-'
        mode = 'ab'

    response = requests.get(url, stream=True, headers=headers)

    # If server doesn't support range requests, start from scratch
    if response.status_code == 200:
        mode = 'wb'
        initial_size = 0
    total_size = int(response.headers.get('content-length', 0)) + initial_size

    # Check if download already complete
    if initial_size == total_size and total_size > 0:
        print(f"Download already complete: {gz_path}")
    else:
        print(f"Downloading {total_size / (1024**3):.2f} GB to: {gz_path}")
        with open(gz_path, mode) as file:
            with tqdm(total=total_size, initial=initial_size, unit='B', unit_scale=True) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        pbar.update(len(chunk))

    # Decompress
    print(f"Decompressing to: {output_path}")
    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Remove gz file
    os.remove(gz_path)
    print("Done.")