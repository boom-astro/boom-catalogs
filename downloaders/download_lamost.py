"""Script to download LAMOST http://www.lamost.org/dr10/v2.0/catalogue."""
import os
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
    url_medium_resolution = "https://www.lamost.org//dr10/v2.0/medcas/catdl?name=dr10_v2.0_MRS_catalogue.csv.gz"
    url = url_medium_resolution  # Change to the desired URL
    output_path = os.path.join(output_dir, "lamost_medium_resolution.csv")

    # Check if file already exists
    headers = {}
    mode = 'wb'
    initial_size = 0
    if os.path.exists(output_path):
        initial_size = os.path.getsize(output_path)
        headers['Range'] = f'bytes={initial_size}-'
        mode = 'ab'

    response = requests.get(url, stream=True, headers=headers)

    # If server doesn't support range requests, start from scratch
    if response.status_code == 200:
        mode = 'wb'
        initial_size = 0
    total_size = int(response.headers.get('content-length', 0)) + initial_size

    # Check if already complete
    if initial_size == total_size and total_size > 0:
        print(f"File already complete: {output_path}")
        exit(0)

    print(f"Downloading {total_size / (1024**3):.2f} GB to: {output_path}")

    with open(output_path, mode) as file:
        with tqdm(total=total_size, initial=initial_size, unit='B', unit_scale=True) as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
                    pbar.update(len(chunk))

    print("Download completed successfully.")