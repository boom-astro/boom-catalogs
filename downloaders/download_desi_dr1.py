"""Script to download DESI DR1 zcatalog (zall-tilecumulative-iron.fits)."""
import requests
import os
import argparse

from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./desi_dr1/
DESI_DR1_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/desi_dr1/"

parser = argparse.ArgumentParser(description="Download DESI DR1 zcatalog.")
parser.add_argument("--output-dir", type=str, default=DESI_DR1_OUTPUT_DIR, help="Directory to save the downloaded file")

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    url = "https://data.desi.lbl.gov/public/dr1/spectro/redux/iron/zcatalog/v1/zall-tilecumulative-iron.fits"
    output_path = os.path.join(output_dir, "zall-tilecumulative-iron.fits")
    response = requests.get(url, stream=True, allow_redirects=True)
    total_size = int(response.headers.get('content-length', 0))
    # if the file already exists and is complete, skip downloading
    if os.path.exists(output_path):
        if os.path.getsize(output_path) == total_size:
            print("File already exists and is complete. Skipping download.")
            exit(0)
        os.remove(output_path)
    with open(output_path, 'wb') as file:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=output_path) as pbar:
            for data in response.iter_content(chunk_size=1024):
                file.write(data)
                pbar.update(len(data))
