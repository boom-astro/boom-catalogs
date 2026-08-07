"""Script to download NED LVS."""
import os
import re
import requests
import argparse

from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./ned_lvs/
NED_LVS_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/ned_lvs/"

parser = argparse.ArgumentParser(description="Download NED LVS.")
parser.add_argument("--output-dir", type=str, default=NED_LVS_OUTPUT_DIR, help="Directory to save the downloaded file")

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    # "Current" always points at the latest release. Since the 2026-04-24 release it
    # carries the galaxy angular diameter columns (Diam, Diam_ba, Diam_pa, ...).
    url = "https://ned.ipac.caltech.edu/NED::LVS/fits/Current/"
    output_path = os.path.join(output_dir, "ned_lvs.fits")

    # ask for the size/release of the current file before downloading a gigabyte of it
    head = requests.head(url, allow_redirects=True)
    head.raise_for_status()
    total_size = int(head.headers.get('content-length', 0))
    # NED serves the release-stamped name (e.g. NEDLVS_20260424.fits) here
    release = re.search(r'filename=([^\s;]+)', head.headers.get('content-disposition', ''))
    print(f"NED LVS current release: {release.group(1) if release else 'unknown'}")

    # if the file already exists and is complete, skip downloading
    if os.path.exists(output_path):
        if os.path.getsize(output_path) == total_size:
            print("File already exists and is complete. Skipping download.")
            exit(0)
        os.remove(output_path)

    # download to a partial file so an interrupted run isn't mistaken for a complete one
    partial_path = f"{output_path}.part"
    response = requests.get(url, stream=True, allow_redirects=True)
    response.raise_for_status()
    with open(partial_path, 'wb') as file:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=output_path) as pbar:
            for data in response.iter_content(chunk_size=1024):
                file.write(data)
                pbar.update(len(data))
    os.rename(partial_path, output_path)
