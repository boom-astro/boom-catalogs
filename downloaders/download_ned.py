"""Script to download NED LVS."""
import os
import re
import time
import requests
import argparse

from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./ned_lvs/
NED_LVS_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/ned_lvs/"

# the file is ~1.2GB, so read it in 1MB chunks rather than 1KB
CHUNK_SIZE = 1024 * 1024
MAX_ATTEMPTS = 5

parser = argparse.ArgumentParser(description="Download NED LVS.")
parser.add_argument("--output-dir", type=str, default=NED_LVS_OUTPUT_DIR, help="Directory to save the downloaded file")
parser.add_argument("--attempts", type=int, default=MAX_ATTEMPTS, help="Number of download attempts before giving up")

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

    # download to a partial file so an interrupted run isn't mistaken for a complete one,
    # and so a retry can pick up where the previous attempt stopped
    partial_path = f"{output_path}.part"
    for attempt in range(args.attempts):
        try:
            downloaded = os.path.getsize(partial_path) if os.path.exists(partial_path) else 0
            # NED advertises "Accept-Ranges: bytes", so ask for the rest of the file only
            headers = {"Range": f"bytes={downloaded}-"} if downloaded else {}
            response = requests.get(url, stream=True, allow_redirects=True, headers=headers)
            # a server that ignores Range replies 200 with the whole file, so start over
            if downloaded and response.status_code != requests.codes.partial_content:
                downloaded = 0
            response.raise_for_status()

            with open(partial_path, 'ab' if downloaded else 'wb') as file:
                with tqdm(total=total_size, initial=downloaded, unit='B', unit_scale=True, desc=output_path) as pbar:
                    for data in response.iter_content(chunk_size=CHUNK_SIZE):
                        file.write(data)
                        pbar.update(len(data))

            size = os.path.getsize(partial_path)
            if total_size and size != total_size:
                raise IOError(f"Downloaded {size} bytes, expected {total_size}")
            break
        except Exception as e:
            if attempt == args.attempts - 1:
                raise RuntimeError(f"Failed to download NED LVS after {args.attempts} attempts: {e}") from e
            delay = 2 ** attempt
            print(f"Attempt {attempt + 1} failed ({e}); retrying in {delay}s...")
            time.sleep(delay)

    os.rename(partial_path, output_path)
