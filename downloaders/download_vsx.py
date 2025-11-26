"""Script to download VSX."""
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "requests",
#     "tqdm",
# ]
# ///

import requests
from tqdm import tqdm
import os
import argparse

parser = argparse.ArgumentParser(description="Download VSX.")
parser.add_argument("--output-dir", type=str, default="./vsx/", help="Directory to save the downloaded file")

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    url = "https://cdsarc.cds.unistra.fr/ftp/B/vsx/vsx.dat"
    output_path = os.path.join(output_dir, "vsx.dat")

    response = requests.get(url, stream=True)
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