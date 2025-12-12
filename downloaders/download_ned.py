"""Script to download NED LVS."""
import os
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

    url = "https://ned.ipac.caltech.edu/NED::LVS/fits/Current/"
    output_path = os.path.join(output_dir, "ned_lvs.fits")

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