"""Script to download milliquas."""
import requests
import os
import argparse

from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./milliquas/
MILLIQUAS_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/milliquas/"

parser = argparse.ArgumentParser(description="Download milliquas.")
parser.add_argument("--output-dir", type=str, default=MILLIQUAS_OUTPUT_DIR, help="Directory to save the downloaded file")

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    url = "https://quasars.org/milliquas.fits.zip"
    output_path = os.path.join(output_dir, "milliquas.fits.zip")
    # we need to mimick a browser to avoid 403 errors
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
    response = requests.get(url, headers=headers, stream=True, allow_redirects=True)
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