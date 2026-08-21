"""Script to download the eROSITA eRASS1 main catalog and extract the FITS file."""
import requests
import os
import argparse
import tarfile

from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./erosita/
EROSITA_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/erosita/"

parser = argparse.ArgumentParser(description="Download the eROSITA eRASS1 main catalog.")
parser.add_argument("--output-dir", type=str, default=EROSITA_OUTPUT_DIR, help="Directory to save the downloaded file")

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    url = "https://erosita.mpe.mpg.de/dr1/AllSkySurveyData_dr1/Catalogues_dr1/MerloniA_DR1/eRASS1_Main.v1.2.fits.tar.gz"
    filename = "eRASS1_Main.v1.2.fits.tar.gz"
    tar_path = os.path.join(output_dir, filename)
    fits_path = os.path.join(output_dir, "eRASS1_Main.v1.2.fits")

    if os.path.exists(fits_path):
        print("FITS file already exists. Skipping download and extraction.")
        exit(0)

    response = requests.get(url, stream=True, allow_redirects=True)
    total_size = int(response.headers.get('content-length', 0))

    if os.path.exists(tar_path):
        if os.path.getsize(tar_path) == total_size:
            print("Archive already downloaded. Skipping download.")
        else:
            os.remove(tar_path)

    if not os.path.exists(tar_path):
        with open(tar_path, 'wb') as file:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as pbar:
                for data in response.iter_content(chunk_size=1024):
                    file.write(data)
                    pbar.update(len(data))

    print("Extracting...")
    with tarfile.open(tar_path) as tar:
        tar.extractall(path=output_dir)
    os.remove(tar_path)
    print(f"Done. FITS file saved to {fits_path}")
