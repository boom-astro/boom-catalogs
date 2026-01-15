"""Script to download all 2MASS PSC source CSV files in parallel."""
import os
import argparse
import requests

from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./2mass_files/
TWOMASS_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/2mass_files/"

parser = argparse.ArgumentParser(description="Download all 2MASS PSC source CSV files in parallel.")
parser.add_argument("--output-dir", type=str, default=TWOMASS_OUTPUT_DIR, help="Directory to save downloaded files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")

def get_urls(main_url = "https://irsa.ipac.caltech.edu/2MASS/download/allsky/"):
    response = requests.get(main_url)
    lines = response.text.splitlines()
    urls = []
    for line in lines:
        if 'href="' in line:
            start = line.index('href="') + len('href="')
            end = line.index('"', start)
            filename = line[start:end]
            if filename.startswith('psc_') and filename.endswith('.gz'):
                urls.append(main_url + filename)
    return urls

def download_file(arguments):
    url, output_dir = arguments
    output_path = os.path.join(output_dir, url.split('/')[-1])
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    # if the file already exists and is complete, skip downloading
    if os.path.exists(output_path):
        if os.path.getsize(output_path) == total_size:
            return
        os.remove(output_path)
    with open(output_path, 'wb') as file:
        for data in response.iter_content(chunk_size=1024):
            file.write(data)

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    nb_processes = min(args.processes, cpu_count() - 2)
    
    os.makedirs(output_dir, exist_ok=True)

    urls = get_urls()
    print(f"Found {len(urls)} files to download.")

    with tqdm(total=len(urls)) as pbar:
        with Pool(processes=nb_processes) as pool:
            for _ in pool.imap_unordered(
                download_file,
                [(url, output_dir) for url in urls]
            ):
                pbar.update()