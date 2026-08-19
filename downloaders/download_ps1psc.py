"""Script to download all of PS1 PSC's FITS files in parallel, with resume support."""
import os
import time
import argparse
import requests

from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./ps1_psc/
PS1_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/ps1_psc/"

parser = argparse.ArgumentParser(description="Download all of PS1 PSC's FITS files in parallel.")
parser.add_argument("--output-dir", type=str, default=PS1_OUTPUT_DIR, help="Directory to save downloaded files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")
parser.add_argument("--retries", type=int, default=5, help="Number of retry attempts per file on failure")

def get_urls(main_url="https://archive.stsci.edu/hlsps/ps1-psc/"):
    response = requests.get(main_url)
    lines = response.text.splitlines()
    urls = []
    for line in lines:
        if 'href="' in line:
            start = line.index('href="') + len('href="')
            end = line.index('"', start)
            filename = line[start:end]
            if filename.endswith('_cat.fits'):
                urls.append(main_url + filename)
    return urls

def download_file(arguments):
    url, output_dir, tries = arguments
    name = url.split('/')[-1]
    dest = os.path.join(output_dir, name)
    part = dest + ".part"

    if os.path.exists(dest):
        return None

    for attempt in range(tries):
        have = os.path.getsize(part) if os.path.exists(part) else 0
        headers = {"Range": f"bytes={have}-"} if have else {}
        try:
            r = requests.get(url, headers=headers, stream=True, timeout=(10, 120))
            if r.status_code == 416:  # already complete
                break
            if have and r.status_code == 200:  # server ignored Range, restart cleanly
                have = 0
                os.remove(part)
            r.raise_for_status()
            with open(part, "ab" if have else "wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
            os.rename(part, dest)
            return None
        except requests.RequestException:
            time.sleep(2 ** attempt)
    return name

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    nb_processes = min(args.processes, cpu_count() - 2)

    os.makedirs(output_dir, exist_ok=True)

    urls = get_urls()
    print(f"Found {len(urls)} files to download (using {nb_processes} processes)...")

    failures = []
    with tqdm(total=len(urls)) as pbar:
        with Pool(processes=nb_processes) as pool:
            for failed in pool.imap_unordered(
                download_file,
                [(url, output_dir, args.retries) for url in urls]
            ):
                if failed:
                    failures.append(failed)
                pbar.update()

    if failures:
        print(f"Failed to download {len(failures)} file(s) after {args.retries} attempts:")
        for name in failures:
            print(f"  - {name}")
