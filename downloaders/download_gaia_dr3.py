"""Script to download all Gaia DR3 source CSV files in parallel."""
import os
import argparse
import requests

from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()
# Retrieve output directory from environment variable or use ./gaia_dr3_files/
GAIA_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/gaia_dr3_files/"

parser = argparse.ArgumentParser(description="Download all Gaia DR3 source CSV files in parallel.")
parser.add_argument("--output-dir", type=str, default=GAIA_OUTPUT_DIR, help="Directory to save downloaded files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")

def get_session():
    """Create a requests session with retry logic."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,  # wait 2, 4, 8, 16, 32 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def get_urls(main_url = "https://sdsc-users.flatironinstitute.org/~gaia/dr3/csv/GaiaSource/"):
    session = get_session()
    response = session.get(main_url, timeout=10)
    lines = response.text.splitlines()
    urls = []
    for line in lines:
        if 'href="' in line:
            start = line.index('href="') + len('href="')
            end = line.index('"', start)
            filename = line[start:end]
            if filename.endswith('.csv.gz'):
                urls.append(main_url + filename)

    if not urls:
        # let's try to parse with beautifulsoup if no urls found
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and href.endswith('.csv.gz'):
                urls.append(main_url + href)
    return urls

def download_file(arguments):
    url, output_dir = arguments
    output_path = os.path.join(output_dir, url.split('/')[-1])
    session = get_session()
    try:
        # First, get the file size with a HEAD request
        head_response = session.head(url, timeout=30)
        total_size = int(head_response.headers.get('content-length', 0))

        # Skip if file already exists and is complete
        if os.path.exists(output_path):
            if os.path.getsize(output_path) == total_size:
                return {"status": "skipped", "url": url}
            os.remove(output_path)

        # Download the file
        response = session.get(url, stream=True, timeout=120)
        response.raise_for_status()

        with open(output_path, 'wb') as file:
            for data in response.iter_content(chunk_size=8192):
                file.write(data)

        return {"status": "success", "url": url}

    except Exception as e:
        return {"status": "error", "url": url, "error": str(e)}

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    nb_processes = min(args.processes, cpu_count() - 2)
    
    os.makedirs(output_dir, exist_ok=True)

    urls = get_urls()
    print(f"Found {len(urls)} files to download.")

    errors = []
    with tqdm(total=len(urls)) as pbar:
        with Pool(processes=nb_processes) as pool:
            for result in pool.imap_unordered(
                    download_file,
                    [(url, output_dir) for url in urls]
            ):
                pbar.update()
                if result and result["status"] == "error":
                    errors.append(result)

    # Report errors at the end
    if errors:
        print(f"\n{len(errors)} files failed to download:")
        for err in errors:
            print(f"  - {err['url']}: {err['error']}")