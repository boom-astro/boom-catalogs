"""Script to download all Legacy Survey DR10 tractor FITS files in parallel."""
import os
import json
import argparse
import requests
from bs4 import BeautifulSoup

from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./legacysurvey_tractor/
OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/ls_dr10_tractor/"

parser = argparse.ArgumentParser(description="Download all Legacy Survey DR10 tractor FITS files in parallel.")
parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="Directory to save downloaded files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")
parser.add_argument("--refresh-cache", action="store_true", help="Force refresh the URL cache")

def get_cache_path(output_dir):
    """Get the path to the URL cache file."""
    return os.path.join(output_dir, ".url_cache.json")

def load_urls_from_cache(cache_path):
    """Load URLs from cache file if it exists."""
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
                print(f"Loaded {len(data)} URLs from cache")
                return data
        except Exception as e:
            print(f"Error loading cache: {e}")
    return None

def save_urls_to_cache(urls, cache_path):
    """Save URLs to cache file."""
    try:
        with open(cache_path, 'w') as f:
            json.dump(urls, f)
        print(f"Cached {len(urls)} URLs to {cache_path}")
    except Exception as e:
        print(f"Error saving cache: {e}")

def get_subdirectories(main_url="https://portal.nersc.gov/cfs/cosmo/data/legacysurvey/dr10/south/tractor/"):
    """Fetch the main directory and get all subdirectory URLs."""
    try:
        response = requests.get(main_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        subdirs = []
        for link in soup.find_all('a'):
            href = link.get('href')
            # Look for directory links (typically end with /, and a number that is left padded with zeros to 3 digits)
            if (
                href and href.endswith('/')
                and href not in ['../', '/']
                and len(href.strip('/')) == 3 and href.strip('/').isdigit()
            ):
                # Skip parent directory
                if href != '../':
                    subdirs.append(main_url + href)
        return subdirs
    except Exception as e:
        print(f"Error fetching subdirectories: {e}")
        return []

def get_tractor_files(subdir_url):
    """Fetch all tractor FITS files from a subdirectory."""
    try:
        response = requests.get(subdir_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        files = []
        for link in soup.find_all('a'):
            href = link.get('href')
            # Look for tractor FITS files
            if href and href.startswith('tractor-') and href.endswith('.fits'):
                files.append(subdir_url + href)
        return files
    except Exception as e:
        print(f"Error fetching files from {subdir_url}: {e}")
        return []

def get_urls(main_url="https://portal.nersc.gov/cfs/cosmo/data/legacysurvey/dr10/south/tractor/"):
    """Get all tractor file URLs from all subdirectories."""
    print("Fetching subdirectories...")
    subdirs = get_subdirectories(main_url)
    print(f"Found {len(subdirs)} subdirectories")
    
    urls = []
    # for subdir in tqdm(subdirs, desc="Fetching URLs from subdirectories"):
    #     files = get_tractor_files(subdir)
    #     urls.extend(files)
    # let's parallelize the fetching of files from subdirectories
    with Pool(processes=min(len(subdirs), cpu_count() - 2)) as pool:
        results = list(tqdm(pool.imap(get_tractor_files, subdirs), total=len(subdirs), desc="Fetching URLs from subdirectories"))
        for files in results:
            urls.extend(files)
    
    return urls

def download_file(arguments):
    url, output_dir, base_url = arguments
    # Extract relative path from URL by removing base URL
    relative_path = url.replace(base_url, '')
    output_path = os.path.join(output_dir, relative_path)
    
    # Create subdirectory structure if needed
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        
        # if the file already exists and is complete, skip downloading
        if os.path.exists(output_path):
            if os.path.getsize(output_path) == total_size:
                return
            os.remove(output_path)
        
        with open(output_path, 'wb') as file:
            for data in response.iter_content(chunk_size=8192):
                file.write(data)
    except Exception as e:
        print(f"Error downloading {url}: {e}")

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    nb_processes = min(args.processes, cpu_count() - 2)
    
    os.makedirs(output_dir, exist_ok=True)

    cache_path = get_cache_path(output_dir)
    
    # Base URL for extracting relative paths
    base_url = "https://portal.nersc.gov/cfs/cosmo/data/legacysurvey/dr10/south/tractor/"
    
    # Try to load from cache unless --refresh-cache is specified
    if not args.refresh_cache:
        urls = load_urls_from_cache(cache_path)
    else:
        urls = None
    
    # If cache miss or refresh requested, fetch URLs from the server
    if urls is None:
        urls = get_urls(base_url)
        save_urls_to_cache(urls, cache_path)
    
    print(f"Found {len(urls)} files to download (using {nb_processes} processes)...")

    if urls:
        with tqdm(total=len(urls)) as pbar:
            with Pool(processes=nb_processes) as pool:
                for _ in pool.imap_unordered(
                    download_file,
                    [(url, output_dir, base_url) for url in urls]
                ):
                    pbar.update()
        print(f"Download complete! Files saved to {output_dir}")
    else:
        print("No files found to download.")
