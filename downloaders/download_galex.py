"""Script to download all Galex source CSV files in parallel."""
import os
import argparse
import requests
from bs4 import BeautifulSoup

from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./galex_files/
GALEX_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/galex_files/"

parser = argparse.ArgumentParser(description="Download all Galex source CSV files in parallel.")
parser.add_argument("--output-dir", type=str, default=GALEX_OUTPUT_DIR, help="Directory to save downloaded files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")

def get_urls(url: str = "http://dolomiti.pha.jhu.edu/uvsky/GUVcat/GUVcat_AIS.html"):
    """
    Parse the GUVcat webpage to extract all CSV.gz download URLs.
    
    Args:
        url: The URL of the GUVcat page (default is the AIS catalog page)
    
    Returns:
        A list of URLs pointing to CSV.gz files
    """
    # Fetch the webpage
    response = requests.get(url)
    response.raise_for_status()
    
    # Parse the HTML
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all links that contain "Download csv.gz" text
    csv_urls = []
    for link in soup.find_all('a'):
        link_text = link.get_text(strip=True)
        if 'Download csv.gz' in link_text:
            href = link.get('href')
            if href and href.endswith('.csv.gz'):
                csv_urls.append(href)
    
    return csv_urls

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