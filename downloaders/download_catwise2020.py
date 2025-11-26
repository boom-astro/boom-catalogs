"""Script to download all catWISE 2020 files in parallel."""
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "requests",
#     "tqdm",
# ]
# ///

import requests
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import os
import argparse

parser = argparse.ArgumentParser(description="Download all catWISE 2020 files in parallel.")
parser.add_argument("--output-dir", type=str, default="./catwise2020_files/", help="Directory to save downloaded files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")

def collect_from_dir(arguments):
    main_url, dir_name = arguments
    dir_url = main_url + dir_name
    dir_response = requests.get(dir_url)
    dir_lines = dir_response.text.splitlines()
    dir_urls = []
    for line in dir_lines:
        if 'href="' in line:
            start = line.index('href="') + len('href="')
            end = line.index('"', start)
            filename = line[start:end]
            if filename.endswith('.tbl.gz'):
                dir_urls.append(dir_url + filename)
    return dir_urls

def get_urls(main_url = "https://portal.nersc.gov/project/cosmo/data/CatWISE/2020/", nb_processes=8):
    response = requests.get(main_url)
    lines = response.text.splitlines()
    urls = []
    top_level_dirs = []
    for line in lines:
        if 'href="' in line:
            start = line.index('href="') + len('href="')
            end = line.index('"', start)
            filename = line[start:end]
            if filename.endswith('/'):
                top_level_dirs.append(filename)
    with tqdm(total=len(top_level_dirs), desc="Collecting URLs") as pbar:
        with Pool(processes=nb_processes) as pool:
            for sublist in pool.imap_unordered(
                collect_from_dir,
                [(main_url, dir_name) for dir_name in top_level_dirs]
            ):
                urls.extend(sublist)
                pbar.update()
    return urls

def download_file(arguments):
    url, output_dir = arguments
    # output_path = os.path.join(output_dir, url.split('/')[-1])
    # we want to preserve the directory structure
    relative_path = url.replace("https://portal.nersc.gov/project/cosmo/data/CatWISE/2020/", "")
    output_path = os.path.join(output_dir, relative_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
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