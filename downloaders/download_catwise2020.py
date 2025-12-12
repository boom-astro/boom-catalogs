"""Script to download all catWISE 2020 files in parallel."""
import requests
import os
import argparse
import pandas as pd
import gzip

from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from io import StringIO
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./catwise2020_files/
CATWISE_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/catwise2020_files/"

parser = argparse.ArgumentParser(description="Download all catWISE 2020 files in parallel.")
parser.add_argument("--output-dir", type=str, default=CATWISE_OUTPUT_DIR, help="Directory to save downloaded files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")
parser.add_argument("--convert-to-parquet", action='store_true', help="Convert downloaded .tbl.gz files to parquet format")

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

def get_urls(main_url = "https://portal.nersc.gov/project/cosmo/data/CatWISE/2020/", output_dir="./catwise2020_files/", nb_processes=8):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    if os.path.exists(os.path.join(output_dir, "catwise2020_file_urls.txt")):
        print("Found existing URL list, loading from file.")
        with open(os.path.join(output_dir, "catwise2020_file_urls.txt"), 'r') as f:
            urls = [line.strip() for line in f.readlines()]
        return urls
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

    # save the list of urls to a text file
    with open(os.path.join(output_dir, "catwise2020_file_urls.txt"), 'w') as f:
        for url in urls:
            f.write(url + '\n')
    return urls

def download_file(arguments):
    url, output_dir, convert_to_parquet = arguments
    relative_path = url.replace("https://portal.nersc.gov/project/cosmo/data/CatWISE/2020/", "")
    output_path = os.path.join(output_dir, relative_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    parquet_path = output_path.replace('.tbl.gz', '.parquet')
    if convert_to_parquet and os.path.exists(parquet_path):
        if os.path.getsize(parquet_path) > 0:
            return
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    # if the file already exists and is complete, skip downloading
    download = True
    if os.path.exists(output_path):
        if os.path.getsize(output_path) == total_size:
            if not convert_to_parquet:
                return
            download = False
        else:
            os.remove(output_path)
    if download:
        with open(output_path, 'wb') as file:
            for data in response.iter_content(chunk_size=1024):
                file.write(data)

    if convert_to_parquet and output_path.endswith('.tbl.gz'):
        # the catwise .tbl.gz files star with a number of comments lines starting with '\'
        # then we have one line with all the column names separated by | (padded with spaces)
        # then one line with all the data types separated by | (padded with spaces)
        # then one line with all the units separated by | (padded with spaces)
        # then one line with whether or not the column can have nulls separated by | (padded with spaces)
        # then the data rows separated by spaces (not |)
        # let's skip all the lines that start with '\', column names and types
        # and finally read the data into a pandas DataFrame
        # it's a gzipped file, so when we use with open, it will handle the decompression
        try:
            with gzip.open(output_path, 'rt') as f:
                lines = f.readlines()
            data_start_idx = 0
            for i, line in enumerate(lines):
                if not line.startswith('\\'):
                    data_start_idx = i
                    break
            column_names = [col.strip() for col in lines[data_start_idx].strip().split('|')][1:-1]
            data_types = [dtype.strip() for dtype in lines[data_start_idx + 1].strip().split('|')][1:-1]
            dtype_map = {}
            for col, dtype in zip(column_names, data_types):
                dtype = dtype.lower().strip()
                if dtype in ['int', 'integer', 'long', 'i']:
                    dtype_map[col] = 'Int64'
                elif dtype in ['float', 'double', 'real', 'r']:
                    dtype_map[col] = 'float64'
                elif dtype == 'boolean':
                    dtype_map[col] = 'boolean'
                elif dtype in ['char', 'string']:
                    dtype_map[col] = 'string'
                else:
                    dtype_map[col] = 'string'

            data_lines = lines[data_start_idx + 4:]
            data_str = '\n'.join([line.strip() for line in data_lines if line.strip() != ''])
            df = pd.read_csv(StringIO(data_str), names=column_names, dtype=dtype_map, na_values=['NULL', 'null', 'NaN', 'nan', 'n', ''], sep='\\s+')
            df.to_parquet(parquet_path, index=False)
            os.remove(output_path)
        except Exception as e:
            print(f"Error processing file {output_path}: {e}")

if __name__ == "__main__":
    args = parser.parse_args()
    output_dir = args.output_dir
    nb_processes = min(args.processes, cpu_count() - 2)
    convert_to_parquet = args.convert_to_parquet
    
    os.makedirs(output_dir, exist_ok=True)

    urls = get_urls(output_dir=output_dir, nb_processes=nb_processes)
    print(f"Found {len(urls)} files to download.")

    with tqdm(total=len(urls)) as pbar:
        with Pool(processes=nb_processes) as pool:
            for _ in pool.imap_unordered(
                download_file,
                [(url, output_dir, convert_to_parquet) for url in urls]
            ):
                pbar.update()