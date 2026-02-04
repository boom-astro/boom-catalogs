import argparse
import os
from astropy.io import fits
import argparse
import numpy as np
import pandas as pd
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
INPUT_DIR = f"{os.getenv('INPUT_DIR','.')}/ls_dr10_photoz/"
OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/ls_dr10_photoz_minified/"

parser = argparse.ArgumentParser(description="Minify all Legacy Survey DR10 sweep photo-z FITS files in parallel.")
parser.add_argument("--input-dir", type=str, default=INPUT_DIR, help="Directory with input FITS files")
parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="Directory to save minified files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")

columns_to_keep = [
	'release', 'brickid', 'objid',
	'z_phot_mean', 'z_phot_std',
	'z_phot_mean_i', 'z_phot_std_i',
	'z_spec',
]

final_columns = ['lsid', 'z_phot', 'z_phot_err', 'photo_z_type']

def process_sweep_photoz_file(args):
	file_path, output_path = args
	with fits.open(file_path) as F:
		data = F[1].data
		small_data = {}

		for col in columns_to_keep:
			small_data[col] = data[col]
			# the data might be in big endian format, convert to native
			if small_data[col].dtype.byteorder == '>':
				small_data[col] = small_data[col].byteswap().view(small_data[col].dtype.newbyteorder())

		df: pd.DataFrame = pd.DataFrame(small_data)
		del small_data  # free memory

		# we add an lsid column, as objid + (brickid<<N)+(release<<40)
		# if release >= 10000 (DRO10) N=20 else N=16 (earlier releases, e.g. DR9, DR8, ...)
		df['lsid'] = df['objid'].astype(np.uint64) + (
			np.where(df['release'] >= 10000,
				df['brickid'].astype(np.uint64) << np.uint64(20),
				df['brickid'].astype(np.uint64) << np.uint64(16)
			)
		) + (df['release'].astype(np.uint64) << np.uint64(40))

		# let's add a new column: z_phot, which is z_phot_mean_i if available (missing values are -99) else z_phot_mean
		# we also add the associated error column: z_phot_err, which is z_phot_std_i if we used z_phot_mean_i else z_phot_std
		# first let's make a mask of valid z_phot_mean_i values
		valid_z_phot_i_mask = df['z_phot_mean_i'] > -99.0
		df['z_phot'] = np.where(valid_z_phot_i_mask, df['z_phot_mean_i'], df['z_phot_mean'])
		df['z_phot_err'] = np.where(valid_z_phot_i_mask, df['z_phot_std_i'], df['z_phot_std'])
		# add a photo_z_type column: 'grzi' if z_phot_mean_i is used, else 'grz'
		df['photo_z_type'] = np.where(valid_z_phot_i_mask, 'grzi', 'grz')

		# where z_spec is available (not -99), we replace z_phot with z_spec, and z_phot_err with 0.0
		valid_z_spec_mask = df['z_spec'] > -99.0
		df.loc[valid_z_spec_mask, 'z_phot'] = df.loc[valid_z_spec_mask, 'z_spec']
		df.loc[valid_z_spec_mask, 'z_phot_err'] = 0.0
		# also replace photo_z_type with 'spec' in that case
		df.loc[valid_z_spec_mask, 'photo_z_type'] = 'spec'

		# now we just keep: lsid, z_phot, z_phot_err, photo_z_type
		df = df[final_columns]

		# make sure the output directory exists
		os.makedirs(os.path.dirname(output_path), exist_ok=True)

		# save to parquet
		df.to_parquet(output_path, index=False)

if __name__ == "__main__":
	args = parser.parse_args()
	input_dir = args.input_dir
	output_dir = args.output_dir
	os.makedirs(output_dir, exist_ok=True)

	# prepare the list of files to process so we can parallelize
	inputs = []
	for root, dirs, files in os.walk(input_dir):
		for file in files:
			if file.endswith(".fits"):
				input_path = os.path.join(root, file)
				# create the corresponding output path
				relative_path = os.path.relpath(input_path, input_dir)
				output_path = os.path.join(output_dir, relative_path.replace('.fits', '.parquet'))
				# add to inputs
				inputs.append((input_path, output_path))

	# let's parallelize the processing
	nb_processes = min(args.processes, cpu_count() - 2)
	with tqdm(total=len(inputs), desc="Processing tractor files") as pbar:
		with Pool(processes=nb_processes) as pool:
			for _ in pool.imap_unordered(process_sweep_photoz_file, inputs):
				pbar.update()
