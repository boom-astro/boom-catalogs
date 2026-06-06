import argparse
import os
from astropy.io import fits
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

load_dotenv()
INPUT_DIR = f"{os.getenv('INPUT_DIR','.')}/ls_dr10_tractor/"
OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/ls_dr10_tractor_minified/"

parser = argparse.ArgumentParser(description="Minify all Legacy Survey DR10 tractor FITS files in parallel.")
parser.add_argument("--input-dir", type=str, default=INPUT_DIR, help="Directory with input FITS files")
parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="Directory to save minified files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel download processes")

SNR_THRESHOLD = 3.0
DIFFMAGLIM_SIGMA = 5.0

sig_str = f"{str(DIFFMAGLIM_SIGMA).replace('.', 'p')}"
columns_to_keep = [
	# release/brickid/objid are read only to build `lsid` (the photo-z join key); dropped from output.
	'release', 'brickid', 'objid',
	# ra/dec plus their ivars (ivars used for the astrometry-valid mask and ra_err/dec_err).
	'ra', 'dec', 'ra_ivar', 'dec_ivar',

	# Extra non-photometry columns: not needed for an ra/dec + photo-z join, but kept here
	# (commented out) in case we later want more than photo-z.
	# 'maskbits', 'fitbits', 'type', 'ebv',

	# LS photometry (commented out: not wanted for the photo-z-only minify)
	# 'flux_g', 'flux_r', 'flux_i', 'flux_z', 'flux_ivar_g', 'flux_ivar_r', 'flux_ivar_i', 'flux_ivar_z',
	# 'mw_transmission_g', 'mw_transmission_r', 'mw_transmission_i', 'mw_transmission_z',
	# "nobs_g", "nobs_r", "nobs_i", "nobs_z",
	# "rchisq_g", "rchisq_r", "rchisq_i", "rchisq_z",
	# "psfdepth_g", "psfdepth_r", "psfdepth_i", "psfdepth_z",

	# WISE photometry (commented out: not wanted for the photo-z-only minify)
	# "flux_w1", "flux_w2", "flux_w3", "flux_w4",
	# "flux_ivar_w1", "flux_ivar_w2", "flux_ivar_w3", "flux_ivar_w4",
	# "mw_transmission_w1", "mw_transmission_w2", "mw_transmission_w3", "mw_transmission_w4",
	# "nobs_w1", "nobs_w2", "nobs_w3", "nobs_w4",
	# "rchisq_w1", "rchisq_w2", "rchisq_w3", "rchisq_w4",
	# "psfdepth_w1", "psfdepth_w2", "psfdepth_w3", "psfdepth_w4",
]

final_columns = [
	# Unique identifier (join key with the photo-z catalog)
	'lsid',
	# Basic info
	'ra', 'dec', 'ra_err', 'dec_err',
	# 'maskbits', 'fitbits', 'type', 'ebv',

	# LS photometry (commented out: requires the photometry loop below)
	# 'mag_g', 'mag_err_g', 'snr_g', f'limmag_g', 'rchisq_g', 'nobs_g', 'magcorr_g',
	# 'mag_r', 'mag_err_r', 'snr_r', f'limmag_r', 'rchisq_r', 'nobs_r', 'magcorr_r',
	# 'mag_i', 'mag_err_i', 'snr_i', f'limmag_i', 'rchisq_i', 'nobs_i', 'magcorr_i',
	# 'mag_z', 'mag_err_z', 'snr_z', f'limmag_z', 'rchisq_z', 'nobs_z', 'magcorr_z',

	# WISE photometry (commented out: requires the photometry loop below)
	# 'mag_w1', 'mag_err_w1', 'snr_w1', f'limmag_w1', 'rchisq_w1', 'nobs_w1', 'magcorr_w1',
	# 'mag_w2', 'mag_err_w2', 'snr_w2', f'limmag_w2', 'rchisq_w2', 'nobs_w2', 'magcorr_w2',
	# 'mag_w3', 'mag_err_w3', 'snr_w3', f'limmag_w3', 'rchisq_w3', 'nobs_w3', 'magcorr_w3',
	# 'mag_w4', 'mag_err_w4', 'snr_w4', f'limmag_w4', 'rchisq_w4', 'nobs_w4', 'magcorr_w4',
]

def process_tractor_file(args):
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

	# remove rows where ra_ivar or dec_ivar is zero
	# let's make a mask and apply it
	mask = (df['ra_ivar'] > 0) & (df['dec_ivar'] > 0)
	df = df[mask].reset_index(drop=True)

	# add ra_err and dec_err columns
	df['ra_err'] = 1.0 / np.sqrt(df['ra_ivar'])
	df['dec_err'] = 1.0 / np.sqrt(df['dec_ivar'])

	# Photometry derivation (commented out: not needed for the ra/dec + photo-z minify).
	# Re-enable this loop, plus the LS/WISE entries in columns_to_keep and final_columns,
	# if we later want magnitudes / SNR / limiting-mags in the output.
	# Add Legacy Survey mag + mag err + snr + 5-sigma limiting mag columns for g, r, i, z bands
	# for band in ['g', 'r', 'i', 'z', 'w1', 'w2', 'w3', 'w4']:
	# 	# now compute the magnitude conversion, handling non-positive fluxes
	# 	flux_col, flux_ivar_col = f'flux_{band}', f'flux_ivar_{band}'
	# 	mag_col, mag_err_col = f'mag_{band}', f'mag_err_{band}'

	# 	# create a mask for valid flux and flux_ivar values (>0)
	# 	valid_flux_mask = (df[flux_col] > 0) & (df[flux_ivar_col] > 0)
	# 	# convert from flux_ivar to flux_err
	# 	df.loc[valid_flux_mask, 'flux_err_' + band] = 1.0 / np.sqrt(df.loc[valid_flux_mask, flux_ivar_col])
	# 	# compute snr
	# 	df.loc[valid_flux_mask, 'snr_' + band] = df.loc[valid_flux_mask, flux_col] / df.loc[valid_flux_mask, 'flux_err_' + band]
	# 	# compute mag and mag err where snr > 1.0
	# 	snr_mask = df['snr_' + band] > SNR_THRESHOLD
	# 	df.loc[snr_mask, mag_col] = 22.5 - 2.5 * np.log10(df.loc[snr_mask, flux_col])
	# 	df.loc[snr_mask, mag_err_col] = 1.0857 * df.loc[snr_mask, 'flux_err_' + band] / df.loc[snr_mask, flux_col]

	# 	# using the psfdepth (1/nanomaggy^2), compute the 5-sigma limiting mag as well:
	# 	# AB mag = −2.5[log10(5/(√psfdepth_g))−9]
	# 	df[f'limmag_' + band] = - 2.5 * (np.log10(DIFFMAGLIM_SIGMA / np.sqrt(df['psfdepth_' + band])) - 9.0)
	#
	# 	# lets also add a magcorr_{band} column:mag + 2.5log10(mw_transmission), only where mag is defined
	# 	magcorr_col = f'magcorr_{band}'
	# 	df.loc[snr_mask, magcorr_col] = df.loc[snr_mask, mag_col] + 2.5 * np.log10(df.loc[snr_mask, f'mw_transmission_{band}'])

	# we add an lsid column, as objid + (brickid<<N)+(release<<40)
	# if release >= 10000 (DRO10) N=20 else N=16 (earlier releases, e.g. DR9, DR8, ...)
	df['lsid'] = df['objid'].astype(np.uint64) + (
		np.where(df['release'] >= 10000,
			df['brickid'].astype(np.uint64) << np.uint64(20),
			df['brickid'].astype(np.uint64) << np.uint64(16)
		)
	) + (df['release'].astype(np.uint64) << np.uint64(40))

	# let's create our finalized dataframe with selected columns
	df = df[final_columns]

	# make sure the output directory exists
	os.makedirs(os.path.dirname(output_path), exist_ok=True)

	# save to parquet
	df.to_parquet(output_path, index=False)

if __name__ == "__main__":
	args = parser.parse_args()
	input_dir = args.input_dir
	output_dir = args.output_dir

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
			for _ in pool.imap_unordered(process_tractor_file, inputs):
				pbar.update()


