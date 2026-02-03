import os
from astropy.io import fits
import numpy as np
import pandas as pd

SNR_THRESHOLD = 3.0
DIFFMAGLIM_SIGMA = 5.0

sig_str = f"{str(DIFFMAGLIM_SIGMA).replace('.', 'p')}"
columns_to_keep = [
	'release', 'brickid', 'objid', 'maskbits', 'fitbits', 'type', 'ra', 'dec', 'ra_ivar', 'dec_ivar',

	# LS photometry
	'flux_g', 'flux_r', 'flux_i', 'flux_z', 'flux_ivar_g', 'flux_ivar_r', 'flux_ivar_i', 'flux_ivar_z',
	'mw_transmission_g', 'mw_transmission_r', 'mw_transmission_i', 'mw_transmission_z',
	"nobs_g", "nobs_r", "nobs_i", "nobs_z",
	"rchisq_g", "rchisq_r", "rchisq_i", "rchisq_z",
	"psfdepth_g", "psfdepth_r", "psfdepth_i", "psfdepth_z",

	# WISE photometry
	"flux_w1", "flux_w2", "flux_w3", "flux_w4",
	"flux_ivar_w1", "flux_ivar_w2", "flux_ivar_w3", "flux_ivar_w4",
	"mw_transmission_w1", "mw_transmission_w2", "mw_transmission_w3", "mw_transmission_w4",
	"nobs_w1", "nobs_w2", "nobs_w3", "nobs_w4",
	"rchisq_w1", "rchisq_w2", "rchisq_w3", "rchisq_w4",
	"psfdepth_w1", "psfdepth_w2", "psfdepth_w3", "psfdepth_w4",
]

final_columns = [
	# Unique identifier
	'lsid',
	# Basic info
	'maskbits', 'fitbits', 'type', 'ra', 'dec', 'ra_err', 'dec_err', 'ebv',

	# LS photometry
	'mag_g', 'mag_err_g', 'snr_g', f'limmag_g', 'rchisq_g', 'nobs_g', 'magcorr_g',
	'mag_r', 'mag_err_r', 'snr_r', f'limmag_r', 'rchisq_r', 'nobs_r', 'magcorr_r',
	'mag_i', 'mag_err_i', 'snr_i', f'limmag_i', 'rchisq_i', 'nobs_i', 'magcorr_i',
	'mag_z', 'mag_err_z', 'snr_z', f'limmag_z', 'rchisq_z', 'nobs_z', 'magcorr_z',

	# WISE photometry
	'mag_w1', 'mag_err_w1', 'snr_w1', f'limmag_w1', 'rchisq_w1', 'nobs_w1', 'magcorr_w1',
	'mag_w2', 'mag_err_w2', 'snr_w2', f'limmag_w2', 'rchisq_w2', 'nobs_w2', 'magcorr_w2',
	'mag_w3', 'mag_err_w3', 'snr_w3', f'limmag_w3', 'rchisq_w3', 'nobs_w3', 'magcorr_w3',
	'mag_w4', 'mag_err_w4', 'snr_w4', f'limmag_w4', 'rchisq_w4', 'nobs_w4', 'magcorr_w4',
]

def process_tractor_file(file_path, output_dir):
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
	if not np.all(mask):
		print(f"Warning: {np.sum(~mask)} rows with ra_ivar or dec_ivar == 0 in file {file_path}, removing them")
	df = df[mask].reset_index(drop=True)

	# add ra_err and dec_err columns
	df['ra_err'] = 1.0 / np.sqrt(df['ra_ivar'])
	df['dec_err'] = 1.0 / np.sqrt(df['dec_ivar'])

	# Add Legacy Survey mag + mag err + snr + 5-sigma limiting mag columns for g, r, i, z bands
	for band in ['g', 'r', 'i', 'z', 'w1', 'w2', 'w3', 'w4']:
		# DEBUG, check the number of rows where flux_ivar == 0
		num_zero_ivar = np.sum(df[f'flux_ivar_{band}'] == 0)
		if num_zero_ivar > 0:
			print(f"Warning: {num_zero_ivar} rows with flux_ivar_{band} == 0 in file {file_path}")
		# now compute the magnitude conversion, handling non-positive fluxes
		flux_col, flux_ivar_col = f'flux_{band}', f'flux_ivar_{band}'
		mag_col, mag_err_col = f'mag_{band}', f'mag_err_{band}'

		# create a mask for valid flux values
		valid_flux_mask = df[flux_col] > 0
		# convert from flux_ivar to flux_err
		df.loc[valid_flux_mask, 'flux_err_' + band] = 1.0 / np.sqrt(df.loc[valid_flux_mask, flux_ivar_col])
		# compute snr
		df.loc[valid_flux_mask, 'snr_' + band] = df.loc[valid_flux_mask, flux_col] / df.loc[valid_flux_mask, 'flux_err_' + band]
		# compute mag and mag err where snr > 1.0
		snr_mask = df['snr_' + band] > SNR_THRESHOLD
		df.loc[snr_mask, mag_col] = 22.5 - 2.5 * np.log10(df.loc[snr_mask, flux_col])
		df.loc[snr_mask, mag_err_col] = 1.0857 * df.loc[snr_mask, 'flux_err_' + band] / df.loc[snr_mask, flux_col]

		# using the psfdepth (1/nanomaggy^2), compute the 5-sigma limiting mag as well:
		# AB mag = −2.5[log10(5/(√psfdepth_g))−9]
		df[f'limmag_' + band] = - 2.5 * (np.log10(DIFFMAGLIM_SIGMA / np.sqrt(df['psfdepth_' + band])) - 9.0)


		# lets also add a magcorr_{band} column:mag + 2.5log10(mw_transmission), only where mag is defined
		magcorr_col = f'magcorr_{band}'
		df.loc[snr_mask, magcorr_col] = df.loc[snr_mask, mag_col] + 2.5 * np.log10(df.loc[snr_mask, f'mw_transmission_{band}'])

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

	# show the first few rows for debugging, where no values are NaN
	print(df.dropna()[['lsid', 'ra', 'dec', 'mag_g', 'mag_r', 'mag_i', 'mag_z']].head())

	# save to parquet at output_dir with same filename but .parquet extension
	output_path = os.path.join(output_dir, os.path.basename(file_path).replace('.fits', '.parquet'))
	df.to_parquet(output_path, index=False)
	print(f"Processed {file_path}, saved to {output_path}")

if __name__ == "__main__":
	path = "./legacysurvey_tractor/tractor-0001m002.fits"
	output_dir = "./legacysurvey_tractor_processed/"
	os.makedirs(output_dir, exist_ok=True)
	process_tractor_file(path, output_dir)

	# DEBUG, compare the size of original FITS file and processed parquet file
	original_size = os.path.getsize(path)
	processed_size = os.path.getsize(os.path.join(output_dir, os.path.basename(path).replace('.fits', '.parquet')))
	print(f"Original FITS size: {original_size / (1024*1024):.2f} MB")
	print(f"Processed Parquet size: {processed_size / (1024*1024):.2f} MB")
	# as a percentage
	print(f"Size reduction: {(1 - processed_size / original_size) * 100:.2f} %")
