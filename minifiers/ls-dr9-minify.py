"""Minify Legacy Survey DR9 sweep + photo-z sweep FITS pairs into one parquet file each.

Unlike DR10 (where the tractor and photo-z catalogs have different on-disk layouts and have to
be joined on `lsid` with DuckDB, see combine-minify.py), the DR9 photo-z sweeps are
row-by-row-matched to the DR9 sweep catalogs: `sweep-X.fits` and `sweep-X-pz.fits` have
identical row counts and ordering. So the two are merged positionally and no join is needed.

The output columns are exactly the ones `LegacySurvey::from_dataframe` reads on the Rust side,
minus `flux_i`: DR9 predates the i-band, so that column does not exist in the sweeps and is
left absent (the Rust field is an Option and the lookup is optional).

`release`/`brickid`/`objid` are kept rather than being collapsed into an `lsid` here, because
the parquet ingest builds `_id` itself from those three columns.

North/south resolve
-------------------
DR9 reduces the same sky twice: dr9/north is BASS+MzLS, dr9/south is DECam, and the two
footprints overlap. The survey's own rule for counting a source once is to take the northern
reduction only where Dec > 32.375 AND the source is north of the Galactic plane. See
--resolve for why you might not want the Galactic half of that.
"""
import argparse
import os

import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord
from astropy.io import fits
from dotenv import load_dotenv
from multiprocessing import Pool
from tqdm import tqdm

load_dotenv()
INPUT_DIR = f"{os.getenv('INPUT_DIR','.')}/ls_dr9/"
OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/ls_dr9_minified/"

# The Dec at which the Legacy Surveys switch from the DECam (south) reduction to the
# BASS+MzLS (north) one.
RESOLVE_DEC = 32.375

# Columns taken from the sweep catalog. release/brickid/objid are the _id ingredients; the
# rest mirror the DR10 column set that boom's host-galaxy association and crossmatch
# projections read.
SWEEP_COLUMNS = [
	'release', 'brickid', 'objid',
	'ra', 'dec', 'type', 'ebv',
	# flux_i is DR10-only (DR9 has no i-band) and is deliberately absent.
	'flux_g', 'flux_r', 'flux_z',
	'flux_w1', 'flux_w2', 'flux_w3', 'flux_w4',
	# g/z inverse variances give a REX S/N fallback when r is missing.
	'flux_ivar_g', 'flux_ivar_z',
	# Tractor ellipse + the quality columns used to reject marginal REX hosts.
	'shape_r', 'shape_e1', 'shape_e2', 'sersic', 'flux_ivar_r',
	# Blending in all three bands, not just r.
	'fracflux_g', 'fracflux_r', 'fracflux_z',
	# Exposure counts per band, for diagnosing missing-band sources.
	'nobs_g', 'nobs_r', 'nobs_z',
]

# Columns taken from the photo-z sweep. release/brickid/objid are read only to verify the
# row-matching and are then dropped in favour of the sweep's copies.
PHOTOZ_KEY_COLUMNS = ['release', 'brickid', 'objid']
PHOTOZ_COLUMNS = [
	'z_spec', 'survey',
	'z_phot_mean', 'z_phot_median', 'z_phot_std', 'z_phot_l95', 'z_phot_u95',
]

FINAL_COLUMNS = SWEEP_COLUMNS + PHOTOZ_COLUMNS

# The photo-z sweeps use -99 as the "no value" marker for every float field. Mongo should get
# an absent field instead, so these become NaN (which parquet stores as null) and deserialize
# into None. Compare against -98 rather than -99 exactly, since these are float32.
MISSING_SENTINEL = -98.0

parser = argparse.ArgumentParser(
	description="Minify Legacy Survey DR9 sweep + photo-z FITS pairs into parquet."
)
parser.add_argument("--input-dir", type=str, default=INPUT_DIR,
                    help="Directory containing sweep/ and photo-z/ subdirectories of FITS files")
parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR,
                    help="Directory to save minified parquet files")
parser.add_argument("--processes", type=int, default=8, help="Number of parallel worker processes")
parser.add_argument("--resolve", choices=["dr9", "dec-only", "none"], default="dr9",
                    help="Row cut for the north/south overlap. 'dr9' is the survey's own rule "
                         "(Dec > 32.375 and north of the Galactic plane). 'dec-only' drops the "
                         "Galactic half, which keeps the Dec > 32.375, b < 0 sources that no "
                         "other ingested catalog covers. 'none' keeps every row.")


def available_cpus():
	"""CPU count that respects a Slurm allocation, so this behaves on an MSI batch node.

	cpu_count() reports the whole machine, which on a shared node is far more than the job
	was given and leads to heavy oversubscription.
	"""
	slurm = os.getenv("SLURM_CPUS_PER_TASK")
	if slurm:
		return int(slurm)
	return len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count()


def _native(array):
	"""Return `array` in native byte order; FITS data is big-endian and pandas will not take it."""
	if array.dtype.byteorder == '>':
		return array.byteswap().view(array.dtype.newbyteorder())
	return array


def _read_columns(path, columns):
	"""Read `columns` out of the first table HDU of `path` into a DataFrame."""
	with fits.open(path, memmap=True) as hdul:
		data = hdul[1].data
		frame = {}
		for col in columns:
			values = _native(data[col])
			# `type` (3A) and `survey` (10A) arrive as fixed-width bytes padded with spaces.
			if values.dtype.kind in ('S', 'U'):
				values = np.char.strip(values.astype(str))
			frame[col] = values
		return pd.DataFrame(frame)


def _resolve_mask(df, resolve):
	"""Rows of a dr9/north sweep that this catalog should own, under the chosen rule."""
	if resolve == "none":
		return np.ones(len(df), dtype=bool)

	mask = df['dec'].to_numpy() > RESOLVE_DEC
	if resolve == "dec-only":
		return mask

	# Only the survivors of the Dec cut need a coordinate transform, which is the expensive
	# part of this function.
	galactic_b = np.zeros(len(df), dtype=bool)
	if mask.any():
		coords = SkyCoord(
			ra=df.loc[mask, 'ra'].to_numpy(),
			dec=df.loc[mask, 'dec'].to_numpy(),
			unit="deg",
			frame="icrs",
		)
		galactic_b[mask] = coords.galactic.b.deg > 0.0
	return galactic_b


def minify_pair(sweep_path, photoz_path, output_path, resolve="dr9", check_rows=4096):
	"""Merge one sweep/photo-z FITS pair into a single minified parquet file."""
	sweep = _read_columns(sweep_path, SWEEP_COLUMNS)
	photoz = _read_columns(photoz_path, PHOTOZ_KEY_COLUMNS + PHOTOZ_COLUMNS)

	# The positional merge below is only valid if the two files really are row-matched. A
	# truncated or half-written download is the realistic way that stops being true, and it
	# would silently attach each source to some other source's redshift, so check rather
	# than trust. Comparing every row costs more than it is worth; a strided sample catches
	# any misalignment, which is never isolated to a handful of rows.
	if len(sweep) != len(photoz):
		raise ValueError(
			f"row count mismatch: {os.path.basename(sweep_path)} has {len(sweep)} rows, "
			f"{os.path.basename(photoz_path)} has {len(photoz)}"
		)
	step = max(1, len(sweep) // check_rows)
	probe = slice(None, None, step)
	for key in PHOTOZ_KEY_COLUMNS:
		if not np.array_equal(sweep[key].to_numpy()[probe], photoz[key].to_numpy()[probe]):
			raise ValueError(
				f"{os.path.basename(sweep_path)} and {os.path.basename(photoz_path)} "
				f"disagree on '{key}'; the files are not row-matched"
			)

	df = pd.concat([sweep, photoz[PHOTOZ_COLUMNS]], axis=1)
	del sweep, photoz

	df = df[_resolve_mask(df, resolve)].reset_index(drop=True)

	# -99 means "not measured" for every float column the photo-z sweeps contribute. Turn
	# those into nulls so the ingest omits the field entirely, rather than writing a -99 that
	# would read as a real (and wildly wrong) redshift downstream.
	for col in PHOTOZ_COLUMNS:
		if df[col].dtype.kind == 'f':
			df.loc[df[col] <= MISSING_SENTINEL, col] = np.nan
	# `survey` is blank for sources with no photo-z entry.
	df.loc[df['survey'] == '', 'survey'] = None

	df = df[FINAL_COLUMNS]

	os.makedirs(os.path.dirname(output_path), exist_ok=True)
	tmp_path = output_path + ".part"
	df.to_parquet(tmp_path, index=False)
	# Rename only once the file is complete, so an interrupted run leaves no half-written
	# parquet that a resumed run would mistake for finished work.
	os.replace(tmp_path, output_path)
	return len(df)


def _minify_pair_task(arguments):
	sweep_path, photoz_path, output_path, resolve = arguments
	try:
		rows = minify_pair(sweep_path, photoz_path, output_path, resolve=resolve)
		return (sweep_path, rows, None)
	except Exception as e:
		# One corrupt file should not take down a run that is most of the way through a few
		# hundred of them; the caller logs these for re-download.
		return (sweep_path, 0, f"{type(e).__name__}: {e}")


def find_pairs(input_dir, output_dir):
	"""Pair sweep files with their photo-z counterparts, skipping ones already minified."""
	sweep_dir = os.path.join(input_dir, "sweep")
	photoz_dir = os.path.join(input_dir, "photo-z")
	pairs = []
	missing = []
	for name in sorted(os.listdir(sweep_dir)):
		if not name.endswith(".fits"):
			continue
		photoz_name = name.replace(".fits", "-pz.fits")
		photoz_path = os.path.join(photoz_dir, photoz_name)
		if not os.path.exists(photoz_path):
			missing.append(name)
			continue
		output_path = os.path.join(output_dir, name.replace(".fits", ".parquet"))
		if os.path.exists(output_path):
			continue
		pairs.append((os.path.join(sweep_dir, name), photoz_path, output_path))
	return pairs, missing


if __name__ == "__main__":
	args = parser.parse_args()
	os.makedirs(args.output_dir, exist_ok=True)

	pairs, missing = find_pairs(args.input_dir, args.output_dir)
	if missing:
		print(f"WARNING: {len(missing)} sweep files have no photo-z counterpart, skipping them")
	print(f"Found {len(pairs)} pairs left to minify (resolve={args.resolve}).")

	nb_processes = max(1, min(args.processes, available_cpus()))
	total_rows = 0
	failures = []
	tasks = [(s, p, o, args.resolve) for s, p, o in pairs]
	with tqdm(total=len(tasks), desc="Minifying DR9 sweeps") as pbar:
		with Pool(processes=nb_processes) as pool:
			for sweep_path, rows, error in pool.imap_unordered(_minify_pair_task, tasks):
				if error is not None:
					failures.append((sweep_path, error))
					pbar.write(f"SKIPPED {sweep_path}: {error}")
				total_rows += rows
				pbar.update()

	print(f"\nDone: {len(tasks) - len(failures)}/{len(tasks)} pairs minified, "
	      f"{len(failures)} failed, {total_rows:,} rows written.")
	if failures:
		fail_log = os.path.join(args.output_dir, "failed_files.txt")
		with open(fail_log, "w") as f:
			for sweep_path, error in failures:
				f.write(f"{sweep_path}\t{error}\n")
		print(f"Failed files written to {fail_log}")
