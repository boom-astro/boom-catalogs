"""Script to download Legacy Survey DR10.1 catalog as parquet files using lsdb."""
import os
import argparse

import lsdb
from dotenv import load_dotenv

load_dotenv()
OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR', '.')}/ls_dr10/"

parser = argparse.ArgumentParser(description="Download Legacy Survey DR10.1 catalog as parquet files.")
parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="Directory to save downloaded parquet files")
parser.add_argument("--batch-size", type=int, default=1_000_000, help="Number of rows per output parquet file")

COLUMNS = [
    "OBJID",
    "SHAPE_R",
    "SHAPE_R_IVAR",
    "SHAPE_E1",
    "SHAPE_E1_IVAR",
    "SHAPE_E2",
    "SHAPE_E2_IVAR",
    "RA",
    "TYPE",
    "DEC",
    "FLUX_R",
    "NOBS_G",
    "NOBS_R",
    "NOBS_Z",
    "FITBITS",
    "RA_IVAR",
    "DEC_IVAR",
    "MW_TRANSMISSION_R",
    "Z_PHOT_MEAN",
    "Z_PHOT_STD",
    "Z_SPEC",
]

CATALOG_URL = "https://data.lsdb.io/hats/legacysurvey_dr10.1"


def download_catalog(output_dir, batch_size):
    os.makedirs(output_dir, exist_ok=True)

    print(f"Opening catalog from {CATALOG_URL}...")
    catalog = lsdb.open_catalog(CATALOG_URL, columns=COLUMNS)
    print(f"Catalog has {catalog.__len__()} rows")
    print(f"Columns: {catalog.columns.tolist()}")

    print(f"Computing and saving to parquet in {output_dir}...")
    df = catalog.compute()

    # rename columns to lowercase
    df.columns = [c.lower() for c in df.columns]

    total_rows = len(df)
    file_idx = 0
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        chunk = df.iloc[start:end]
        out_path = os.path.join(output_dir, f"lsdr10_{file_idx:04d}.parquet")
        chunk.to_parquet(out_path, index=False)
        print(f"Wrote {len(chunk)} rows to {out_path}")
        file_idx += 1

    print(f"Done. Wrote {total_rows} rows across {file_idx} files.")


if __name__ == "__main__":
    args = parser.parse_args()
    download_catalog(args.output_dir, args.batch_size)
