"""Combine the minified Legacy Survey DR10 tractor and photo-z catalogs into one.

Both minified catalogs share a globally-unique `lsid` (= objid + (brickid<<N) + (release<<40)),
so they are joined purely on `lsid`:
    tractor : lsid, ra, dec, ra_err, dec_err          (per-brick parquet, ~101 GB)
    photo-z : lsid, z_phot, z_phot_err, photo_z_type  (per-sweep parquet, ~34 GB)

The two have different on-disk layouts (per-brick vs per-sweep) and `lsid` is unique per
source, so we do a single out-of-core hash join with DuckDB (which spills to disk) rather than
trying to pair files by sky position. A LEFT join keeps every tractor source (positions are the
point of the catalog) and attaches photo-z where it exists.

Output is written as a partitioned parquet dataset (hive-style `ra_deg=<NN>/...`), which keeps
individual files a manageable size and lets downstream readers glob `**/*.parquet`.
"""
import os
import argparse

import duckdb
from dotenv import load_dotenv

load_dotenv()
OUTPUT_BASE = os.getenv("OUTPUT_DIR", ".")
DEFAULT_TRACTOR = f"{OUTPUT_BASE}/ls_dr10_tractor_minified"
DEFAULT_PHOTOZ = f"{OUTPUT_BASE}/ls_dr10_photoz_minified"
DEFAULT_OUTPUT = f"{OUTPUT_BASE}/ls_dr10_combined_minified"

parser = argparse.ArgumentParser(
    description="Combine minified LS DR10 tractor + photo-z catalogs on lsid (DuckDB hash join)."
)
parser.add_argument("--tractor-dir", default=DEFAULT_TRACTOR, help="Dir of minified tractor parquet files")
parser.add_argument("--photoz-dir", default=DEFAULT_PHOTOZ, help="Dir of minified photo-z parquet files")
parser.add_argument("--output-dir", default=DEFAULT_OUTPUT, help="Dir to write the combined parquet dataset")
parser.add_argument("--join", choices=["left", "inner"], default="left",
                    help="left: keep all tractor sources (photo-z null if missing); inner: only matched")
parser.add_argument("--threads", type=int, default=8, help="DuckDB worker threads")
parser.add_argument("--memory-limit", default="16GB", help="DuckDB in-memory budget before spilling to disk")
parser.add_argument("--temp-dir", default=None, help="Scratch dir for DuckDB spill (default: <output-dir>/.duckdb_tmp)")


def main():
    args = parser.parse_args()

    tractor_glob = os.path.join(args.tractor_dir, "**", "*.parquet")
    photoz_glob = os.path.join(args.photoz_dir, "*.parquet")
    temp_dir = args.temp_dir or os.path.join(args.output_dir, ".duckdb_tmp")

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)

    join_type = "LEFT JOIN" if args.join == "left" else "INNER JOIN"

    con = duckdb.connect()
    con.execute(f"SET threads={args.threads};")
    con.execute(f"SET memory_limit='{args.memory_limit}';")
    con.execute(f"SET temp_directory='{temp_dir}';")
    con.execute("SET preserve_insertion_order=false;")  # lower memory for the big streaming COPY

    # ra_deg is a 0..359 partition key derived from the tractor RA; it keeps output files small
    # and roughly mirrors the tractor catalog's RA-binned subdirectories.
    query = f"""
    COPY (
        SELECT
            t.lsid,
            t.ra,
            t.dec,
            t.ra_err,
            t.dec_err,
            p.z_phot,
            p.z_phot_err,
            p.photo_z_type,
            (CAST(floor(t.ra) AS INTEGER) % 360) AS ra_deg
        FROM read_parquet('{tractor_glob}') AS t
        {join_type} read_parquet('{photoz_glob}') AS p USING (lsid)
    )
    TO '{args.output_dir}'
    (FORMAT PARQUET, PARTITION_BY (ra_deg), OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'part_{{uuid}}');
    """
    print(f"Joining ({args.join}) tractor <- photo-z on lsid")
    print(f"  tractor: {tractor_glob}")
    print(f"  photoz : {photoz_glob}")
    print(f"  output : {args.output_dir}  (partitioned by ra_deg)")
    print(f"  threads={args.threads} memory_limit={args.memory_limit} temp_dir={temp_dir}")
    con.execute(query)

    # Report row counts so the result is verifiable.
    n_out = con.execute(
        f"SELECT count(*) FROM read_parquet('{os.path.join(args.output_dir, '**', '*.parquet')}')"
    ).fetchone()[0]
    n_matched = con.execute(
        f"SELECT count(*) FROM read_parquet('{os.path.join(args.output_dir, '**', '*.parquet')}') "
        f"WHERE z_phot IS NOT NULL"
    ).fetchone()[0]
    print(f"\nDone. combined rows: {n_out:,}; with photo-z: {n_matched:,}; without: {n_out - n_matched:,}")


if __name__ == "__main__":
    main()
