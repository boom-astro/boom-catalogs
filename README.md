# boom-catalogs
A repo with scripts to ingest some astronomical catalogs in MongoDB, that boom can cross-match alerts with

## Environment Setup
Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

Install project dependencies:
```bash
pip install -r requirements.txt
```

Set up your environment variables by Copy the `.env.example` to `.env` and modify the variables as needed:
```bash
cp .env.example .env
```

## Usage
To run the download scripts, use the following command:
```bash
python downloaders/<script_name>.py <arguments>
```

To run the ingestion scripts:
```bash
cargo build --release
./target/release/add_<file_type>_catalog
```

## Archival spectroscopy metadata

Use the bulk importer for a complete release ingestion. It reads catalog
tables and DESI exposure metadata and leaves spectral FITS downloads on demand.

```bash
python downloaders/import_archival_spectra.py --all --plan --output archival-spectra
python downloaders/import_archival_spectra.py --all --output archival-spectra
```

The plan reads the DESI release index and lists the bulk inputs. The import
uses LAMOST general and stellar CSVs, SDSS FITS summary tables (including
BOSSNet visits and both APOGEE eras), and the DESI Iron tile-pixel index and
Redrock tables. Each completed partition is checkpointed. Run the same
command again after an interruption to reuse completed partitions and staged
downloads. The completed `manifest.json` lists the gzip JSONL shards and row
counts. Use a new output directory for a new release. Reserve scratch space
for the downloaded metadata and unpacked FITS under `.downloads/`, in
addition to the output shards. Limit concurrent DESI partitions with
`--workers` and limit the run to specific releases with repeatable
`--survey` options. The SDSS scope is BOSS and APOGEE spectroscopy, including
earlier optical releases. Integral-field products such as MaNGA and LVM are
outside this fiber spectroscopy catalog. The SDSS-V BOSS input is the daily
visit summary. Derived epoch and all-epoch coadds are outside the visit index.

Use the footprint importer to build a gzip JSONL observation catalog from
LAMOST DR12 v2.0 (LRS and MRS), SDSS DR20 (legacy optical, BOSS, and APOGEE
visits carried over from DR19), and DESI DR1 (Redrock and Milky Way Survey).
Choose one or more cone centers in ICRS degrees and a radius up to one
degree. For example, these three cones exercise different survey products:

```bash
python downloaders/import_archival_spectra.py \
  --cone 232.2622 0.5424 0.22 \
  --cone 233.456 37.991 0.22 \
  --cone 132.825 11.8 0.22 \
  --output archival-spectra-footprint.jsonl.gz
```

The output has one record per archive product, with deterministic IDs,
BOOM-style sky coordinates, original quality flags, and FITS locators where
supported. The importer reads metadata only. Use the DESI exposure IDs and
coadd links to avoid counting coadds as additional epochs. LAMOST MRS blue
and red arms share an epoch ID. RV values and uncertainties retain the native
pipeline conventions and need quality and zero-point checks before cross-survey
comparisons. Some SDSS reductions have metadata without a FITS locator.

For a test database, load the output using MongoDB Database Tools and create
the spatial index with `mongosh`. The import uses the record ID for repeatable
upserts. The test collection name can be changed independently of the filename.

```bash
gzip -dc archival-spectra-footprint.jsonl.gz | \
  mongoimport --uri mongodb://127.0.0.1:27017/boom_test \
    --collection Archival_Spectra_Footprint_v1 --mode upsert --stopOnError
mongosh mongodb://127.0.0.1:27017/boom_test --eval \
  'db.Archival_Spectra_Footprint_v1.createIndex({"coordinates.radec_geojson": "2dsphere"})'
```

This collection is intended for direct queries. BOOM's catalog registry and
SkyPortal viewer require their own integration to expose it in a deployment.
For a bulk test load, stream the completed manifest's shards into a single
MongoDB import and then create the spatial index as above.

```bash
set -o pipefail
jq -r '.jobs[].shards[].path' archival-spectra/manifest.json | (
  while IFS= read -r shard; do
    gzip -dc "archival-spectra/$shard" || exit
  done
) | mongoimport --uri mongodb://127.0.0.1:27017/boom_test \
    --collection Archival_Spectra_Footprint_v1 --mode upsert --stopOnError
```

The cone importer fails when a bounded archive query reaches its row limit.

### Search for repeat radial velocities

Scan a completed release manifest after backfill:

```bash
python downloaders/search_archival_rv.py \
  --manifest archival-spectra/manifest.json \
  --scratch-dir archival-spectra/scratch \
  --output candidates-v1.jsonl.gz
```

This scans every shard, groups visits by Gaia DR3 ID where available and by
namespaced survey target ID otherwise, and reports the strongest passing pair
per source and pipeline. It filters archive flags and S/N, merges simultaneous
LAMOST arms and same-exposure DESI rows, and uses an MRS coadd only if no usable
exposures from that visit remain. It keeps the FITS locators in the output;
the search never fetches spectral pixels. The default discovery cuts are
30 km/s, five nominal sigma, and S/N 5. The nominal significance includes
conservative per-pipeline error floors and is a ranking statistic, with no
correction for the number of tested pairs. Confirm candidates on the spectra.
By default, comparisons stay within a release and pipeline. Use the optional
calibration workflow below for cross-survey pairs. Use `--footprint` instead of `--manifest` for a
local test, and inspect `--help` for alternate thresholds or survey selection.

#### Calibrate cross-pipeline pairs

Fit a calibration to Gaia DR3-matched stars with at least two independent
visits in each pipeline. Use published RV standards where possible via
`--control-ids` (a whitespace-delimited list of Gaia DR3 source IDs).
The fitter splits on Gaia ID, fits the median B-minus-A offset and robust
scatter, and checks residuals on the held-out stars. By default it requires
30 training and 10 holdout controls. Confirm the frame conventions before
fitting. Selecting `survey_native` explicitly retains the archive-native
conventions for an exploratory comparison.

```bash
python downloaders/search_archival_rv.py --manifest archival-spectra/manifest.json \
  --fit-cross-pipeline lamost:dr12-v2.0:mrs:LAMOST_MRS_BR1 sdss:dr20:boss:BOSSNet \
  --rv-frames survey_native survey_native --output mrs-boss-calibration.json
python downloaders/search_archival_rv.py --manifest archival-spectra/manifest.json \
  --cross-pipeline-calibration mrs-boss-calibration.json --output candidates-v1.jsonl.gz
```

A calibration is specific to its two release and pipeline IDs, frame labels,
and minimum S/N. Raw and corrected signed shifts are retained. The score
includes both per-visit uncertainties, the offset uncertainty, and calibration
scatter. Only Gaia DR3-matched visits from different surveys enter this
comparison. The output ranks `same_pipeline` and `cross_pipeline` separately.
Use the exact visit IDs to check whether a cross-pipeline candidate reuses an
extreme visit from a same-pipeline result.

#### Include 4MOST visits

Append `--fourmost-visits fourmost-visits.csv` to include a public 4MOST
metadata export. The flag also works on its own. This is a normalized exchange
format for reduced visit RVs; it does not read ESO raw frames or assume a
particular upstream 4PA table schema.

Required CSV fields are `release`, `pipeline`, `spectrograph` (`lrs-a`,
`lrs-b`, or `hrs`), `target_id`, `exposure_id`, `ra`, `dec`,
`mjd_utc_mid`, `exptime_s`, `snr`, `rv_barycentric_km_s`,
`rv_error_km_s`, `quality_ok`, `public`, and `spectrum_url`. A
`gaia_dr3_source_id` is optional. Coordinates are ICRS degrees and identifiers
are strings. Export independent spectra with a single RV per target and
exposure. Set `quality_ok` to `1` for an accepted pipeline result and
`public` to `1` for each row. Supply a public HTTPS locator for the
extracted visit spectrum. Use distinct pipeline labels when extraction or
RV zero-point calibrations differ. LRS-A, LRS-B, and HRS are ranked separately
with provisional discovery floors of 3, 3, and 1 km/s respectively.
The scan preserves exposure duration for phase-smearing checks.

## Docker and Apptainer
You can use Docker or Apptainer to create a containerized environment for running the scripts.

### Apptainer
To build and run:
```bash
apptainer build apptainer.sif apptainer.def
apptainer instance start --bind <host_data_path> apptainer.sif apptainer
```
To shell into the running instance:
```bash
apptainer shell instance://apptainer
```
To run the scripts inside the instance:
```bash
add_<file_type>_catalog <arguments>
```

To stop the instance:
```bash
apptainer instance stop apptainer
```