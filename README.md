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
mongoimport --uri mongodb://127.0.0.1:27017/boom_test \
  --collection Archival_Spectra_Footprint_v1 --mode upsert --stopOnError \
  --gzip --file archival-spectra-footprint.jsonl.gz
mongosh mongodb://127.0.0.1:27017/boom_test --eval \
  'db.Archival_Spectra_Footprint_v1.createIndex({"coordinates.radec_geojson": "2dsphere"})'
```

This collection is intended for direct queries. BOOM's catalog registry and
SkyPortal viewer require their own integration to expose it in a deployment.
For survey-wide ingestion use partitioned release files. The cone importer
fails when a bounded archive query reaches its row limit.

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