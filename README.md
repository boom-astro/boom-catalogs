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