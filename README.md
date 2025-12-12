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