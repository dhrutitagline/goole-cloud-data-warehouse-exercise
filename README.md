### Data Pipeline – Postgres + API → BigQuery

This project demonstrates a simple data pipeline that:
1. Loads user data from Postgres into BigQuery (incremental load).
2. Fetches crypto price data from a public API and stores it in BigQuery.
3. Creates a staging table by joining Postgres and API data.

### Tech Stack

Postgres → Source database
Public REST API → Crypto data source
Google BigQuery → Data warehouse
Python → ETL pipeline scripts

### Project Structure
```bash
project/
│── config/
│   ├── config.yaml        # Pipeline configuration
│   ├── bq_key.json        # GCP credentials (NOT in git)
│
│── scripts/
│   ├── create_tables.py   # Creates BigQuery raw tables
│   ├── postgres_to_bq.py  # Incremental load from Postgres → BQ
│   ├── api_to_bq.py       # Load crypto API data → BQ
│   ├── staging_users_crypto.py # Transform + join data
│
│── utils/
│   ├── bq_utils.py        # Helper for BigQuery
│   ├── pg_utils.py        # Helper for Postgres
│   ├── api_utils.py       # Helper for API fetch
│   ├── staging_utils.py   # Helper for transformations
│
│── README.md              # Documentation
├── run_pipeline.py 
```

##  Setup Instructions

### Step 1: Create a Google Cloud Project
1. Go to [Google Cloud Console](https://console.cloud.google.com/).
2. At the top, click **Select Project → New Project**.
3. Give it a name (e.g., `data-pipeline-demo`) and click **Create**.

---

### Step 2: Enable BigQuery
1. In the left menu, search for **BigQuery**.
2. Click **Enable API** (if not already enabled).
3. Open the BigQuery Console → you’ll see an empty workspace.

---

### Step 3: Create a Dataset
1. In the BigQuery console, click on your **project name**.
2. Click **Create Dataset**.
3. Fill in:
   - **Dataset ID**: `raw_data`
   - **Location**: `US` (or your preferred region)
4. Click **Create dataset**.

### Step 4: Create a Service Account (for Python scripts)
1. In Google Cloud Console, go to **IAM & Admin → Service Accounts**.
2. Click **Create Service Account**.
   - **Name**: `bigquery-etl`
   - **Role**: `BigQuery Admin` (or minimally `BigQuery Data Editor`)
3. After creation, click **Keys → Add Key → JSON**.
4. Download the JSON file → place it inside `config/credentials.json`.


## How It Works

### Step 1: Postgres → BigQuery
- Connects to Postgres using psycopg2.
- Selects only new rows since the last pipeline run (created_at > last_loaded).
- Appends rows into raw_users table in BigQuery.

### Step 2: API → BigQuery
- Fetches crypto prices from a public REST API.
- Converts JSON to DataFrame.
- Loads into raw_crypto table in BigQuery with a timestamp (fetched_at).

### Step 3: Transformations
- Creates a staging table (staging_users_crypto).
- Joins users and crypto prices for combined analytics.
- Supports incremental population.

### Step 4: Pipeline Runner
- run_pipeline.py executes all steps:
  1. Creates tables (if not exist)
  2. Loads Postgres → BigQuery
  3. Loads API → BigQuery
  4. Populates staging table

Run it with:
```bash
pip install -r requirements.txt
python3 run_pipeline.py
```
