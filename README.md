## How It Works

### Step 1: Postgres → BigQuery
- Connects to Postgres using `psycopg2`.
- Selects only **new rows** since the last pipeline run (`created_at > last_loaded`).
- Appends rows into **`raw_users`** table in BigQuery.

### Step 2: API → BigQuery
- Fetches crypto prices from a public REST API.
- Converts JSON to DataFrame.
- Loads into **`raw_crypto`** table in BigQuery with a timestamp (`fetched_at`).

### Step 3: Transformations
- Creates a **staging table** (`staging_users_crypto`).
- Joins **users** and **crypto prices** for combined analytics.
- Supports **incremental population**.

### Step 4: Pipeline Runner
- `run_pipeline.py` executes all steps:
  1. Creates tables (if not exist)
  2. Loads Postgres → BigQuery
  3. Loads API → BigQuery
  4. Populates staging table

Run it with:
```bash
python3 run_pipeline.py
```
