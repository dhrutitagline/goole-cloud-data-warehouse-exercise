import requests
from datetime import datetime as dt
from datetime import UTC
import yaml
import pandas as pd
from google.cloud import bigquery
from utils.bq_utils import get_bq_client


def fetch_crypto_data():
    """Fetch cryptocurrency data from CoinGecko API."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    fetched_at = dt.now(UTC).isoformat()
    rows = []
    for item in data:
        rows.append({
            "id": item["id"],
            "symbol": item["symbol"],
            "name": item["name"],
            "current_price": item.get("current_price"),
            "market_cap": item.get("market_cap"),
            "price_change_percentage_24h": item.get("price_change_percentage_24h"),
            "last_updated": item.get("last_updated"),
            "fetched_at": fetched_at
        })
    return rows


def main():
    config = yaml.safe_load(open("config/config.yaml"))
    client = get_bq_client(config["gcp"]["credentials"])
    project = config["gcp"]["project_id"]
    dataset = config["gcp"]["dataset"]
    target_table = f"{project}.{dataset}.{config['tables']['raw_crypto']}"

    # Fetch API data
    rows = fetch_crypto_data()
    df = pd.DataFrame(rows)

    # Convert timestamps
    df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True)
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)

    # Step 1: Append new batch into final table
    job = client.load_table_from_dataframe(
        df,
        target_table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"Appended {len(df)} rows into {target_table}")

    # Step 2: Deduplicate by recreating the table (free tier-safe)
    dedup_query = f"""
        CREATE OR REPLACE TABLE `{target_table}` AS
        SELECT
        t.id,
        t.symbol,
        t.name,
        t.current_price,
        t.market_cap,
        t.price_change_percentage_24h,
        t.last_updated,
        t.fetched_at
        FROM (
        SELECT ARRAY_AGG(t ORDER BY fetched_at DESC LIMIT 1)[OFFSET(0)] AS t
        FROM `{target_table}` t
        GROUP BY id, last_updated
        )
        """


    client.query(dedup_query).result()
    print(f"Deduplicated {target_table} (kept only latest per id+last_updated)")


if __name__ == "__main__":
    main()
