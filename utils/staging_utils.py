from google.cloud import bigquery
import pandas as pd

def get_staging_schema():
    return [
        bigquery.SchemaField("user_id", "INTEGER"),
        bigquery.SchemaField("user_name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("crypto_id", "STRING"),
        bigquery.SchemaField("crypto_symbol", "STRING"),
        bigquery.SchemaField("crypto_name", "STRING"),
        bigquery.SchemaField("current_price", "FLOAT64"),
        bigquery.SchemaField("market_cap", "FLOAT64"),
        bigquery.SchemaField("price_change_percentage_24th", "FLOAT64"),
        bigquery.SchemaField("last_updated", "TIMESTAMP"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
    ]

def populate_staging_table_incremental(client, staging_table_id, raw_users_table, raw_crypto_table):
    # Get last run time
    last_run_query = f"""
        SELECT MAX(GREATEST(created_at, fetched_at)) AS last_run
        FROM `{staging_table_id}`
        """
    last_run_df = client.query(last_run_query).result().to_dataframe()
    last_run = last_run_df["last_run"][0]

    # If table is empty or last_run is NaT, set default
    if last_run is None or pd.isna(last_run):
        last_run = "1970-01-01 00:00:00"

    # Build query using CREATE OR REPLACE TABLE
    query = f"""
    CREATE OR REPLACE TABLE `{staging_table_id}` AS
    SELECT
        u.id AS user_id,
        u.name AS user_name,
        u.email,
        u.city,
        u.created_at,
        c.id AS crypto_id,
        c.Symbol AS crypto_symbol,
        c.name AS crypto_name,
        c.current_price,
        c.market_cap,
        c.price_change_percentage_24th,
        c.last_updated,
        c.fetched_at
    FROM `{raw_users_table}` u
    CROSS JOIN `{raw_crypto_table}` c
    WHERE u.created_at > TIMESTAMP('{last_run}') 
       OR c.fetched_at > TIMESTAMP('{last_run}')
    """
    
    # Run the query
    client.query(query).result()

