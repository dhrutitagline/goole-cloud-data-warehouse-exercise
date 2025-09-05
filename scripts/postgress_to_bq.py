import yaml
import pandas as pd
from utils.pg_utils import get_pg_connection
from google.cloud import bigquery

def main():
    config = yaml.safe_load(open("config/config.yaml"))
    project = config['gcp']['project_id']
    dataset = config['gcp']['dataset']
    table_id = f"{project}.{dataset}.{config['tables']['raw_users']}"
    client = bigquery.Client.from_service_account_json(config["gcp"]["credentials"])

    conn = get_pg_connection(config["postgres"])
    cur = conn.cursor()

    query = f"SELECT MAX(created_at) AS last_loaded FROM `{table_id}`"
    last_loaded = client.query(query).result().to_dataframe()["last_loaded"][0]

    if last_loaded is None:
        last_loaded = "1970-01-01"  # First time load

    cur.execute("""
        SELECT id, name, email, city, created_at
        FROM users
        WHERE created_at > %s
    """, (last_loaded,))

    rows = cur.fetchall()

    if not rows:
        print("No new rows found in Postgres.")
    else:
        df = pd.DataFrame(rows, columns=["id", "name", "email", "city", "created_at"])
        
        job = client.load_table_from_dataframe(
            df, 
            table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        )
        job.result()
        print(f"Loaded {len(df)} rows Postgres → BigQuery")

if __name__ == "__main__":
    main()
