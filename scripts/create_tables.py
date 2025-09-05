import yaml
from utils.bq_utils import get_bq_client, create_table_if_not_exists
from google.cloud import bigquery

def main():
    config = yaml.safe_load(open("config/config.yaml"))

    client = get_bq_client(config["gcp"]["credentials"])
    dataset = config["gcp"]["dataset"]
    project = config["gcp"]["project_id"]

    # raw_users table
    schema_users = [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]
    create_table_if_not_exists(client, f"{project}.{dataset}.{config['tables']['raw_users']}", schema_users)

    # raw_crypto table
    schema_crypto = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("Symbol", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("current_price", "FLOAT64"),
        bigquery.SchemaField("market_cap", "FLOAT64"),
        bigquery.SchemaField("price_change_percentage_24th", "FLOAT64"),
        bigquery.SchemaField("last_updated", "TIMESTAMP"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
    ]
    create_table_if_not_exists(client, f"{project}.{dataset}.{config['tables']['raw_crypto']}", schema_crypto)

    print("Tables created!")

if __name__ == "__main__":
    main()
