import yaml
from utils.api_utils import fetch_crypto_data
from utils.bq_utils import get_bq_client
import pandas as pd

def main():
    config = yaml.safe_load(open("config/config.yaml"))
    client = get_bq_client(config["gcp"]["credentials"])
    project = config["gcp"]["project_id"]
    dataset = config["gcp"]["dataset"]

    rows = fetch_crypto_data()

    df = pd.DataFrame(rows, columns=[
        "id","Symbol","name","current_price","market_cap",
        "price_change_percentage_24th","last_updated","fetched_at"
    ])


    df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True)
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)

    table_id = f"{project}.{dataset}.{config['tables']['raw_crypto']}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait until complete

    print(f"Loaded {len(df)} rows API → BigQuery")


if __name__ == "__main__":
    main()
