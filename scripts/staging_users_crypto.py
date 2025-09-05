import yaml
from utils.bq_utils import get_bq_client, create_table_if_not_exists
from utils.staging_utils import get_staging_schema, populate_staging_table_incremental

def main():
    # Load config
    config = yaml.safe_load(open("config/config.yaml"))
    project = config['gcp']['project_id']
    dataset = config['gcp']['dataset']
    client = get_bq_client(config["gcp"]["credentials"])

    # Table names
    staging_table_id = f"{project}.{dataset}.{config['tables']['staging']}"
    raw_users_table = f"{project}.{dataset}.{config['tables']['raw_users']}"
    raw_crypto_table = f"{project}.{dataset}.{config['tables']['raw_crypto']}"

    # Create staging table
    schema = get_staging_schema()
    create_table_if_not_exists(client, staging_table_id, schema)
    print("Staging table created!")

    # Populate staging table
    populate_staging_table_incremental(client, staging_table_id, raw_users_table, raw_crypto_table)
    print("Staging table populated successfully!")

if __name__ == "__main__":
    main()
