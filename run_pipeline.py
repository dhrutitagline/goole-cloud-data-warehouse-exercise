from scripts.api_to_bq import main as load_api_to_bq
from scripts.postgress_to_bq import main as load_postgres_to_bq
from scripts.staging_users_crypto import main as load_staging
from scripts.create_tables import main as create_tables

def run_pipeline():
    print("Starting pipeline...")

    create_tables()

    # Step 1: Postgres → BigQuery
    load_postgres_to_bq()

    # Step 2: API → BigQuery
    load_api_to_bq()

    # Step 3: Transformations (Staging)
    load_staging()

    print("Pipeline finished successfully!")

if __name__ == "__main__":
    run_pipeline()
