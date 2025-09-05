import psycopg2

def get_pg_connection(config):
    return psycopg2.connect(
        host=config["host"],
        database=config["database"],
        user=config["user"],
        password=config["password"],
        port=config["port"]
    )
