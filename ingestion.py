import csv
from flathouse_client import read_csv
from clickhouse_client import get_clickhouse_client, insert_into_clickhouse

def load_csv_columns(file_path, delimiter=','):
    """
    Load and return the header columns from a CSV file.
    """
    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter)
            headers = next(reader)
            return headers
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return []

def ingest_flatfile_to_clickhouse(file_path, delimiter, clickhouse_config, table_name, selected_columns):
    """
    Ingest data from a flat file (CSV) to a ClickHouse table.
    """
    try:
        # Initialize ClickHouse client
        client = get_clickhouse_client(
            host=clickhouse_config['host'],
            port=clickhouse_config['port'],
            user=clickhouse_config['user'],
            jwt_token=clickhouse_config['jwt_token'],
            database=clickhouse_config['database']
        )

        print("Ingestion started...")
        print(f"Selected columns: {selected_columns}")
        print(f"Reading file: {file_path}")

        # Read the CSV and extract selected columns
        columns, rows = read_csv(file_path, delimiter, selected_columns)
        print(f"Read {len(rows)} rows.")

        # Insert into ClickHouse
        insert_into_clickhouse(client, table_name, columns, rows)

        print("Ingestion completed successfully.")
        return len(rows)
    except Exception as e:
        print(f"Error during ingestion: {e}")
        return 0
