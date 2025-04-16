from clickhouse_driver import Client

def get_clickhouse_client(host, port, user, jwt_token, database):
    client = Client(
        host=host,
        port=port,
        user=user,
        password=jwt_token,  # Assuming JWT token is used as the password
        database=database,
        secure=True,
        verify=False
    )
    return client

def insert_into_clickhouse(client, table_name, columns, rows):
    if not columns or not rows:
        raise ValueError("Columns and rows must not be empty")

    col_str = ','.join(columns)
    insert_query = f"INSERT INTO {table_name} ({col_str}) VALUES"
    client.execute(insert_query, rows)
