import logging
import argparse
from clickhouse_driver import Client, errors


'''Parses command line arguments. Connections to ClickHouse'''
def main():
    parser = argparse.ArgumentParser(description='Connecting to the server')

    parser.add_argument('--host', default='localhost', help='Host')
    parser.add_argument('--port', type=int, default=9000, help='Port')
    parser.add_argument('-u', '--user', default='default', help='User')
    parser.add_argument('-p', '--password', default='', help='Password')
    parser.add_argument('--database', default='db_master', help='Name of the database')
    parser.add_argument('--table', default='element', help='Table name')

    args = parser.parse_args()

    try:
        client = Client(host=args.host, port=args.port, user=args.user, password=args.password)
        logging.error('Connection successful')
        check_db(client, args.database, args.table)

    except errors.ServerException as e:
        logging.error(f'Error connecting to ClickHouse: {e}.')


'''Checks if the database and the table in it exist'''
def check_db(client, database_name, table_name):
    try:
        databases = client.execute("SHOW DATABASES")
        if database_name not in [db[0] for db in databases]:
            logging.error(f'Database "{database_name}" does not exist, creating it.')
            create_db(client, database_name, table_name)
            return False

        tables = client.execute(f"SHOW TABLES FROM {database_name}")
        if table_name not in [table[0] for table in tables]:
            logging.error(f'Table "{table_name}" does not exist in database "{database_name}", creating it.')
            create_db(client, database_name, table_name)
            return False

        logging.error(f'Database "{database_name}" and table "{table_name}" exist.')
        return True

    except Exception as e:
        logging.error(f"Check error: {e}.")
        return False

'''Creating a database and a table'''
def create_db(client, database_name, table_name):
    try:
        client.execute(f'''CREATE DATABASE IF NOT EXISTS {database_name}''')
        logging.error('The database has been created successfully')

        client.execute(f'''
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
            (
                doc_id UUID,
                centroid Array(Float64)
            )
            ENGINE = MergeTree()
            ORDER BY doc_id
        ''')
        logging.error('The table was created successfully')

    except Exception as e:
        logging.error(f"Creation error: {e}.")


if __name__ == "__main__":
    main()


