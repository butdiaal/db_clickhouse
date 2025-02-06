import logging
import argparse
from clickhouse_driver import Client, errors


"""Processes command-line arguments and establishes a connection to the ClickHouse, runs a check for the database"""


def main():
    parser = argparse.ArgumentParser(description="Connecting to the server")

    parser.add_argument("--host", default="localhost", help="Host")
    parser.add_argument("--port", type=int, default=9000, help="Port")
    parser.add_argument("-u", "--user", default="default", help="User")
    parser.add_argument("-p", "--password", default="", help="Password")
    parser.add_argument("--database", default="db_master", help="Name of the database")
    parser.add_argument("--table", default="element", help="Table name")
    parser.add_argument("--id", default="doc_id", help="Id database attribute")
    parser.add_argument(
        "--vector", default="centroid", help="The vector database attribute"
    )

    args = parser.parse_args()

    try:
        client = Client(
            host=args.host, port=args.port, user=args.user, password=args.password
        )
        logging.debug("Connection successful")
        check_db(client, args.database, args.table, args.id, args.vector)

    except errors.ServerException as e:
        logging.debug(f"Error connecting to ClickHouse: {e}.")


"""Checks if the database and the table in it exist"""


def check_db(client, database_name, table_name, ids, vector):
    try:
        databases = client.execute("SHOW DATABASES")
        if database_name not in [db[0] for db in databases]:
            logging.debug(f'Database "{database_name}" does not exist, creating it.')
            create_db(client, database_name, table_name, ids, vector)
            return False

        tables = client.execute(f"SHOW TABLES FROM {database_name}")
        if table_name not in [table[0] for table in tables]:
            logging.debug(
                f'Table "{table_name}" does not exist in database "{database_name}", creating it.'
            )
            create_db(client, database_name, table_name, ids, vector)
            return False

        logging.debug(f'Database "{database_name}" and table "{table_name}" exist.')
        return True

    except Exception as e:
        logging.debug(f"Check error: {e}.")
        return False


"""Creating a database and a table"""


def create_db(client, database_name, table_name, ids, vector):
    try:
        client.execute(f"""CREATE DATABASE IF NOT EXISTS {database_name}""")
        logging.debug("The database has been created successfully")

        client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
            (
                {ids} UUID,
                {vector} Array(Float64)
            )
            ENGINE = MergeTree()
            ORDER BY {id}
        """
        )
        logging.debug("The table was created successfully")

    except Exception as e:
        logging.debug(f"Creation error: {e}.")


if __name__ == "__main__":
    main()
