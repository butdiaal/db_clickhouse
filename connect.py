import logging
import argparse
from clickhouse_driver import Client, errors


def create_db(
    client: Client, database_name: str, table_name: str, ids: str, vectors: str
) -> None:
    """
    Creates a database and table in ClickHouse if they do not exist.

    :param client: ClickHouse client instance.
    :param database_name: Name of the database.
    :param table_name: Name of the table.
    :param ids: Attribute name for identifiers.
    :param vectors: Attribute name for vector data.
    """

    try:
        client.execute(f"""CREATE DATABASE IF NOT EXISTS {database_name}""")
        logging.warning("The database has been created successfully")

        client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
            (
                {ids} UUID,
                {vectors} Array(Float64)
                INDEX idx {vectors} TYPE vector_similarity('hnsw', 'L2Distance') GRANULARITY 1
            )
            ENGINE = MergeTree()
            ORDER BY {ids}
        """
        )
        logging.warning("The table was created successfully")

    except Exception as e:
        logging.error(f"Creation error: {e}.")


def check_db(
    client: Client, database_name: str, table_name: str, ids: str, vectors: str
) -> bool:
    """
    Checks if the database and table exist. Creates them if necessary.

    :param client: ClickHouse client instance.
    :param database_name: Name of the database.
    :param table_name: Name of the table.
    :param ids: Attribute name for identifiers.
    :param vectors: Attribute name for vector data.
    :return: True if the database and table exist (or were successfully created), otherwise False.
    """

    databases = {db[0] for db in client.execute("SHOW DATABASES")}
    tables = {
        table[0] for table in client.execute(f"SHOW TABLES FROM {database_name}")
    }

    if database_name not in databases:
        logging.error(f'Database "{database_name}" does not exist, creating it.')
        create_db(client, database_name, table_name, ids, vectors)
        return False

    if table_name not in tables:
        create_db(client, database_name, table_name, ids, vectors)
        return False

    logging.warning(f'Database "{database_name}" and table "{table_name}" exist.')
    return True



def main() -> None:
    """
    Main function that establishes a connection to ClickHouse,checks the database and table, and waits for the model file to appear.
    """

    parser = argparse.ArgumentParser(description="Connecting to the server")

    parser.add_argument("--host", default="localhost", help="Host")
    parser.add_argument("--port", type=int, default=9000, help="Port")
    parser.add_argument("-u", "--user", default="default", help="User")
    parser.add_argument("-p", "--password", default="", help="Password")
    parser.add_argument("--database", default="db_master", help="Name of the database")
    parser.add_argument("--table", default="element", help="Table name")
    parser.add_argument("--ids", default="doc_id", help="Id database attribute")
    parser.add_argument(
        "--vectors", default="centroid", help="The vector database attribute"
    )

    args = parser.parse_args()

    try:
        client = Client(
            host=args.host, port=args.port, user=args.user, password=args.password
        )
        logging.warning("Connection successful")
        check_db(client, args.database, args.table, args.ids, args.vectors)

    except errors.ServerException as e:
        logging.error(f"Error connecting to ClickHouse: {e}.")


if __name__ == "__main__":
    main()
