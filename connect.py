import logging
import argparse
from clickhouse_driver import Client, errors
from typing import Set
from utils import Queries, ClickHouseConnection

logging.basicConfig(level=logging.INFO)


class ClickHouseManager:
    """
    A class for managing connections and database operations in ClickHouse.

    This class provides methods for creating databases and tables, checking their existence,
    and ensuring a stable connection to ClickHouse.
    """

    def __init__(self, connection: ClickHouseConnection):
        """Initializes the repository with an existing ClickHouse connection."""
        self.client = connection.get_client()
        self.database = connection.database

    def check_db_exists(self) -> bool:
        """
        Checks if the database exists.

        :return: True if the database exists, False otherwise.
        """
        databases = {db[0] for db in self.client.execute(Queries.SHOW_DATABASES)}
        return self.database in databases


    def check_table_exists(self, table_name: str) -> bool:
        """
        Checks if a specific table exists in the database.

        :param table_name: The name of the table to check.
        :return: True if the table exists, False otherwise.
        """
        tables = {
            table[0]
            for table in self.client.execute(
                Queries.SHOW_TABLES.format(database=self.database)
            )
        }
        return table_name in tables

    def create_database(self) -> None:
        """
        Creates the database if it does not exist.
        """
        self.client.execute(Queries.CREATE_DATABASE.format(database=self.database))
        logging.info(f"Database '{self.database}' created.")


    def create_table(self, table_name: str, ids: str, vectors: str) -> None:
        """
        Creates a table in the database if it does not exist.

        :param table_name: The name of the table to create.
        :param ids: The column name for unique identifiers.
        :param vectors: The column name for storing vector data.
        """
        self.client.execute(
            Queries.CREATE_TABLE.format(
                database=self.database, table=table_name, ids=ids, vectors=vectors
            )
        )
        logging.info(f"Table '{table_name}' in database '{self.database}' created.")


    def ensure_db_and_table(self, table_name: str, ids: str, vectors: str) -> None:
        """
        Ensures the database and table exist, creating them if necessary.

        :param table_name: The name of the table.
        :param ids: The column name for unique identifiers.
        :param vectors: The column name for storing vector data.
        """
        if not self.check_db_exists():
            logging.warning(
                f"Database '{self.database}' does not exist. Creating it..."
            )
            self.create_database()

        if not self.check_table_exists(table_name):
            logging.warning(f"Table '{table_name}' does not exist. Creating it...")
            self.create_table(table_name, ids, vectors)

        logging.info(f"Database '{self.database}' and table '{table_name}' are ready.")


def main() -> None:
    """
    The main function that connects to ClickHouse and verifies the existence of the database and table.

    It reads parameters from the command line, establishes a connection, and checks if the database
    and table exist, creating them if necessary.
    """
    parser = argparse.ArgumentParser(description="ClickHouse Connection")

    parser.add_argument("--host", default="localhost", help="ClickHouse server host")
    parser.add_argument("--port", type=int, default=9000, help="ClickHouse server port")
    parser.add_argument("-u", "--user", default="default", help="ClickHouse username")
    parser.add_argument("-p", "--password", default="", help="ClickHouse password")
    parser.add_argument("--database", default="db_master", help="Database name")
    parser.add_argument("--table", default="element", help="Table name")
    parser.add_argument(
        "--ids", default="doc_id", help="Column name for unique identifiers"
    )
    parser.add_argument(
        "--vectors", default="centroid", help="Column name for vector data"
    )

    args = parser.parse_args()

    try:
        connection = ClickHouseConnection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )

        db = ClickHouseManager(connection)

        db.ensure_db_and_table(args.table, args.ids, args.vectors)
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
