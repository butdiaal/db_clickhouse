import logging
import argparse
from clickhouse_driver import Client, errors
from typing import Set

logging.basicConfig(level=logging.INFO)


class Queries:
    """
    A class containing SQL queries as reusable constants.

    This class defines parameterized SQL queries for creating databases and tables,
    as well as checking their existence in ClickHouse.
    """

    CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS {database}"

    CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS {database}.{table}
        (
            {ids} UUID,
            {vectors} Array(Float64)
            INDEX idx {vectors} TYPE vector_similarity('hnsw', 'L2Distance') GRANULARITY 1
        )
        ENGINE = MergeTree()
        ORDER BY {ids}
    """

    SHOW_DATABASES = "SHOW DATABASES"

    SHOW_TABLES = "SHOW TABLES FROM {database}"


class ClickHouseManager:
    """
    A class for managing connections and database operations in ClickHouse.

    This class provides methods for creating databases and tables, checking their existence,
    and ensuring a stable connection to ClickHouse.
    """

    def __init__(
        self, host: str, port: int, user: str, password: str, database: str
    ) -> None:
        """Initializes a connection to ClickHouse
        :param host: The ClickHouse server host.
        :param port: The ClickHouse server port.
        :param user: The username for authentication.
        :param password: The password for authentication.
        :param database: The name of the database to work with.
        """
        try:
            self.client = Client(host=host, port=port, user=user, password=password)
            self.database = database
            logging.info("Connected to ClickHouse successfully.")
        except errors.ServerException as e:
            logging.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def create_db(self, table_name: str, ids: str, vectors: str) -> None:
        """
        Creates a database and table if they do not exist.

        :param table_name: The name of the table to create.
        :param ids: The column name for unique identifiers.
        :param vectors: The column name for storing vector data.
        """
        try:
            self.client.execute(Queries.CREATE_DATABASE.format(database=self.database))
            logging.info(f"Database '{self.database}' created or already exists.")

            self.client.execute(
                Queries.CREATE_TABLE.format(
                    database=self.database, table=table_name, ids=ids, vectors=vectors
                )
            )
            logging.info(
                f"Table '{table_name}' in database '{self.database}' created or already exists."
            )
        except Exception as e:
            logging.error(f"Error creating database or table: {e}")

    def check_db(self, table_name: str, ids: str, vectors: str) -> bool:
        """
        Checks if the database and table exist. If not, they are created.

        :param table_name: The name of the table to check.
        :param ids: The column name for unique identifiers.
        :param vectors: The column name for storing vector data.
        :return: True if the database and table exist (or were successfully created), otherwise False.
        """
        try:

            databases: Set[str] = {
                db[0] for db in self.client.execute(Queries.SHOW_DATABASES)
            }

            if self.database not in databases:
                logging.warning(
                    f"Database '{self.database}' does not exist. Creating it..."
                )
                self.create_db(table_name, ids, vectors)
                return False

            tables: Set[str] = {
                table[0]
                for table in self.client.execute(
                    Queries.SHOW_TABLES.format(database=self.database)
                )
            }

            if table_name not in tables:
                logging.warning(f"Table '{table_name}' does not exist. Creating it...")
                self.create_db(table_name, ids, vectors)
                return False

            logging.info(f"Database '{self.database}' and table '{table_name}' exist.")
            return True

        except Exception as e:
            logging.error(f"Error checking database/table existence: {e}")
            return False


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
        manager = ClickHouseManager(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )
        manager.check_db(args.table, args.ids, args.vectors)
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
