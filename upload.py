import logging
import argparse
import os
import json
from typing import List, Tuple
from clickhouse_driver import errors
from utils import Queries, ClickHouseConnection

logging.basicConfig(level=logging.INFO)


class FileLoader:
    """
    A utility class for loading vector data from a JSON file.
    """

    @staticmethod
    def load(file_input: str) -> List[Tuple[str, List[float]]]:
        """
        Loads vector data from a JSON file.

        :param file_input: Path to the input JSON file.
        :return: A list of tuples containing document IDs and vectors.
        """
        data_to_load: List[Tuple[str, List[float]]] = []

        if not os.path.exists(file_input):
            logging.error(f"File '{file_input}' does not exist.")
            return data_to_load

        if os.path.getsize(file_input) == 0:
            logging.error(f"File '{file_input}' is empty.")
            return data_to_load

        try:
            with open(file_input, "r") as file:
                elements = json.load(file)

            for element in elements:
                doc_id = element["id"]
                centroid = element["vector"]
                data_to_load.append((doc_id, centroid))

            logging.info(f"Loaded {len(data_to_load)} records from '{file_input}'.")
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Error reading JSON file '{file_input}': {e}")

        return data_to_load


class DatabaseUploader:
    """
    Handles database connection and data insertion into ClickHouse.
    """

    def __init__(self, connection: ClickHouseConnection):
        """Initializes the repository with an existing ClickHouse connection."""
        self.client = connection.get_client()
        self.database = connection.database

    def insert_data(
        self,
        database: str,
        table: str,
        ids: str,
        vectors: str,
        data: List[Tuple[str, List[float]]],
    ) -> None:
        """
        Inserts data into the specified ClickHouse table.

        :param database: Database name.
        :param table: Table name.
        :param data: List of tuples containing document IDs and vector data.
        """
        if not data:
            logging.error("No data to insert.")
            return

        try:
            query = Queries.INSERT_DATA.format(
                database=database, table=table, ids=ids, vectors=vectors
            )
            self.client.execute(query, data)
            logging.info(
                f"Successfully inserted {len(data)} records into '{database}.{table}'."
            )
        except errors.ServerException as e:
            logging.error(f"Error inserting data into ClickHouse: {e}")


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    :return: Parsed arguments as a namespace object.
    """
    parser = argparse.ArgumentParser(description="Upload data to ClickHouse")

    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=9000, help="ClickHouse port")
    parser.add_argument("-u", "--user", default="default", help="ClickHouse user")
    parser.add_argument("-p", "--password", default="", help="ClickHouse password")
    parser.add_argument("--database", default="db_master", help="Database name")
    parser.add_argument("--table", default="element", help="Table name")
    parser.add_argument("--ids", default="doc_id", help="ID column name")
    parser.add_argument("--vectors", default="centroid", help="Vector column name")
    parser.add_argument(
        "--file_input", type=str, default="elements.json", help="Path to the JSON file"
    )

    return parser.parse_args()


def main() -> None:
    """
    Main function that handles database connection, file processing, and data insertion.
    """
    args = parse_arguments()

    try:
        connection = ClickHouseConnection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )

        db = DatabaseUploader(connection)

        data = FileLoader.load(args.file_input)
        if data:
            db.insert_data(args.database, args.table, args.ids, args.vectors, data)

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
