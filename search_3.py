import json
import logging
import argparse
import numpy as np
from clickhouse_driver import Client, errors
from typing import List, Tuple, Dict, Optional


logging.basicConfig(level=logging.INFO)


class Queries:
    """
    A class that stores SQL queries as constants.
    """
    SEARCH_SIMILAR = """
        WITH {vector} AS reference_vector
        SELECT {id_column}, L2Distance({vector_column}, reference_vector) AS distance
        FROM {database}.{table}
        ORDER BY distance
        LIMIT {count}
    """


class ClickHouseRepository:
    """
    A repository class for executing vector similarity searches in ClickHouse.
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        """Initializes a connection to ClickHouse
        :param host: The ClickHouse server host.
        :param port: The ClickHouse server port.
        :param user: The username for authentication.
        :param password: The password for authentication.
        :param database: The name of the database to work with.
        """
        self.client = Client(
            host=host,
            port=port,
            user=user,
            password=password
        )
        self.database = database
        logging.info("Successfully connected to ClickHouse.")

    def search_similar_vectors(
        self, input_vectors: List[List[float]], table: str, id_column: str, vector_column: str, count: int
    ) -> List[Tuple[int, List[Tuple[str, float]]]]:
        """
        Finds the most similar vectors using the L2 (Euclidean) distance function in ClickHouse.

        :param input_vectors: A list of input vectors.
        :param table: The name of the table.
        :param id_column: The column name for document IDs.
        :param vector_column: The column name for vector data.
        :param count: The number of most similar vectors to retrieve.
        :return: A list of tuples, each containing:
            - The index of the input vector.
            - A list of tuples with document IDs and distances.
        """
        try:
            all_results = []

            for index, input_vector in enumerate(input_vectors, start=1):
                vector_str = "[" + ",".join(map(str, input_vector)) + "]"

                query = Queries.SEARCH_SIMILAR.format(
                    vector=vector_str,
                    database=self.database,
                    table=table,
                    id_column=id_column,
                    vector_column=vector_column,
                    count=count
                )

                result = self.client.execute(query)
                all_results.append((index, result))

            logging.info("Query executed successfully")
            return all_results

        except Exception as e:
            logging.error(f"Error executing query: {e}")
            return []


class VectorUtils:
    """
    A utility class for handling vectors.
    """

    @staticmethod
    def print_similar_vectors(similar_vectors: List[Tuple[int, List[Tuple[str, float]]]]) -> None:
        """
        Logs the results of similar vector searches.

        :param similar_vectors: A list of tuples where each tuple contains:
            - The index of the input vector.
            - A list of tuples (document ID, distance).
        """
        for index, result in similar_vectors:
            logging.warning(f"Results for the {index}th vector:")
            for row in result:
                logging.warning(f"ID: {row[0]}, Distance: {row[1]}")

    @staticmethod
    def vectors_from_json(file_path: str) -> List[List[float]]:
        """
        Loads input vectors from a JSON file.

        :param file_path: Path to the JSON file.
        :return: A list of vectors.
        """
        with open(file_path, "r") as f:
            data = json.load(f)

        return [item["vector"] for item in data]


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    :return: Parsed arguments as a namespace object.
    """
    parser = argparse.ArgumentParser(description="Vector Similarity Search with ClickHouse")

    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=9000, help="ClickHouse port")
    parser.add_argument("-u", "--user", default="default", help="ClickHouse username")
    parser.add_argument("-p", "--password", default="", help="ClickHouse password")
    parser.add_argument("--database", default="db_master", help="ClickHouse database name")
    parser.add_argument("--table", default="element", help="ClickHouse table name")
    parser.add_argument("--ids", default="doc_id", help="Column name for document IDs")
    parser.add_argument("--vectors", default="centroid", help="Column name for vector data")
    parser.add_argument("--count", type=int, default=10, help="Number of similar vectors to retrieve")
    parser.add_argument("--file", type=str, default="test.json", help="Path to input JSON file with vectors")

    return parser.parse_args()


def main() -> None:
    """
    Main function that handles database connection, retrieves data,
    and performs similarity search.
    """
    args = parse_arguments()

    input_vectors = VectorUtils.vectors_from_json(args.file)

    try:

        db = ClickHouseRepository(
            host=args.host, port=args.port, user=args.user, password=args.password, database=args.database
        )

        similar_vectors = db.search_similar_vectors(input_vectors, args.table, args.ids, args.vectors, args.count)

        VectorUtils.print_similar_vectors(similar_vectors)

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()