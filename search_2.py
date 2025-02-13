import json
import faiss
import logging
import argparse
import numpy as np
from clickhouse_driver import Client, errors
from typing import List, Dict, Tuple, Optional


logging.basicConfig(level=logging.INFO)


class Queries:
    """
    A class that stores SQL queries as constants.
    """

    GET_VECTORS = "SELECT {ids}, {vectors} FROM {database}.{table}"


class ClickHouseRepository:
    """
    A repository class for interacting with a ClickHouse database.
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        """Initializes a connection to ClickHouse
        :param host: The ClickHouse server host.
        :param port: The ClickHouse server port.
        :param user: The username for authentication.
        :param password: The password for authentication.
        :param database: The name of the database to work with.
        """
        self.client = Client(host=host, port=port, user=user, password=password)
        self.database = database
        logging.info("Successfully connected to ClickHouse.")

    def get_vectors(
        self, table: str, ids: str, vectors: str
    ) -> Optional[Dict[str, np.ndarray]]:
        """
        Retrieves vector data from the ClickHouse database.

        :param table: The name of the table containing vector data.
        :param ids: The column name containing document IDs.
        :param vectors: The column name containing vector data.
        :return: A dictionary mapping document IDs to NumPy vector arrays.
        """
        query = Queries.GET_VECTORS.format(
            database=self.database, table=table, ids=ids, vectors=vectors
        )

        try:
            result = self.client.execute(query)
            logging.info("Vector data retrieved successfully.")

            return {str(row[0]): np.array(row[1], dtype="float64") for row in result}

        except Exception as e:
            logging.error(f"Error retrieving vector data: {e}")
            return None


class VectorSearcher:
    """
    A class for performing similarity searches using FAISS.
    """

    def __init__(self, vectors_index: Dict[str, np.ndarray]):
        """
        Initializes the FAISS index.

        :param vectors_index: A dictionary where keys are document IDs and values are vectors.
        """
        if not vectors_index:
            raise ValueError("Vector index is empty.")

        self.doc_ids = np.array(list(vectors_index.keys()))
        self.db_vectors = np.array(list(vectors_index.values()), dtype="float64")

        self.index = faiss.IndexFlatL2(self.db_vectors.shape[1])
        self.index.add(self.db_vectors)

    def search_similar(
        self, input_vectors: List[List[float]], count: int
    ) -> Dict[int, List[Tuple[str, float]]]:
        """
        Searches for the most similar vectors using FAISS.

        :param input_vectors: A list of input vectors for which to find similar vectors.
        :param count: The number of similar vectors to return.
        :return: A dictionary mapping input vector indices to lists of tuples (document ID, distance).
        """
        similar_vectors: Dict[int, List[Tuple[str, float]]] = {}

        for idx, input_vector in enumerate(input_vectors):
            input_vector_np = np.array(input_vector, dtype="float64").reshape(1, -1)

            distances, indices = self.index.search(input_vector_np, count)

            distances = np.sqrt(distances)

            similar_vectors[idx] = [
                (self.doc_ids[indices[0][i]], distances[0][i]) for i in range(count)
            ]

        return similar_vectors

    @staticmethod
    def print_similar_vectors(
        similar_vectors: Dict[int, List[Tuple[str, float]]],
    ) -> None:
        """
        Logs the results of similar vector searches.

        :param similar_vectors: A dictionary where keys are input vector indices
                                and values are lists of tuples (document ID, distance).
        """
        for index, result in similar_vectors.items():
            logging.warning(f"Results for the {index + 1}th input vector:")
            for doc_id, distance in result:
                logging.warning(f"ID: {doc_id}, Distance: {distance}")


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
    parser = argparse.ArgumentParser(description="Vector Similarity Search with FAISS")

    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=9000, help="ClickHouse port")
    parser.add_argument("-u", "--user", default="default", help="ClickHouse username")
    parser.add_argument("-p", "--password", default="", help="ClickHouse password")
    parser.add_argument(
        "--database", default="db_master", help="ClickHouse database name"
    )
    parser.add_argument("--table", default="element", help="ClickHouse table name")
    parser.add_argument("--ids", default="doc_id", help="Column name for document IDs")
    parser.add_argument(
        "--vectors", default="centroid", help="Column name for vector data"
    )
    parser.add_argument(
        "--count", type=int, default=10, help="Number of similar vectors to retrieve"
    )
    parser.add_argument(
        "--file",
        type=str,
        default="test.json",
        help="Path to input JSON file with vectors",
    )

    return parser.parse_args()


def main() -> None:
    """
    The main function that handles database connection, retrieves vector data,
    and performs similarity search.
    """
    args = parse_arguments()

    input_vectors = vectors_from_json(args.file)

    try:
        db = ClickHouseRepository(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )

        vectors_db = db.get_vectors(args.table, args.ids, args.vectors)

        if vectors_db is None:
            logging.error("No vectors retrieved from database.")
            return

        searcher = VectorSearcher(vectors_db)
        similar_vectors = searcher.search_similar(input_vectors, args.count)

        searcher.print_similar_vectors(similar_vectors)

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
