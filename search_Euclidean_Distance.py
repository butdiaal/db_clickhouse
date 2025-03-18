import logging
import argparse
import numpy as np
from typing import List, Dict, Tuple
from utils import Queries, VectorUtils, ClickHouseConnection


logging.basicConfig(level=logging.INFO)


class ClickHouseRepository:
    """
    A repository class for interacting with a ClickHouse database.

    """

    def __init__(self, connection: ClickHouseConnection):
        """Initializes the repository with an existing ClickHouse connection."""
        self.client = connection.get_client()
        self.database = connection.database

    def get_vectors(self, table: str, ids: str, vectors: str) -> Dict[str, np.ndarray]:
        """
        Retrieves vector data from a specified ClickHouse table.

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
            return {}


class VectorSearcher:
    """
    A class for performing similarity searches on vector data.
    """

    @staticmethod
    def euclidean_distance(a: np.ndarray, b: np.ndarray) -> float:
        """
        Computes the Euclidean distance between two vectors.

        :param a: The first vector.
        :param b: The second vector.
        :return: The Euclidean distance between the two vectors.
        """
        return np.sqrt(np.sum((a - b) ** 2))

    def search_similar(
        self,
        vectors_index: Dict[str, np.ndarray],
        input_vectors: List[List[float]],
        count: int,
    ) -> Dict[int, List[Tuple[str, float]]]:
        """
        Finds the most similar vectors based on Euclidean distance.

        :param vectors_index: A dictionary where keys are document IDs and values are stored vectors.
        :param input_vectors: A list of vectors for which similar ones need to be found.
        :param count: The number of similar vectors to return for each input vector.
        :return: A dictionary mapping input vector indices to lists of tuples (document ID, distance).
        """
        if not vectors_index:
            return {}

        similar_vectors: Dict[int, List[Tuple[str, float]]] = {}

        for idx, input_vector in enumerate(input_vectors):
            input_vector_np = np.array(input_vector, dtype="float64")

            similarities = [
                (doc_id, self.euclidean_distance(input_vector_np, vector))
                for doc_id, vector in vectors_index.items()
            ]

            sorted_similarities = sorted(similarities, key=lambda x: x[1])

            similar_vectors[idx] = sorted_similarities[:count]

        return similar_vectors


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    :return: Parsed arguments as a namespace object.
    """
    parser = argparse.ArgumentParser(description="Vector Similarity Search")

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

    input_vectors = VectorUtils.vectors_from_json(args.file)

    try:
        connection = ClickHouseConnection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )

        db = ClickHouseRepository(connection)

        vectors_db = db.get_vectors(args.table, args.ids, args.vectors)

        searcher = VectorSearcher()
        similar_vectors = searcher.search_similar(vectors_db, input_vectors, args.count)

        VectorUtils.print_similar_vectors(similar_vectors)

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
