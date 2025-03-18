import logging
import argparse
from typing import List, Tuple, Dict
from utils import Queries, VectorUtils, ClickHouseConnection

logging.basicConfig(level=logging.INFO)


class ClickHouseRepository:
    """
    A repository class for executing vector similarity searches in ClickHouse.
    """

    def __init__(self, connection: ClickHouseConnection):
        """Initializes the repository with an existing ClickHouse connection."""
        self.client = connection.get_client()
        self.database = connection.database

    def search_similar_vectors(
        self,
        input_vectors: List[List[float]],
        table: str,
        id_column: str,
        vector_column: str,
        count: int,
    ) -> Dict[int, List[Tuple[str, float]]]:
        """
        Finds the most similar vectors using the L2 (Euclidean) distance function in ClickHouse.

        :param input_vectors: A list of input vectors.
        :param table: The name of the table.
        :param id_column: The column name for document IDs.
        :param vector_column: The column name for vector data.
        :param count: The number of most similar vectors to retrieve.
        :return: A dictionary where keys are indices of input vectors and values are lists of tuples with document IDs and distances.
        """
        results_dict = {}

        for index, input_vector in enumerate(input_vectors, start=1):
            vector_str = "[" + ",".join(map(str, input_vector)) + "]"

            query = Queries.SEARCH_SIMILAR_cosineDistance.format(
                vector=vector_str,
                database=self.database,
                table=table,
                id_column=id_column,
                vector_column=vector_column,
                count=count,
            )

            result = self.client.execute(query)
            results_dict[index] = result

        return results_dict


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
    Main function that handles database connection, retrieves data,
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

        similar_vectors = db.search_similar_vectors(
            input_vectors, args.table, args.ids, args.vectors, args.count
        )

        VectorUtils.print_similar_vectors(similar_vectors)

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
