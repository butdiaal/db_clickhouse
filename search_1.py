import json
import logging
import argparse
import numpy as np
from clickhouse_driver import Client, errors


def print_similar_vectors(similar_vectors: dict[int, list[tuple[str, float]]]) -> None:
    """
    Logs the results of similar vector searches.

    :param similar_vectors: A dictionary where keys are input vector indices
                            and values are lists of tuples (document ID, distance).
    """
    for index, result in similar_vectors.items():
        logging.warning(f"Results for the {index+1}th vector:")
        for row in result:
            logging.warning(f"ID: {row[0]}, Distance: {row[1]}")


def search_similar(
    vectors_index: dict[str, np.ndarray], input_vectors: list[list[float]], count: int
) -> dict[int, list[tuple[str, float]]]:
    """
    Searches for the most similar vectors using Euclidean distance.

    :param vectors_index: A dictionary of stored vectors with document IDs as keys.
    :param input_vectors: A list of input vectors for which to find similar vectors.
    :param count: The number of similar vectors to return.
    :return: A dictionary mapping input vector indices to lists of similar document IDs and distances.
    """
    if len(vectors_index) == 0:
        return {}

    similar_vectors = {}

    for idx, input_vector in enumerate(input_vectors):
        input_vector = np.array(input_vector).astype("float64")

        similarities = [
            (doc_id, euclidean_distance(input_vector, vector))
            for doc_id, vector in vectors_index.items()
        ]

        sorted_similarities = sorted(similarities, key=lambda x: x[1])

        similar_vectors[idx] = sorted_similarities[:count]

    return similar_vectors


def euclidean_distance(a: np.ndarray, b: np.ndarray) -> float:
    """
    Calculates the Euclidean distance between two vectors.

    :param a: First vector.
    :param b: Second vector.
    :return: Euclidean distance between a and b.
    """
    return np.sqrt(np.sum((a - b) ** 2))


def get_vectors(
    client: Client, database: str, table: str, ids: str, vectors: str
) -> dict[str, np.ndarray]:
    """
    Retrieves vector data from the ClickHouse database.

    :param client: ClickHouse client instance.
    :param database: The name of the database.
    :param table: The name of the table.
    :param ids: The column name for document IDs.
    :param vectors: The column name for vector data.
    :return: A dictionary mapping document IDs to vector arrays.
    """
    try:
        result = client.execute(f"""SELECT {ids}, {vectors} FROM {database}.{table}""")
        logging.warning("Data found and received")

        vectors_index = {}
        for row in result:
            doc_id, centroid = row
            vectors_index[str(doc_id)] = np.array(centroid).astype("float64")

        return vectors_index

    except Exception as e:
        logging.error(f"Error when receiving data: {e}")
        return None


def vectors_from_json(file_path: str) -> list[list[float]]:
    """
    Loads input vectors from a JSON file.

    :param file_path: Path to the JSON file.
    :return: A list of vectors.
    """
    with open(file_path, "r") as f:
        data = json.load(f)
    input_vectors = [item["vector"] for item in data]
    return input_vectors


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    :return: Parsed arguments as a namespace object.
    """
    parser = argparse.ArgumentParser(description="The first search method")

    parser.add_argument("--host", default="localhost", help="Host")
    parser.add_argument("--port", default=9000, help="Port")
    parser.add_argument("-u", "--user", default="default", help="User")
    parser.add_argument("-p", "--password", default="", help="Password")
    parser.add_argument("--database", default="db_master", help="Name of the database")
    parser.add_argument("--table", default="element", help="Table name")
    parser.add_argument("--ids", default="doc_id", help="Id database attribute")
    parser.add_argument(
        "--vectors", default="centroid", help="The vector database attribute"
    )
    parser.add_argument(
        "--low", type=float, default=0.0, help="The lower limit of the range"
    )
    parser.add_argument(
        "--high", type=float, default=1.0, help="Upper limit of the range"
    )
    parser.add_argument("--size", type=int, default=512, help="The size of each vector")
    parser.add_argument("--count", type=int, default=10, help="Count of similar data")
    parser.add_argument(
        "--file",
        type=str,
        default="test.json",
        help="Vector storage file for searching",
    )

    return parser.parse_args()


def main() -> None:
    """
    Main function that handles database connection, retrieves data,
    and performs similarity search.
    """

    args = parse_arguments()

    input_vectors = vectors_from_json(args.file)

    try:
        client = Client(
            host=args.host, port=args.port, user=args.user, password=args.password
        )
        logging.warning("Connection successful")

        vectors_db = get_vectors(
            client, args.database, args.table, args.ids, args.vectors
        )

        similar_vectors = search_similar(vectors_db, input_vectors, args.count)
        print_similar_vectors(similar_vectors)

    except Exception as e:
        logging.error(f"An error has occurred: {e}")


if __name__ == "__main__":
    main()
