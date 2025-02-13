import json
import logging
import argparse
import numpy as np
from clickhouse_driver import Client, errors


def print_similar_vectors(
    similar_vectors: list[tuple[int, list[tuple[str, float]]]],
) -> None:
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


def similar_vectors(
    client: Client,
    input_vectors: list[list[float]],
    database: str,
    table: str,
    id_column: str,
    vector_column: str,
    count: int,
) -> list[tuple[int, list[tuple[str, float]]]]:
    """
    Finds the most similar vectors using the L2 (Euclidean) distance function in ClickHouse.

    :param client: ClickHouse client instance.
    :param input_vectors: A list of input vectors.
    :param database: The name of the database.
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

            result = client.execute(
                f"""
                    WITH {vector_str} AS reference_vector
                     SELECT 
                        {id_column}, 
                        L2Distance({vector_column}, reference_vector) AS distance
                    FROM {database}.{table}
                    ORDER BY distance
                    LIMIT {count}"""
            )

            all_results.append((index, result))

        logging.warning("Query executed successfully")
        return all_results
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return []


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
    parser.add_argument("--id", default="doc_id", help="Id database attribute")
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

        similar = similar_vectors(
            client,
            input_vectors,
            args.database,
            args.table,
            args.id,
            args.vectors,
            args.count,
        )
        print_similar_vectors(similar)

    except Exception as e:
        logging.error(f"An error has occurred: {e}")


if __name__ == "__main__":
    main()
