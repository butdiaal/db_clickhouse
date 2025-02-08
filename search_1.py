import uuid
import json
import faiss
import random
import logging
import argparse
import numpy as np
from clickhouse_driver import Client, errors
from connect import check_db

"""The main function, configuration parameters, the function searches for similar vectors in the ClickHouse database."""


def main():
    parser = argparse.ArgumentParser(description="The first search method")

    parser.add_argument(
        "--low", type=float, default=0.0, help="The lower limit of the range"
    )
    parser.add_argument(
        "--high", type=float, default=1.0, help="Upper limit of the range"
    )
    parser.add_argument("--size", type=int, default=512, help="The size of each vector")
    parser.add_argument("--count", type=int, default=10, help="Count of similar data")
    parser.add_argument("--host", default="localhost", help="Host")
    parser.add_argument("--port", type=int, default=9000, help="Port")
    parser.add_argument("-u", "--user", default="default", help="User")
    parser.add_argument("-p", "--password", default="", help="Password")
    parser.add_argument("--database", default="db_master", help="Name of the database")
    parser.add_argument("--table", default="element", help="Table name")
    parser.add_argument("--id", default="doc_id", help="Id database attribute")
    parser.add_argument(
        "--vector", default="centroid", help="The vector database attribute"
    )

    args = parser.parse_args()

    vector = generate_vector(args.low, args.high, args.size)

    try:
        client = Client(
            host=args.host, port=args.port, user=args.user, password=args.password
        )
        logging.error("Connection successful")

        vectors_db = get_vectors(
            client, args.database, args.table, args.id, args.vector
        )

        check_db(client, args.database, args.table, args.id, args.vector)

        similar_vectors = search_similar(vectors_db, vector, args.count)
        print_similar_vectors(similar_vectors)

    except errors.ServerException as e:
        logging.error(f"Error connecting to ClickHouse: {e}")
    except Exception as e:
        logging.error(f"An error has occurred: {e}")


"""Generates a vector"""


def generate_vector(low, high, size):
    vector = np.random.uniform(low=low, high=high, size=size).tolist()
    return vector


"""Retrieves identifiers and vectors from the Database"""


def get_vectors(client, database, table, ids, vector):
    try:
        result = client.execute(f"""SELECT {ids}, {vector} FROM {database}.{table}""")
        logging.error("Data found and received")

        vectors_index = {}
        for row in result:
            doc_id, centroid = row
            vectors_index[str(doc_id)] = np.array(centroid).astype("float32")

        return vectors_index

    except Exception as e:
        logging.error(f"Error when receiving data: {e}")
        return None


"""Calculates the difference between one vector and vectors from the Database"""


def calculating_distance():
    distances = {}
    for doc_id, db_vector in vectors_index.items():
        distance = np.linalg.norm(db_vector - vector)
        distances[doc_id] = distance

    sorted_distances = sorted(distances.items(), key=lambda item: item[1])
    return sorted_distances


"""Calculates the Euclidean distance"""


def euclidean_distance(a, b):
    return np.sum((a - b) ** 2)


"""Searches for the most similar vectors to the same vector"""


def search_similar(vectors_index, input_vector, count):
    if len(vectors_index) == 0:
        return []

    input_vector = np.array(input_vector)

    similarities = [
        (doc_id, euclidean_distance(input_vector, vector))
        for doc_id, vector in vectors_index.items()
    ]

    sorted_similarities = sorted(similarities, key=lambda x: x[1])

    return sorted_similarities[:count]


def print_similar_vectors(similar_vectors):
    logging.error("Similar vectors:")
    for doc_id, distance in similar_vectors:
        logging.error(f"ID: {doc_id}, Distance: {distance:.2f}")


if __name__ == "__main__":
    main()
