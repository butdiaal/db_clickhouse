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

    input_vector = generate_vector(args.low, args.high, args.size)

    try:
        client = Client(
            host=args.host, port=args.port, user=args.user, password=args.password
        )
        logging.error("Connection successful")

        check_db(client, args.database, args.table, args.id, args.vector)
        # output_index(client, args.database, args.table, args.id, args.vector)
        similar = similar_vectors(
            client,
            input_vector,
            args.database,
            args.table,
            args.id,
            args.vector,
            args.count,
        )
        print_similar_vectors(similar)

    except errors.ServerException as e:
        logging.error(f"Error connecting to ClickHouse: {e}")
    except Exception as e:
        logging.error(f"An error has occurred: {e}")


"""Generates a vector"""


def generate_vector(low, high, size):
    vector = np.random.uniform(low=low, high=high, size=size).tolist()
    return vector


def output_index(client, database, table, id_column, vector_column):
    result = client.execute(
        f"""CREATE INDEX idx_bloom_filter ON {database}.{table}({vector_column}) TYPE bloom_filter GRANULARITY 3;"""
    )


"""Calculating the Euclidean distance in an SQL query"""


def similar_vectors(
    client, input_vector, database, table, id_column, vector_column, count
):
    try:
        vector_str = "[" + ",".join(map(str, input_vector)) + "]"
        result = client.execute(
            f"""SELECT
            {id_column},
            {vector_column},
            sqrt(arraySum(arrayMap(i -> power({vector_column}[i] - {vector_str}[i], 2), range(length({vector_column}))))) AS distance
        FROM
            {database}.{table}
        WHERE
            length({vector_column}) = length({vector_str}) 
        ORDER BY
            distance ASC
        LIMIT {count};"""
        )
        logging.info("Query executed successfully")
        return result
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return []


def print_similar_vectors(similar_vectors):
    logging.error("Similar vectors:")
    for doc_id, distance in similar_vectors:
        logging.error(f"ID: {doc_id}, Distance: {distance:.2f}")


if __name__ == "__main__":
    main()
