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
    parser.add_argument("--index", default="idx", help="The index database attribute")

    args = parser.parse_args()

    input_vector = generate_vector(args.low, args.high, args.size)

    try:
        client = Client(
            host=args.host, port=args.port, user=args.user, password=args.password
        )
        logging.error("Connection successful")

        check_db(client, args.database, args.table, args.id, args.vector)

        check_index(client, args.database, args.table, args.vector, args.index)

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


"""Checks whether the index exists in the table"""


def check_index(client, database, table, vector_column, index_name):
    client.execute(f"""SET allow_experimental_vector_similarity_index = 1;""")

    result = client.execute(f"SHOW CREATE TABLE {database}.{table}")

    create_table_statement = result[0][0]

    if f"INDEX {index_name}" in create_table_statement:
        logging.error(f"The index '{index_name}' was successfully added")
    else:
        logging.error(f"The index '{index_name}' already exists")
        add_index(client, database, table, index_name, index_name)


"""The index is added to the database table"""


def add_index(client, database, table, vector_column, index_name):
    client.execute(f"""SET allow_experimental_vector_similarity_index = 1;""")

    client.execute(
        f"""ALTER TABLE {database}.{table}
            ADD INDEX {index_name} {vector_column} TYPE vector_similarity('hnsw', 'L2Distance') GRANULARITY 1;
        """
    )


"""Generates a vector"""


def generate_vector(low, high, size):
    vector = np.random.uniform(low=low, high=high, size=size).tolist()
    return vector


"""Calculating the Euclidean distance in an SQL query"""


def similar_vectors(
    client, input_vector, database, table, id_column, vector_column, count
):
    try:
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
        logging.error("Query executed successfully")
        return result
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return []


def print_similar_vectors(similar_vectors):
    logging.error("Similar vectors:")
    if similar_vectors:
        for vector in similar_vectors:
            doc_id = vector[0]
            distance = vector[1]
            logging.error(f"ID: {doc_id}, Distance: {distance:.2f}")
    else:
        print("No similar vectors found.")


if __name__ == "__main__":
    main()
