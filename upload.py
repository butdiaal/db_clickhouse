import logging
import argparse
import os
import json
from clickhouse_driver import Client, errors
from connect import check_db


def insert_data(client: Client, data_to_load: List[Tuple[str, List[float]]]) -> None:
    """
    Inserts data into the ClickHouse database.

    :param client: ClickHouse client instance.
    :param data_to_load: List of tuples containing document IDs and vector data.
    """
    if data_to_load:
        client.execute(
            "INSERT INTO db_master.element (doc_id, centroid) VALUES", data_to_load
        )
        logging.warning("The data has been successfully inserted")
    else:
        logging.error("No data to insert.")


def load_data(file_input: str) -> List[Tuple[str, List[float]]]:
    """
    Loads vector data from a JSON file.

    :param file_input: Path to the input JSON file.
    :return: A list of tuples containing document IDs and vectors.
    """
    data_to_load = []

    if os.path.exists(file_input) and os.path.getsize(file_input) > 0:
        with open(file_input, "r") as file:
            elements = json.load(file)

        for element in elements:
            doc_id = element["id"]
            centroid = element["vector"]
            data_to_load.append((doc_id, centroid))

    else:
        logging.error(f"The file {file_input} does not exist or it is empty")

    return data_to_load


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    :return: Parsed arguments as a namespace object.
    """
    parser = argparse.ArgumentParser(description="Uploading to the database")

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
        "--file_input",
        type=str,
        default="elements.json",
        help="The name of the file where the vectors are saved",
    )
    return parser.parse_args()


def main() -> None:
    """
    Main function that handles database connection, file waiting, and data insertion.
    """

    args = parse_arguments()

    try:
        client = Client(
            host=args.host, port=args.port, user=args.user, password=args.password
        )
        logging.warning("Connection successful")

        check_db(client, args.database, args.table, args.ids, args.vectors)

        data = load_data(args.file_input)
        if data != None:
            insert_data(client, data)

    except Exception as e:
        logging.error(f"An error has occurred: {e}")


if __name__ == "__main__":
    main()
