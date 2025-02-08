import logging
import argparse
import os
import json
from clickhouse_driver import Client, errors
from connect import check_db


"""Main function, configures arguments, connects to the database and loads data from JSON into the table"""


def main():
    parser = argparse.ArgumentParser(description="Uploading to the database")

    parser.add_argument("--host", default="localhost", help="Host")
    parser.add_argument("--port", type=int, default=9000, help="Port")
    parser.add_argument("-u", "--user", default="default", help="User")
    parser.add_argument("-p", "--password", default="", help="Password")
    parser.add_argument("--database", default="db_master", help="Name of the database")
    parser.add_argument("--table", default="element", help="Table name")
    parser.add_argument(
        "--file_input",
        type=str,
        default="elements.json",
        help="The name of the file where the vectors are saved",
    )

    args = parser.parse_args()

    try:
        client = Client(
            host=args.host, port=args.port, user=args.user, password=args.password
        )
        logging.debug("Connection successful")

        check_db(client, args.database, args.table)

        data = load_data(args.file_input)
        if data != None:
            insert_data(client, data)

    except errors.ServerException as e:
        logging.debug(f"Error connecting to ClickHouse: {e}")
    except Exception as e:
        logging.debug(f"An error has occurred: {e}")


"""Loads data from a JSON file"""


def load_data(file_input):
    if os.path.exists(file_input) and os.path.getsize(file_input) > 0:
        with open(file_input, "r") as file:
            elements = json.load(file)

        data_to_load = []
        for element in elements:
            doc_id = element["id"]
            centroid = element["vector"]
            data_to_load.append((doc_id, centroid))

        return data_to_load
    else:
        logging.debug(f"The file {file_input} does not exist or it is empty")
        return None


"""Inserts data into a database table"""


def insert_data(client, data_to_load):
    if data_to_load:
        try:
            client.execute(
                "INSERT INTO db_master.element (doc_id, centroid) VALUES", data_to_load
            )
            logging.debug("The data has been successfully inserted")
        except Exception as e:
            logging.debug(f"Data insertion error: {e}")
    else:
        logging.debug("No data to insert.")


if __name__ == "__main__":
    main()
