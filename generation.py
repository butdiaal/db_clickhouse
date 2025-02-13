import random
import numpy as np
import logging
import argparse
import json
import uuid


def save_to_json(elements: List[Dict[str, object]], file_output: str) -> None:
    """
    Saves generated elements (UUIDs with vectors) to a JSON file.

    :param elements: List of dictionaries containing UUIDs and vectors.
    :param file_output: File path to save the JSON data.
    """
    with open(file_output, "w") as json_file:
        json.dump(elements, json_file, indent=4)


def generate(low: float, high: float, size: int, count: int) -> List[Dict[str, object]]:
    """
    Generates a list of elements, each containing a random UUID and a vector.

    :param low: Lower limit for vector values.
    :param high: Upper limit for vector values.
    :param size: Dimension of each vector.
    :param count: Number of vectors to generate.
    :return: A list of dictionaries containing UUIDs and vectors.
    """
    existing_uuids = set()

    elements = []
    for i in range(count):

        while True:
            id_uuid = str(uuid.uuid4())
            if id_uuid not in existing_uuids:
                existing_uuids.add(id_uuid)
                break

        vector = np.random.uniform(low=low, high=high, size=size).tolist()

        elements.append({"id": id_uuid, "vector": vector})

    return elements


def main() -> None:
    """
    Parses command-line arguments, generates random vectors, waits for a specified file, and saves the vectors to a file.
    """
    parser = argparse.ArgumentParser(description="Generation")

    parser.add_argument(
        "--low", type=float, default=0.0, help="The lower limit of the range"
    )
    parser.add_argument(
        "--high", type=float, default=1.0, help="Upper limit of the range"
    )
    parser.add_argument("--size", type=int, default=512, help="The size of each vector")
    parser.add_argument("--count", type=int, default=20000, help="Number of vectors")
    parser.add_argument(
        "--file_output",
        type=str,
        default="elements.json",
        help="The name of the file for saving vectors",
    )

    args = parser.parse_args()

    elements = generate(args.low, args.high, args.size, args.count)
    save_to_json(elements, args.file_output)

    logging.warning(f"Vectors have been successfully saved to a file {args.file_output}")


if __name__ == "__main__":
    main()
