import random
import numpy as np
import logging
import argparse
import json
import uuid
from typing import List, Dict, Set


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JSONSaver:
    """
    A utility class for saving generated elements to a JSON file.
    """

    @staticmethod
    def save(elements: List[Dict[str, object]], file_output: str) -> None:
        """
        Saves a list of UUIDs and vectors to a JSON file.

        :param elements: A list of dictionaries, each containing a UUID and a vector.
        :param file_output: The file path where the JSON data should be saved.
        """
        try:
            with open(file_output, "w") as json_file:
                json.dump(elements, json_file, indent=4)
            logger.info(f"Vectors have been successfully saved to '{file_output}'.")
        except Exception as e:
            logger.error(f"Error saving JSON file: {e}")


class VectorGenerator:
    """
    A class for generating random UUIDs and vectors.

    This class generates a specified number of vectors, each associated with a unique UUID.
    """

    def __init__(self, low: float, high: float, size: int, count: int) -> None:
        """
        Initializes the generator with given parameters.

        :param low: The lower limit for the vector values.
        :param high: The upper limit for the vector values.
        :param size: The dimension of each vector.
        :param count: The number of vectors to generate.
        """
        self.low = low
        self.high = high
        self.size = size
        self.count = count

    def generate(self) -> List[Dict[str, object]]:
        """
        Generates a list of elements, each containing a random UUID and a vector.

        :return: A list of dictionaries containing UUIDs and vectors.
        """
        existing_uuids: Set[str] = set()
        elements: List[Dict[str, object]] = []

        for _ in range(self.count):
            while True:
                id_uuid = str(uuid.uuid4())
                if id_uuid not in existing_uuids:
                    existing_uuids.add(id_uuid)
                    break

            vector = np.random.uniform(low=self.low, high=self.high, size=self.size).tolist()
            elements.append({"id": id_uuid, "vector": vector})

        logger.info(f"Generated {self.count} vectors with dimension {self.size}.")
        return elements


def main() -> None:
    """
    Parses command-line arguments, generates random vectors, and saves them to a JSON file.
    """
    parser = argparse.ArgumentParser(description="Vector Generation")

    parser.add_argument("--low", type=float, default=0.0, help="Lower limit of the range")
    parser.add_argument("--high", type=float, default=1.0, help="Upper limit of the range")
    parser.add_argument("--size", type=int, default=512, help="The size of each vector")
    parser.add_argument("--count", type=int, default=20000, help="Number of vectors to generate")
    parser.add_argument("--file_output", type=str, default="elements.json", help="Output file name for saving vectors")

    args = parser.parse_args()

    generator = VectorGenerator(args.low, args.high, args.size, args.count)
    elements = generator.generate()

    JSONSaver.save(elements, args.file_output)


if __name__ == "__main__":
    main()