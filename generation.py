import random
import numpy as np
import logging
import argparse
import json
import uuid


'''The main function of the program processes command line arguments, generates vectors and saves them to a file.'''
def main():
    parser = argparse.ArgumentParser(description='Generation')

    parser.add_argument('--low', type=float, default=0.0, help='The lower limit of the range')
    parser.add_argument('--high', type=float, default=1.0, help='Upper limit of the range')
    parser.add_argument('--size', type=int, default=512, help='The size of each vector')
    parser.add_argument('--count', type=int, default=20000, help='Number of vectors')
    parser.add_argument('--file_output', type=str, default='elements.json', help='The name of the file for saving vectors')

    args = parser.parse_args()

    elements = generate(args)
    save_to_json(elements, args.file_output)

    logging.debug(f"Vectors have been successfully saved to a file {args.file_output}")


'''Generates vectors based on the specified parameters'''
def generate(args):
    existing_uuids = set()

    elements = []
    for i in range(args.count):

        while True:
            id_uuid = str(uuid.uuid4())
            if id_uuid not in existing_uuids:
                existing_uuids.add(id_uuid)
                break

        vector = np.random.uniform(low=args.low, high=args.high, size=args.size).tolist()

        elements.append({
            'id': id_uuid,
            'vector': vector})

    return elements


'''Saves data to a file in JSON format'''
def save_to_json(elements, file_output):
    with open(file_output, 'w') as json_file:
        json.dump(elements, json_file, indent=4)


if __name__ == "__main__":
    main()
