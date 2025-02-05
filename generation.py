import random
import numpy as np
import argparse
import json
import uuid

output_file_name = None

def main():
    parser = argparse.ArgumentParser(description='Генерация')

    parser.add_argument('--low', type=float, default=0.0, help='Нижняя граница диапазона')
    parser.add_argument('--high', type=float, default=1.0, help='Верхняя граница диапазона')
    parser.add_argument('--size', type=int, default=512, help='Размер каждого вектора')
    parser.add_argument('--count', type=int, default=20000, help='Количество векторов')
    parser.add_argument('--file_output', type=str, default='elements.json', help='Имя файла для сохранения векторов.')

    args = parser.parse_args()

    output_file_name = args.file_output

    elements = generate(args)
    save_to_json(elements, output_file_name)

    print(f"Векторы успешно сохранены в файл {output_file_name}")


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


def save_to_json(elements, output_file_name):
    with open(output_file_name, 'w') as json_file:
        json.dump(elements, json_file, indent=4)


if __name__ == "__main__":
    main()
