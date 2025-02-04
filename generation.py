import random
import numpy as np
import argparse
import json
import uuid

parser = argparse.ArgumentParser(description='Генерация')

parser.add_argument('--low', type=float, default=0.0, help='Нижняя граница диапазона')
parser.add_argument('--high', type=float, default=1.0, help='Верхняя граница диапазона')
parser.add_argument('--size', type=int, default=512, help='Размер каждого вектора')
parser.add_argument('--count', type=int, default=20000, help='Количество векторов')
parser.add_argument('--file_output', type=str, default='elements.json', help='Имя файла для сохранения векторов.')

args = parser.parse_args()

element = []
for i in range(args.count):
    id = str(uuid.uuid4())
    vector = np.random.uniform(low=args.low, high=args.high, size=args.size).tolist()

    element.append({
        'id': id,
        'vector': vector
    })

output_file_name = args.file_output

with open(output_file_name, 'w') as json_file:
    json.dump(element, json_file, indent=4)




