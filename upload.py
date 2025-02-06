from generation import output_file_name
from clickhouse_driver import Client
from connect import success, client
import os
import json


def load_data_from_json(file_path):
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        with open(file_path, 'r') as file:
            elements = json.load(file)

        data_to_load = []
        for element in elements:
            doc_id = element['id']
            centroid = element['vector']
            data_to_load.append((doc_id, centroid))

        return data_to_load
    else:
        raise FileNotFoundError(f"Файл '{file_path}' не существует или он пуст.")


def insert_data(data_to_load):
    if success:
        try:
            client.execute('INSERT INTO db_master.element (doc_id, centroid) VALUES', data_to_load)
            print("Данные успешно вставлены.")
        except Exception as e:
            print(f"Ошибка вставки данных: {e}.")
    else:
        print("Не удалось установить соединение с ClickHouse.")


if __name__ == "__main__":
    try:
        data = load_data_from_json(output_file_name)
        insert_data(data)
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"Произошла ошибка: {e}.")
