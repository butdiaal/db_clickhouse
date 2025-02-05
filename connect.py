from clickhouse_driver import Client
import argparse

parser = argparse.ArgumentParser(description='Подключение к серверу')

parser.add_argument('--host', default='localhost', help='Хост')
parser.add_argument('--port', type=int, default=9000, help='Порт')
parser.add_argument('-u', '--user', default='default', help='Имя пользователя')
parser.add_argument('-p', '--password', default='', help='Пароль')

args = parser.parse_args()

success = False

try:
    client = Client(host=args.host, port=args.port, user=args.user, password=args.password)
    print('Подключение успешно.')

    try:
        client.execute('''CREATE DATABASE IF NOT EXISTS db_master''')
        print('База данных успешно создана.')

        client.execute('''CREATE TABLE IF NOT EXISTS element
            (doc_id UUID, 
            centroid Array(Float64)) 
                ENGINE = MergeTree() 
                ORDER BY doc_id''')
        print('Таблица успешно создана.')

        success = True

    except Exception as e:
        print(f"Ошибка создания: {e}.")


except errors.ServerException as e:
    print(f'Ошибка подключения к ClickHouse: {e}.')


