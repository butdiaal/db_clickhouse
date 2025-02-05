from clickhouse_driver import Client, errors
import argparse

success = False
client = None

def main():
    global client
    global success
    parser = argparse.ArgumentParser(description='Подключение к серверу')

    parser.add_argument('--host', default='localhost', help='Хост')
    parser.add_argument('--port', type=int, default=9000, help='Порт')
    parser.add_argument('-u', '--user', default='default', help='Имя пользователя')
    parser.add_argument('-p', '--password', default='', help='Пароль')
    parser.add_argument('--database', default='db_master', help='Имя базы данных')
    parser.add_argument('--table', default='element', help='Имя таблицы')

    args = parser.parse_args()

    try:
        client = Client(host=args.host, port=args.port, user=args.user, password=args.password)
        print('Подключение успешно.')
        create_db(client, args.database, args.table)

    except errors.ServerException as e:
        print(f'Ошибка подключения к ClickHouse: {e}.')


def create_db(client, database_name, table_name):
    global success
    try:
        client.execute(f'''CREATE DATABASE IF NOT EXISTS {database_name}''')
        print('База данных успешно создана.')

        client.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                doc_id UUID,
                centroid Array(Float64)
            )
            ENGINE = MergeTree()
            ORDER BY doc_id
        ''')
        print('Таблица успешно создана.')

        success = True

    except Exception as e:
        print(f"Ошибка создания: {e}.")

if __name__ == "__main__":
    main()


