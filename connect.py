from clickhouse_driver import Client

host = 'localhost'
port = 9000
user = 'default'
password = ''

try:
    client = Client(host=host, port=port, user=user, password=password)

    try:
        client.execute('''CREATE DATABASE IF NOT EXISTS db_master''')

        client.execute('''CREATE TABLE IF NOT EXISTS element
            (doc_id UInt32, 
            centroid Array(Float64)) 
                ENGINE = MergeTree() 
                ORDER BY doc_id''')

    except Exception as e:
        print(f"Ошибка создания: {e}")


except errors.ServerException as e:
    print(f'Ошибка подключения к ClickHouse: {e}')
