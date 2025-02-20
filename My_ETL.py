# 1. Скачайте файлы boking.csv, client.csv и hotel.csv;
# 2. Создайте новый dag;
# 3. Создайте три оператора для получения данных и загрузите файлы. Передайте дата фреймы в оператор трансформации;
# 4. Создайте оператор который будет трансформировать данные:
# — Объедините все таблицы в одну;
# — Приведите даты к одному виду;
# — Удалите невалидные колонки;
# — Приведите все валюты к одной;
# 5. Создайте оператор загрузки в базу данных;
# 6. Запустите dag.


import pandas as pd
import requests
import sqlalchemy
import csv
import os
import sqlalchemy


def download_df(url, name):
    """Функция для скачивания файлов"""
    # Директория для сохранения файла (например, текущая директория)
    save_directory = '.\df'

    # Имя файла
    filename = f'{name}.csv'

    # Путь к файлу
    file_path = os.path.join(save_directory, filename)

    # Загрузка файла
    response = requests.get(url)
    if response.status_code == 200:
        # Запись содержимого файла
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f'Файл {filename} успешно загружен и сохранён в {save_directory}')
    else:
        print(f'Ошибка при загрузке файла. Код статуса: {response.status_code}')

def my_datatime(row):
    """Функция для привидения даты-время к единообразию в столбце"""
    if '-' in row['booking_date']:
        row['booking_date'] = row['booking_date'].replace('-', '/')
    return row

def my_exchange_rate(row):
    """Функция для перевода Фунтов в Евро"""
    exchange_EUR = 1.2

    if isinstance(row['currency'], str) and 'GBP' in row['currency']:
        row['booking_cost'] = row['booking_cost'] * exchange_EUR
        row['currency'] = 'EUR'
    return row

def create_engine(user, password, host, port, database):
    url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    print(url)
    engine = sqlalchemy.create_engine(url)
    return engine

def to_SQL(df):
    """Функция для загрузки данных в локальную базу данных"""
    user = "root"
    password = "rackot123"
    host = "127.0.0.1"
    port = 3306
    database = "ETL"

    engine = create_engine(user, password, host, port, database)
    print(123)
    df.to_sql('my_table', con=engine, if_exists='replace', index=False)


if __name__ == "__main__":
    # ссылки на файлы из интернета
    url_boking = 'https://gbcdn.mrgcdn.ru/uploads/asset/5551670/attachment/6257a083503973164c0bb0571d41d9e8.csv'
    url_client = 'https://gbcdn.mrgcdn.ru/uploads/asset/5551674/attachment/7c6bf202bd10996ca60a2593f755d4f4.csv'
    url_hotel = 'https://gbcdn.mrgcdn.ru/uploads/asset/5551688/attachment/3ed446d2c750d05b6c177f62641af670.csv'

    list_url = [url_boking, url_client, url_hotel]
    list_name = ['boking', 'client', 'hotel'] # Список имен для файлов

    # Скачиваю файлы из интернета и загружаю их в директорию с проектом
    for i, j in zip(list_url, list_name):
        download_df(i, j)


    df_booking = pd.read_csv(r'.\\df\\boking.csv')
    df_hotel = pd.read_csv(r'.\\df\\hotel.csv')
    df_client = pd.read_csv(r'.\\df\\client.csv')


    merged_df_client_booking = pd.merge(df_client, df_booking, on='client_id', how='inner') # Обьединяю client и booking
    df_all = pd.merge(merged_df_client_booking, df_hotel, on='hotel_id', how='inner') # Обьединяю все три таблицы в одну

    result = df_all.apply(my_datatime, axis=1)
    result['booking_date'] = pd.to_datetime(result['booking_date'])
    result_in_EUR = result.apply(my_exchange_rate, axis=1).drop(columns=['client_id'])
    to_SQL(result_in_EUR)
    print(result_in_EUR)
