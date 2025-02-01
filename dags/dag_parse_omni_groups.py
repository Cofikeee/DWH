from airflow import DAG
from airflow.operators.python import PythonOperator

import aiohttp
import asyncio
import asyncpg

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import fix_datetime, fetch_data, fetch_response


def group_data_extractor(record):
    # Извлечение и предобработка данных
    return (
        record.get('group_id'),
        record.get('group_title'),
        record.get('active'),
        fix_datetime(record.get('created_at')),
        fix_datetime(record.get('updated_at'))
    )


async def insert_into_db(response_data, conn):
    """
    Вставляет данные о группах в базу данных.

    Аргументы:
    response_data -- список данных о группах для вставки.
    conn -- соединение с базой данных.
    """
    query = """
        INSERT INTO dim_omni_group(
            group_id,
            group_name,
            active,
            created_date,
            updated_date
        ) 
        VALUES($1, $2, $3, $4, $5) 
        ON CONFLICT (group_id) DO UPDATE
        SET group_name = EXCLUDED.group_name,
            active = EXCLUDED.active,
            created_date = EXCLUDED.created_date,
            updated_date = EXCLUDED.updated_date;
    """

    await conn.executemany(query, response_data)  # Выполняет пакетную вставку данных.


async def fetch_and_insert():
    """
    Основная функция для получения данных о группах и их вставки в базу данных.
    """
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session:
        conn = await asyncpg.connect(**DB_CONFIG)  # Подключение к базе данных.
        print('Успешное подключение к БД')
        try:
            url = f'{OMNI_URL}/groups.json?limit=100'  # Формируем ссылку для API запроса
            response = await fetch_response(session, url)  # Создание запроса для получения данных
            response_data = fetch_data(response, group_data_extractor, 'group')  # Извлечение данных
            await insert_into_db(response_data, conn)  # Вставка данных в базу.

        finally:
            await conn.close()  # Закрываем соединение с базой данных.
            print('Закрыто соединение с БД')


def run_async_func():
    """Запускает асинхронную функцию для получения и вставки данных о группах."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_insert())


# Создаем DAG
with DAG(
        'dag_parse_omni_groups',
        default_args=DAG_CONFIG,
        schedule_interval=None,  # Не запускать автоматически
        catchup=False,
) as dag:

    fetch_groups_task = PythonOperator(
        task_id='parse_omni_groups',
        python_callable=run_async_func,
    )

    fetch_groups_task
