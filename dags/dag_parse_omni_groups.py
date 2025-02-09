from airflow import DAG
from airflow.operators.python import PythonOperator

import aiohttp
import asyncio
import asyncpg

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import functions_general as fg
from functions import functions_data as fd
from queries import queries_log as ql, queries_insert as qi


def group_data_extractor(record):
    # Извлечение и предобработка данных
    return (
        record.get('group_id'),
        record.get('group_title'),
        record.get('active'),
        fd.fix_datetime(record.get('created_at')),
        fd.fix_datetime(record.get('updated_at'))
    )


async def fetch_and_process_groups():
    """
    Основная функция для получения данных о группах и их вставки в базу данных.
    """
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        async with pool.acquire() as conn:
            total_count = await fg.get_snapshot(session, 'groups')
            try:
                url = f'{OMNI_URL}/groups.json?limit=100'  # Формируем ссылку для API запроса
                response = await fg.fetch_response(session, url)  # Создание запроса для получения данных
                response_data = fg.fetch_data(response, group_data_extractor, 'group')  # Извлечение данных
                await qi.insert_groups(conn, response_data)  # Вставка данных в базу.

            finally:
                await ql.log_etl_catalogues(conn, 'dim_omni_group', total_count)
                await conn.close()  # Закрываем соединение с базой данных.


def run_async_func():
    """Запускает асинхронную функцию для получения и вставки данных о группах."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_process_groups())


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
