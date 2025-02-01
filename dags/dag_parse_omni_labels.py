from airflow import DAG
from airflow.operators.python import PythonOperator

import aiohttp
import asyncio
import asyncpg

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import fetch_data, fetch_response


def labels_data_extractor(record):
    # Извлечение и предобработка данных
    return (
        record.get('label_id'),
        record.get('label_title')
    )


async def insert_into_db(response_data, conn):
    """
    Вставляет данные о метках в базу данных.

    Аргументы:
    response_data -- список данных о метках для вставки.
    conn -- соединение с базой данных.
    """
    query = """
        INSERT INTO dim_omni_label(
            label_id,
            label_name
        ) 
        VALUES($1, $2) 
        ON CONFLICT (label_id) DO UPDATE
        SET label_name = EXCLUDED.label_name;
    """

    await conn.executemany(query, response_data)  # Выполняет пакетную вставку данных.


async def fetch_and_insert():
    """
    Основная функция для получения данных о метках и их вставки в базу данных.
    """
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session:
        conn = await asyncpg.connect(**DB_CONFIG)  # Подключение к базе данных.
        print('Успешное подключение к БД.')
        try:
            tasks = []
            page = 1

            while True:
                print('-', page)
                url = f'{OMNI_URL}/labels.json?page={page}&limit=100'
                task = fetch_response(session, url)
                tasks.append(task)  # Добавляем задачу для парсинга.

                if len(tasks) >= 5:  # Обработка 5 страниц одновременно
                    responses = await asyncio.gather(*tasks)
                    tasks = []  # Очистка задач
                    for response in responses:
                        if response is None or len(response) <= 1:
                            print('Все данные обработаны.')
                            return

                        response_data = fetch_data(response, labels_data_extractor, 'label')  # Извлечение данных
                        await insert_into_db(response_data, conn)  # Вставка данных в базу.

                page += 1  # Переходим к следующей странице.

        finally:
            if tasks:  # Обрабатываем любые оставшиеся задачи.
                responses = await asyncio.gather(*tasks)
                for response in responses:
                    if response is None or len(response) <= 1:
                        print('Все данные обработаны.')
                        return

            await conn.close()  # Закрываем соединение с базой данных.
            print('Закрыто соединение с БД.')



def run_async_func():
    """Запускает асинхронную функцию для получения и вставки данных о метках."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_insert())


# Создаем DAG
with DAG(
        'dag_parse_omni_labels',
        default_args=DAG_CONFIG,
        schedule_interval=None,  # Не запускать автоматически
        catchup=False,
) as dag:

    fetch_labels_task = PythonOperator(
        task_id='parse_omni_labels',
        python_callable=run_async_func,
    )

    fetch_labels_task
