from airflow import DAG
from airflow.operators.python import PythonOperator

import aiohttp
import asyncio
import asyncpg

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import fix_null, fix_datetime, fix_int, fetch_data, fetch_response


def company_data_extractor(record):
    return (
        record.get('company_id'),
        fix_null(record.get('company_name')),
        fix_null(record.get('custom_fields', {}).get('cf_8963')),  # tarif
        fix_int(record.get('custom_fields', {}).get('cf_8591')),  # btrx_id
        fix_null(record.get('custom_fields', {}).get('cf_8602')),  # responsible
        record.get('active'),
        record.get('deleted'),
        fix_datetime(record.get('created_at')),
        fix_datetime(record.get('updated_at'))
    )


async def insert_into_db(response_data, conn):
    """
    Вставляет данные о компаниях в базу данных.

    Аргументы:
    response_data -- список данных о компаниях для вставки.
    conn -- соединение с базой данных.
    """
    query = """
        INSERT INTO dim_omni_company(
            company_id,
            company_name,
            tarif,
            btrx_id,
            responsible,
            active,
            deleted,
            created_date,
            updated_date
        ) 
        VALUES($1, $2, (SELECT json_data::json ->> $3 FROM lookup_omni_custom_field WHERE field_id = 8963), $4, $5, $6, $7, $8, $9) 
        ON CONFLICT (company_id) DO UPDATE
        SET company_name = EXCLUDED.company_name,
            tarif = EXCLUDED.tarif,
            btrx_id = EXCLUDED.btrx_id,
            responsible = EXCLUDED.responsible,
            active = EXCLUDED.active,
            deleted = EXCLUDED.deleted,
            created_date = EXCLUDED.created_date,
            updated_date = EXCLUDED.updated_date;
    """

    await conn.executemany(query, response_data)  # Выполняет пакетную вставку данных.


async def fetch_and_insert():
    """
    Основная функция для получения данных о компаниях и их вставки в базу данных.
    """
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session:
        conn = await asyncpg.connect(**DB_CONFIG)  # Подключение к базе данных.
        print("Успешное подключение к БД.")
        try:
            tasks = []
            page = 1

            while True:
                print('-', page)
                url = f'{OMNI_URL}/companies.json?page={page}&limit=100'  # Формируем ссылку для API запроса
                task = fetch_response(session, url)
                tasks.append(task)  # Добавляем задачу для парсинга.

                if len(tasks) >= 5:  # Обработка 5 страниц одновременно
                    responses = await asyncio.gather(*tasks)
                    tasks = []  # Очистка задач
                    for response in responses:
                        if response is None or len(response) <= 1:
                            print("Все данные обработаны.")
                            return

                        response_data = fetch_data(response, company_data_extractor, 'company')  # Извлечение данных
                        await insert_into_db(response_data, conn)  # Вставка данных в базу.

                page += 1  # Переходим к следующей странице.

        finally:
            if tasks:  # Обрабатываем любые оставшиеся задачи.
                responses = await asyncio.gather(*tasks)
                for response in responses:
                    if response is None or len(response) <= 1:
                        print("Все данные обработаны.")
                        return

            await conn.close()  # Закрываем соединение с базой данных.
            print("Закрыто соединение с БД.")



def run_async_func():
    """Запускает асинхронную функцию для получения и вставки данных о компаниях."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_insert())


# Создаем DAG
with DAG(
        'dag_parse_omni_companies',
        default_args=DAG_CONFIG,
        schedule_interval=None,  # Не запускать автоматически
        catchup=False,
) as dag:

    fetch_companies_task = PythonOperator(
        task_id='parse_omni_companies',
        python_callable=run_async_func,
    )

    fetch_companies_task
