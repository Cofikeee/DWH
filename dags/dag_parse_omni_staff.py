from airflow import DAG
from airflow.operators.python import PythonOperator

import aiohttp
import asyncio
import asyncpg

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import fix_null, fix_datetime, fetch_data, fetch_response


def staff_data_extractor(record):
    # Извлечение и предобработка данных
    return (
        record.get('staff_id'),
        fix_null(record.get('staff_full_name')),
        fix_null(record.get('staff_email')),
        record.get('active'),
        fix_datetime(record.get('created_at')),
        fix_datetime(record.get('updated_at'))
    )


async def insert_into_db(response_data, conn):
    """
    Вставляет данные о сотрудниках в базу данных.

    Аргументы:
    response_data -- список данных о сотрудниках для вставки.
    conn -- соединение с базой данных.
    """
    query = """
        INSERT INTO dim_omni_staff(
            staff_id,
            full_name,
            email,
            active,
            created_date,
            updated_date
        ) 
        VALUES($1, $2, $3, $4, $5, $6) 
        ON CONFLICT (staff_id) DO UPDATE
        SET full_name = EXCLUDED.full_name,
            email = EXCLUDED.email,
            active = EXCLUDED.active,
            created_date = EXCLUDED.created_date,
            updated_date = EXCLUDED.updated_date;
    """

    await conn.executemany(query, response_data)  # Выполняет пакетную вставку данных


async def fetch_and_insert():
    """
    Основная функция для получения данных о сотрудниках и их вставки в базу данных.
    """
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session:
        conn = await asyncpg.connect(**DB_CONFIG)  # Подключение к базе данных.
        print('Успешное подключение к БД')
        try:
            tasks = []
            page = 1

            while True:
                print('-', page)
                url = f'{OMNI_URL}/staff.json?page={page}&limit=100'  # Формируем ссылку для API запроса
                task = fetch_response(session, url)  # Формируем задачу для заданной страницы
                tasks.append(task)  # Добавляем задачу для парсинга.

                if len(tasks) >= 2:  # Обработка 2 (всех) страниц одновременно
                    responses = await asyncio.gather(*tasks)  # Тянем результаты задач в responses
                    tasks = []  # Очистка задач

                    for response in responses:
                        if response is None or len(response) <= 1:
                            print('Все данные обработаны.')
                            return

                        response_data = fetch_data(response, staff_data_extractor, 'staff')  # Извлечение данных

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
    """Запускает асинхронную функцию для получения и вставки данных о сотрудниках."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_insert())


# Создаем DAG
with DAG(
        'dag_parse_omni_staff',
        default_args=DAG_CONFIG,
        schedule_interval=None,  # Не запускать автоматически
        catchup=False,
) as dag:

    fetch_staff_task = PythonOperator(
        task_id='parse_omni_staff',
        python_callable=run_async_func,
    )

    fetch_staff_task
