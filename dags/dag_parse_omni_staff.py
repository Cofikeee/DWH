from airflow import DAG
from airflow.operators.python import PythonOperator

import aiohttp
import asyncio
import asyncpg

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import functions_general as fg, functions_data as fd
from queries import queries_log as ql, queries_insert as qi


def staff_data_extractor(record):
    # Извлечение и предобработка данных
    return (
        record.get('staff_id'),
        fd.fix_null(record.get('staff_full_name')),
        fd.fix_null(record.get('staff_email')),
        record.get('active'),
        fd.fix_datetime(record.get('created_at')),
        fd.fix_datetime(record.get('updated_at'))
    )


async def fetch_and_process_staff():
    """
    Основная функция для получения данных о сотрудниках и их вставки в базу данных.
    """
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        async with pool.acquire() as conn:
            total_count = await fg.get_snapshot(session, 'staff')
            try:
                tasks = []
                page = 1

                while True:
                    print('-', page)
                    url = f'{OMNI_URL}/staff.json?page={page}&limit=100'  # Формируем ссылку для API запроса
                    task = fg.fetch_response(session, url)  # Формируем задачу для заданной страницы
                    tasks.append(task)  # Добавляем задачу для парсинга.

                    if len(tasks) >= 2:  # Обработка 2 (всех) страниц одновременно
                        responses = await asyncio.gather(*tasks)  # Тянем результаты задач в responses
                        tasks = []  # Очистка задач

                        for response in responses:
                            if response is None or len(response) <= 1:
                                print('Все данные обработаны.')
                                return

                            response_data = fg.fetch_data(response, staff_data_extractor, 'staff')  # Извлечение данных

                            await qi.insert_staff(response_data, conn)  # Вставка данных в базу.

                    page += 1  # Переходим к следующей странице.

            finally:
                if tasks:  # Обрабатываем любые оставшиеся задачи.
                    responses = await asyncio.gather(*tasks)
                    for response in responses:
                        if response is None or len(response) <= 1:
                            print('Все данные обработаны.')
                            return

                await ql.log_etl_catalogues(conn, 'dim_omni_staff', total_count)
                await conn.close()  # Закрываем соединение с базой данных.
                print('Закрыто соединение с БД.')



def run_async_func():
    """Запускает асинхронную функцию для получения и вставки данных о сотрудниках."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_process_staff())


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
