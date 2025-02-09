from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import aiohttp
import asyncio
import asyncpg
from dateutil.relativedelta import relativedelta
from queries import queries_select as qs, queries_log as ql
from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import (functions_general as fg,
                       functions_data as fd)


def user_data_extractor(record):
    """ Извлечение и предобработка данных из ответа API """
    return (
        record.get('user_id'),
        record.get('type'),  # channel_type
        record.get(fd.fix_channel(record.get('type'))),  # channel_value
        record.get('company_name'),
        fd.fix_null(record.get('custom_fields', {}).get('cf_6439')),  # user_id
        fd.fix_bool(record.get('custom_fields', {}).get('cf_7017')),
        fd.get_user_roles_array(record),  # roles array
        fd.fix_datetime(record.get('created_at')),
        fd.fix_datetime(record.get('updated_at'))
    )



async def insert_into_db(conn, response_data):
    """ Вставка данных в базу """
    query = """
        INSERT INTO dim_omni_user(
            omni_user_id,
            omni_channel_type,
            omni_channel_value,
            company_id,
            user_id,
            confirmed,
            omni_roles,
            linked_users,
            created_date,
            updated_date
        ) 
        VALUES($1, $2, $3, (SELECT min(company_id) FROM dim_omni_company WHERE company_name = $4), $5, $6, $7, null, $8, $9) 
        ON CONFLICT (omni_user_id) DO UPDATE
        SET omni_channel_type = EXCLUDED.omni_channel_type,
            omni_channel_value = EXCLUDED.omni_channel_value,
            company_id = EXCLUDED.company_id,
            user_id = EXCLUDED.user_id,
            confirmed = EXCLUDED.confirmed,
            omni_roles = EXCLUDED.omni_roles,
            created_date = EXCLUDED.created_date,
            updated_date = EXCLUDED.updated_date;
    """

    await conn.executemany(query, response_data)


async def fetch_and_insert():
    """
    Основная функция для получения данных о пользователях и их вставки в базу данных.
    """
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        async with pool.acquire() as conn:
            total_count = await fg.get_snapshot(session, 'users')
            try:
                tasks, log_tasks = [], []
                page = 1
                response_page = 1
                from_time = datetime.strptime('2020-11-03 00:00:00', '%Y-%m-%d %H:%M:%S') #  qs.select_max_ts('dim_omni_user')
                to_time = (from_time + relativedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

                while True:

                    if from_time >= fg.get_today():
                        print("Дошли до сегодняшнего дня.")
                        return

                    print(from_time, '-', to_time, '-', page)
                    url = f'{OMNI_URL}/users.json?from_updated_time={from_time}&to_updated_time={to_time}&page={page}&limit=100'
                    data = await fg.fetch_response(session, url)
                    if not data or len(data) <= 1:
                        print(f'Собраны данные за период {from_time} - {to_time}')
                        from_time = to_time
                        to_time = (from_time + relativedelta(days=1)).replace(hour=0, minute=0, second=0,
                                                                              microsecond=0)
                        page = 1
                        response_page = 1
                        continue

                    if page == 1:
                        period_total = data.get("total_count", 0)
                        period_pages = (period_total + 99) // 100

                    task = fg.fetch_response(session, url)
                    tasks.append(task)  # Добавляем задачу для парсинга.

                    if len(tasks) >= 5:  # Обработка 5 страниц одновременно
                        responses = await asyncio.gather(*tasks)

                        tasks, log_tasks = [], []

                        for response in responses:

                            user_list = fg.fetch_data(response, user_data_extractor, 'user')

                            await ql.log_etl_users(conn, from_time, to_time, response_page, len(user_list), period_total)
                            await insert_into_db(conn, user_list)

                            response_page += 1


                    page += 1  # Переходим к следующей странице.

            finally:
                if tasks:  # Обрабатываем любые оставшиеся задачи.
                    responses = await asyncio.gather(*tasks)
                    for response in responses:
                        if response is None or len(response) <= 1:
                            print('Все данные обработаны.')
                            return

                await ql.log_etl_catalogues(conn, 'dim_omni_user', total_count)
                await conn.close()  # Закрываем соединение с базой данных.
                print('Закрыто соединение с БД.')



def run_async_func():
    """Запускает асинхронную функцию для получения и вставки данных о пользователях."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_insert())
