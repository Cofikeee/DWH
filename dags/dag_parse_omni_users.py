from airflow import DAG
from airflow.operators.python import PythonOperator

import aiohttp
import asyncio
import asyncpg
from dateutil.relativedelta import relativedelta
from datetime import datetime

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import (
    fix_null, fix_datetime, select_max_ts, fix_channel, fix_bool,
    fix_int, get_today, fetch_data, fetch_response, get_user_roles_array
)

# Ограничение одновременных запросов (чтобы не перегружать API)
SEMAPHORE_LIMIT = 5


async def fetch_user_details(session, user_id, semaphore):
    """ Асинхронно получает подробную информацию по user_id """
    async with semaphore:
        user_url = f'{OMNI_URL}/users/{user_id}.json'
        try:
            async with session.get(user_url) as response:
                if response.status == 200:
                    user_data = await response.json()
                    return user_data
                else:
                    print(f"Ошибка при получении данных пользователя {user_id}: {response.status}")
                    return None
        except Exception as e:
            print(f"Ошибка при запросе {user_url}: {e}")
            return None


def user_data_extractor(record):
    """ Извлечение и предобработка данных из ответа API """
    return (
        record.get('user_id'),
        record.get('type'),  # channel_type
        record.get(fix_channel(record.get('type'))),  # channel_value
        record.get('company_name'),
        fix_null(record.get('custom_fields', {}).get('cf_6439')),  # user_id
        fix_bool(record.get('custom_fields', {}).get('cf_7017')),
        get_user_roles_array(record),  # roles array
        fix_int(record.get('linked_users')),  # linked users array
        fix_datetime(record.get('created_at')),
        fix_datetime(record.get('updated_at'))
    )


async def insert_into_db(response_data, conn):
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
        VALUES($1, $2, $3, (SELECT min(company_id) FROM dim_omni_company WHERE company_name = $4), $5, $6, $7, $8, $9, $10) 
        ON CONFLICT (omni_user_id) DO UPDATE
        SET omni_channel_type = EXCLUDED.omni_channel_type,
            omni_channel_value = EXCLUDED.omni_channel_value,
            company_id = EXCLUDED.company_id,
            user_id = EXCLUDED.user_id,
            confirmed = EXCLUDED.confirmed,
            omni_roles = EXCLUDED.omni_roles,
            linked_users = EXCLUDED.linked_users,
            created_date = EXCLUDED.created_date,
            updated_date = EXCLUDED.updated_date;
    """

    await conn.executemany(query, response_data)
    print(f"Успешно вставлено {len(response_data)} записей")


async def fetch_and_insert():
    """ Основная функция DAG для обработки данных """
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async with (aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session):
        conn = await asyncpg.connect(**DB_CONFIG)
        print('Успешное подключение к БД.')

        try:
            tasks = []
            page = 1
            from_time = select_max_ts('dim_omni_user')

            ##datetime.strptime('2020-12-01T00:00:00', '%Y-%m-%dT%H:%M:%S')
            to_time = from_time + relativedelta(days=1)

            while True:
                print(from_time, '-', to_time, '-', page)
                url = f'{OMNI_URL}/users.json?from_updated_time={from_time}&to_updated_time={to_time}&page={page}&limit=100'
                tasks.append(fetch_response(session, url))

                if len(tasks) >= 5:  # Пакетная обработка страниц по 5
                    responses = await asyncio.gather(*tasks)
                    tasks = []

                    user_tasks = []
                    for response in responses:
                        if response is None or len(response) <= 1:
                            from_time = to_time
                            to_time = from_time + relativedelta(days=1)
                            page = 0
                            if from_time >= get_today():
                                print('Дошли до сегодняшнего дня.')
                                return
                            break

                        user_list = fetch_data(response, user_data_extractor, 'user')

                        # Получение детальной информации по каждому пользователю
                        for user in user_list:
                            print(user)
                            user_id = user[0]
                            user_tasks.append(fetch_user_details(session, user_id, semaphore))

                    # Асинхронно получаем информацию по каждому пользователю
                    detailed_users = await asyncio.gather(*user_tasks)

                    enriched_user_list = []
                    for user, detailed_info in zip(user_list, detailed_users):
                        print('user2')
                        if detailed_info:
                            linked_users = fix_int(detailed_info.get('linked_users'))
                            print(linked_users)
                            enriched_user_list.append((*user[:6], linked_users, *user[7:]))

                    # Запись данных в БД
                    if enriched_user_list:
                        print('enriched_user_list')
                        await insert_into_db(enriched_user_list, conn)

                if page == 495:
                    from_time = select_max_ts('dim_omni_user')
                    page = 1
                    print(f'495 страниц обработано, начинаю с даты {from_time}')
                    continue

                page += 1

        finally:
            await conn.close()
            print('Закрыто соединение с БД.')


def run_async_func():
    """ Запуск асинхронной функции из Airflow DAG """
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_insert())


# Создание DAG
with DAG(
        'dag_parse_omni_users',
        default_args=DAG_CONFIG,
        schedule_interval=None,
        catchup=False,
) as dag:

    fetch_users_task = PythonOperator(
        task_id='parse_omni_users',
        python_callable=run_async_func,
    )

    fetch_users_task
