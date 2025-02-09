from airflow import DAG
from airflow.operators.python import PythonOperator
import aiohttp
import asyncio
import asyncpg
from dateutil.relativedelta import relativedelta
from datetime import datetime

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from classes.omni_user import OmniUser

from queries import queries_select as qs, queries_log as ql, queries_insert as qi
from functions import functions_general as fg, functions_data as fd, function_logging as fl

logger = fl.setup_logger('dag_parse_omni_users')


async def fetch_and_process_users(from_time=None, backfill=False):
    page = 1
    batch_size = 5  # Размер пакета страниц
    if not from_time:
        from_time = qs.select_max_ts('dim_omni_user')
    to_time = (from_time + relativedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:

        async with pool.acquire() as conn:
            while True:

                if from_time >= fg.get_today():
                    logger.info("Дошли до сегодняшнего дня.")
                    return

                batch_users = []

                for i in range(batch_size):
                    url = f"{OMNI_URL}/users.json?from_updated_time={from_time}&to_updated_time={to_time}&page={page}&limit=100"
                    data = await fg.fetch_response(session, url)
                    if not data or len(data) <= 1:
                        if from_time == qs.select_max_ts('dim_omni_user'):
                            logger.info(f'Нет данных за период {from_time} - {to_time}, страница {page}')
                            return
                        logger.info(f'Нет данных за период {from_time} - {to_time}, страница {page}')
                        from_time = to_time
                        to_time = (from_time + relativedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                        page = 1
                        continue
                        #raise Exception('Получили неожиданный результат - пустую страницу.')

                    if page == 1:
                        period_total = data.get("total_count", 0)
                        period_pages = (period_total + 99) // 100

                    if period_pages > 500:
                        last_page_url = f"{OMNI_URL}/users.json?from_updated_time={from_time}&to_updated_time={to_time}&page=500&limit=100"
                        last_page_data = await fg.fetch_response(session, last_page_url)
                        last_page_record = last_page_data["99"]["user"]["updated_at"]
                        to_time = fd.fix_datetime(last_page_record) - relativedelta(seconds=1)
                        continue

                    users_data = []

                    for item in data.values():
                        if isinstance(item, dict) and "user" in item:

                            user = OmniUser(item["user"])
                            processed_user = (user.user_properties())
                            if processed_user:
                                users_data.append(processed_user)

                    batch_users.extend([user for user in users_data])  # Убираем метки

                    await ql.log_etl_users(
                        conn, from_time, to_time, page, len(users_data), period_total
                    )

                    if page == period_pages:
                        page += 1
                        break

                    page += 1

                if period_pages > 500:
                    continue

                if batch_users:
                    await qi.insert_users(conn, batch_users)

                if page > period_pages:
                    if backfill:
                        logger.info(f'Забэкфилили пропуски {from_time} - {to_time}')
                        return
                    logger.info(f'Собраны данные за период {from_time} - {to_time}')
                    from_time = to_time
                    to_time = (from_time + relativedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                    page = 1


def run_async():
    asyncio.run(fetch_and_process_users())


# Создаем DAG
with DAG(
        'dag_parse_omni_users',
        default_args=DAG_CONFIG,
        schedule_interval=None,  # Не запускать автоматически
        catchup=False,
) as dag:

    fetch_users_task = PythonOperator(
        task_id='parse_omni_users',
        python_callable=run_async,
    )

    fetch_users_task
