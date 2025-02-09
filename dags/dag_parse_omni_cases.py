import asyncio
import aiohttp
import asyncpg
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from classes.omni_case import OmniCase

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import functions_data as fd, functions_general as fg
from queries import queries_log as ql, queries_insert as qi, queries_select as qs


async def fetch_and_process_cases(from_time=None, backfill=False):
    page = 1
    batch_size = 5  # Размер пакета страниц
    if not from_time:
        from_time = qs.select_max_ts('fact_omni_case')
    to_time = (from_time + relativedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:

        async with pool.acquire() as conn:
            while True:

                if from_time >= fg.get_today():
                    print("Дошли до сегодняшнего дня.")
                    return

                batch_cases, batch_labels = [], []
                for i in range(batch_size):
                    url = f"{OMNI_URL}/cases.json?from_updated_time={from_time}&to_updated_time={to_time}&page={page}&limit=100&sort=updated_at_asc"
                    print(url)
                    data = await fg.fetch_response(session, url)
                    if not data or len(data) <= 1:
                        if from_time == qs.select_max_ts('fact_omni_case'):
                            print(f'Нет данных за период {from_time} - {to_time}')
                            return
                        raise Exception('Получили неожиданный результат - пустую страницу.')

                    if page == 1:
                        period_total = data.get("total_count", 0)
                        period_pages = (period_total + 99) // 100

                    if period_pages > 500:
                        last_page_url = f"{OMNI_URL}/cases.json?from_updated_time={from_time}&to_updated_time={to_time}&page=500&limit=100&sort=updated_at_asc"
                        last_page_data = await fg.fetch_response(session, last_page_url)
                        last_page_record = last_page_data["99"]["case"]["updated_at"]
                        to_time = fd.fix_datetime(last_page_record) - relativedelta(seconds=1)
                        continue

                    cases_data = []
                    blacklisted_cases = 0

                    for item in data.values():
                        if isinstance(item, dict) and "case" in item:

                            case = OmniCase(item["case"])
                            processed_case = (case.case_properties())
                            if processed_case:
                                cases_data.append(processed_case)
                            else:
                                blacklisted_cases += 1

                    batch_cases.extend([case[:-1] for case in cases_data])  # Убираем метки
                    batch_labels.extend([(case[0], case[-1]) for case in cases_data])  # Добавляем метки

                    await ql.log_etl_cases(
                        conn, from_time, to_time, page, len(cases_data),
                        blacklisted_cases, period_total
                    )

                    if page == period_pages:
                        page += 1
                        break

                    page += 1

                if period_pages > 500:
                    continue

                if batch_cases:
                    await qi.insert_cases(conn, batch_cases)
                    await qi.insert_case_labels(conn, batch_labels)

                if page > period_pages:
                    if backfill:
                        print(f'Забэкфилили пропуски {from_time} - {to_time}')
                        return
                    print(f'Собраны данные за период {from_time} - {to_time}')
                    from_time = to_time
                    to_time = (from_time + relativedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                    page = 1


def run_async():
    asyncio.run(fetch_and_process_cases())


with DAG(
    'dag_parse_omni_cases',
    default_args=DAG_CONFIG,
    catchup=False,
    schedule_interval=None,
) as dag:
    fetch_cases_task = PythonOperator(
        task_id='parse_omni_cases',
        python_callable=run_async,
    )
    fetch_cases_task