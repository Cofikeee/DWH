from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import asyncio
import aiohttp
import asyncpg

from dateutil.relativedelta import relativedelta

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import (
    fix_null, fix_datetime, fix_bool, fix_int, blacklist_check,
    get_today, log_etl_statistics, fetch_response
)


def process_case(record):
    """Обработка данных кейса с учетом фильтрации по блэк-листу."""
    if blacklist_check(record):
        return None  # Игнорируем записи из блэк-листа

    return (
        record.get('case_id'),
        record.get('case_number'),
        fix_null(record.get('subject')),
        record.get('user_id'),
        record.get('staff_id'),
        record.get('group_id'),
        fix_int(record.get('custom_fields', {}).get('cf_6998')),  # category_id
        fix_int(record.get('custom_fields', {}).get('cf_4605')),  # block_id
        fix_int(record.get('custom_fields', {}).get('cf_8497')),  # topic_id
        fix_int(record.get('custom_fields', {}).get('cf_9129')),  # task_id
        record.get('status'),
        record.get('priority'),
        fix_null(record.get('rating')),
        fix_null(record.get('custom_fields', {}).get('cf_9112')),  # device
        fix_bool(record.get('custom_fields', {}).get('cf_8522')),  # incident
        fix_bool(record.get('custom_fields', {}).get('cf_5916')),  # mass_issue
        fix_datetime(record.get('created_at')),
        fix_datetime(record.get('updated_at')),
        fix_datetime(record.get('closed_at')),
        record.get('labels') or []
    )


async def insert_case_labels(labels_data, conn):
    """Массовая вставка меток кейсов в БД."""
    query = """
        INSERT INTO bridge_case_label (case_id, label_id)
        VALUES ($1, $2)
        ON CONFLICT (case_id, label_id) DO NOTHING;
    """
    values = [(case_id, label) for case_id, labels in labels_data for label in labels]
    if values:
        await conn.executemany(query, values)


async def insert_cases(cases, conn):
    """Массовая вставка обращений в БД."""
    query = """
        INSERT INTO fact_omni_case(
            case_id, case_number, subject, omni_user_id, staff_id, group_id, 
            category_id, block_id, topic_id, task_id, status, priority, rating, device, 
            incident, mass_issue, created_date, updated_date, closed_date
        ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) 
        ON CONFLICT (case_id) DO UPDATE
        SET subject = EXCLUDED.subject, staff_id = EXCLUDED.staff_id,
            group_id = EXCLUDED.group_id, category_id = EXCLUDED.category_id,
            block_id = EXCLUDED.block_id, topic_id = EXCLUDED.topic_id,
            task_id = EXCLUDED.task_id, status = EXCLUDED.status,
            priority = EXCLUDED.priority, device = EXCLUDED.device,
            incident = EXCLUDED.incident, mass_issue = EXCLUDED.mass_issue,
            updated_date = EXCLUDED.updated_date, closed_date = EXCLUDED.closed_date;
    """
    if cases:
        await conn.executemany(query, cases)


async def fetch_and_insert():
    """Основной цикл для парсинга и вставки данных в БД."""
    page = 1
    batch_size = 5  # Количество страниц для пакетной обработки

    from_time = datetime.strptime('2021-11-01T00:00:00', '%Y-%m-%dT%H:%M:%S')  # select_max_ts('fact_omni_case')
    to_time = from_time + relativedelta(days=1)

    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:

        async with pool.acquire() as conn:
            while True:
                batch_cases, batch_labels = [], []

                for i in range(batch_size):
                    url = f"{OMNI_URL}/cases.json?from_updated_time={from_time}&to_updated_time={to_time}&page={page}&limit=100&sort=updated_at_asc"

                    print(f'?from_updated_time={from_time}&to_updated_time={to_time}&page={page}&limit=100&sort=updated_at_asc')
                    data = await fetch_response(session, url)

                    if not data or len(data) <= 1:
                        break  # Если данных больше нет, выходим

                    period_total = data.get("total_count", 0)
                    period_pages = (period_total + 99) // 100
                    if period_pages > 500:
                        url = f"{OMNI_URL}/cases.json?from_updated_time={from_time}&to_updated_time={to_time}&page=500&limit=100&sort=updated_at_asc"
                        last_page_data = await fetch_response(session, url)
                        last_page_record = last_page_data["99"]["case"]["updated_at"]
                        to_time = fix_datetime(last_page_record) - relativedelta(seconds=1)
                        continue

                    cases_data = [process_case(item["case"]) for item in data.values() if isinstance(item, dict) and "case" in item]
                    cases_data = [case for case in cases_data if case]
                    blacklisted_cases = sum(1 for item in data.values() if isinstance(item, dict) and "case" in item and blacklist_check(item["case"]))

                    await log_etl_statistics(
                        conn, "fact_omni_case", from_time, to_time,
                        page, len(cases_data), blacklisted_cases, period_total
                    )


                    batch_cases.extend([case[:-1] for case in cases_data])  # Убираем метки
                    batch_labels.extend([(case[0], case[-1]) for case in cases_data])  # Добавляем метки
                    if page == period_pages:
                        page += 1
                        break
                    page += 1

                if period_pages > 500:
                    continue

                # Вставляем собранные данные за батч
                if batch_cases:
                    await insert_cases(batch_cases, conn)
                    await insert_case_labels(batch_labels, conn)

                if page > period_pages:
                    if to_time >= get_today():
                        print("Дошли до сегодняшнего дня.")
                        return
                    from_time = to_time
                    to_time = (to_time + relativedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                    page = 1


def run_async():
    asyncio.run(fetch_and_insert())


# Создаем DAG
with DAG(
        'dag_parse_omni_cases',
        default_args=DAG_CONFIG,
        schedule_interval=None,
        catchup=False,
) as dag:
    fetch_cases_task = PythonOperator(
        task_id='parse_omni_cases',
        python_callable=run_async,
    )

    fetch_cases_task
