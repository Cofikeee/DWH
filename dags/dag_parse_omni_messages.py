from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
import aiohttp
import asyncpg
from datetime import datetime
from dateutil.relativedelta import relativedelta

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import (
    fix_datetime, fix_message_type, fix_message_text,
    log_etl_messages, fix_message_attachment, get_today
)
from classes import RateLimiter

QUEUE_SIZE = 10
WORKERS = 10
DELAY_BETWEEN_REQUESTS = 0.05

PAUSE_EVENT = asyncio.Event()  # Глобальный объект для паузы воркеров


async def process_case(case_id, session, pool):
    """Обработка сообщений одного case_id."""
    page = 1
    all_messages = []

    while True:
        await PAUSE_EVENT.wait()  # Ждём, если стоит глобальная пауза

        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
        page_url = f"{OMNI_URL}/cases/{case_id}/messages.json?page={page}"
   #     print(f"Запрос к API: {page_url}")

        try:
            data = await fetch_response(session, page_url)  # Запрос к API
        except Exception as e:
            print(f"Ошибка при запросе {page_url}: {e}")
            return

        if not data:
            break

        period_total = data.get("total_count", 0)
        period_pages = (period_total + 99) // 100

        messages_data = [
            (
                item["message"].get("message_id"), case_id, item["message"].get("user_id"),
                item["message"].get("staff_id"), fix_message_type(item["message"]),
                fix_message_attachment(item["message"]), fix_message_text(item["message"]),
                fix_datetime(item["message"].get("created_at"))
            )
            for item in data.values()
            if isinstance(item, dict) and "message" in item
        ]
        all_messages.extend(messages_data)

        async with pool.acquire() as conn:
            await log_etl_messages(conn, case_id, page, len(messages_data), period_total)

        if page == period_pages:
            break
        page += 1

    if all_messages:
        async with pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO dim_omni_message_test (
                    message_id, case_id, omni_user_id, staff_id, 
                    message_type, attachment_type, message_text, created_date
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (message_id, case_id) DO NOTHING;
            """, all_messages)


async def worker(queue, session, pool):
    """Воркер, обрабатывающий case_id по очереди."""
    while True:
        case_id = await queue.get()
        if case_id is None:
            break

        await PAUSE_EVENT.wait()  # Если API дал 429, ждём

        await process_case(case_id, session, pool)
        queue.task_done()


async def fetch_and_insert_messages():
    """Основной процесс обработки сообщений."""
    from_time = datetime.strptime('2024-03-08 00:00:00', '%Y-%m-%d %H:%M:%S')
    to_time = from_time + relativedelta(days=1)

    offset_value = 0
    offset_skew = 15
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)

    PAUSE_EVENT.set()  # Включаем обработку сразу

    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:

        workers = [asyncio.create_task(worker(queue, session, pool)) for _ in range(WORKERS)]

        while True:
            await PAUSE_EVENT.wait()  # Ждём, если API дал 429
            await asyncio.sleep(1.6)
            async with pool.acquire() as conn:
                case_ids = await conn.fetch("""
                    SELECT case_id FROM fact_omni_case
                    WHERE updated_date >= $1
                      AND updated_date < $2
                    ORDER BY updated_date, created_date
                    LIMIT $3 OFFSET $4
                """, from_time, to_time, offset_skew, offset_value)
                case_ids = [row['case_id'] for row in case_ids]

            if not case_ids:
                print(f'Данные за период {from_time} - {to_time} собраны. Offset = {offset_value}')
                if to_time >= get_today():
                    break
                from_time = to_time
                to_time = (to_time + relativedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                offset_value = 0
                continue

            for case_id in case_ids:
                await queue.put(case_id)

            offset_value += offset_skew

        for _ in range(WORKERS):
            await queue.put(None)

        await queue.join()
        await asyncio.gather(*workers)


async def fetch_response(session, url, max_retries=5):
    """Асинхронное получение JSON-данных с повторными попытками при ошибке 429."""
    limiter = RateLimiter(min_interval=DELAY_BETWEEN_REQUESTS)

    for attempt in range(max_retries):
        await limiter.wait()

        async with session.get(url) as response:
            if response.status == 429:
                print(f"{datetime.now()} - Получен 429, пауза на 60 сек.")
                PAUSE_EVENT.clear()  # Останавливаем все воркеры
                await asyncio.sleep(60)  # Ждём минуту
                PAUSE_EVENT.set()  # Возобновляем работу
                continue

            response.raise_for_status()
            return await response.json()

    raise Exception(f"Превышено количество попыток для URL: {url}")


def run_async():
    asyncio.run(fetch_and_insert_messages())


with DAG(
        'dag_parse_omni_messages',
        default_args=DAG_CONFIG,
        schedule_interval=None,
        catchup=False,
) as dag:
    fetch_messages_task = PythonOperator(
        task_id='parse_omni_messages',
        python_callable=run_async,
    )
    fetch_messages_task
