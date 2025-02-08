import asyncpg
import asyncio
from airflow.operators.python import PythonOperator
from dag_parse_omni_cases import fetch_and_process_cases
import aiohttp
from airflow import DAG
from datetime import datetime
from dateutil.relativedelta import relativedelta
from queries import queries_select as qs
from functions import functions_general as fg
from config import DB_CONFIG, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG, WORKERS_CONFIG, GLOBAL_PAUSE
from classes.ratelimiter import RateLimiter
from classes.omni_message_processor import OmniMessageProcessor
WORKERS = 5
OFFSET_VALUE = 0
OFFSET_SKEW = 20
QUEUE_SIZE = 10


async def worker(queue, session, pool):
    """Воркер, обрабатывающий case_id по очереди."""
    while True:
        case_id = await queue.get()
        if case_id is None:
            break

        await GLOBAL_PAUSE.wait()  # Если API дал 429, ждём

        await OmniMessageProcessor(case_id, session, pool).process_case()
        queue.task_done()


async def fetch_and_process_missing_messages():
    """Основной процесс обработки сообщений."""
    case_ids = []
    offset_value = OFFSET_VALUE
    offset_skew = OFFSET_SKEW
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)

    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:

        workers = [asyncio.create_task(worker(queue, session, pool)) for _ in range(WORKERS)]
        try:
            while True:
                await GLOBAL_PAUSE.wait()  # Ждём, если API дал 429
                await RateLimiter(min_interval=1).wait()  # Ждём между батчами запросов
                async with pool.acquire() as conn:
                    case_ids = await qs.select_missing_case_ids(conn, offset_skew)

                if not case_ids:
                    offset_value = 0
                    async with pool.acquire() as conn:
                        case_ids = await qs.select_missing_case_ids(conn, offset_skew)
                    if not case_ids:
                        print('Успешная проверка сообщений')
                        break
                    else:
                        raise Exception('В сообщениях не удалось забэкфилить все данные')

                for case_id in case_ids:
                    await queue.put(case_id)

                offset_value += offset_skew

            for _ in range(WORKERS):
                await queue.put(None)


        finally:
            # Signal workers to exit
            for _ in range(WORKERS):
                await queue.put(None)

            # Wait for all workers to finish
            await asyncio.gather(*workers)

            # Close the connection pool
            await pool.close()


def run_async():
    asyncio.run(fetch_and_process_missing_messages())


with DAG(
    'dag_validate_omni_messages',
    default_args=DAG_CONFIG,
    catchup=False,
    schedule_interval=None,
) as dag:
    validate_messages = PythonOperator(
        task_id='dag_validate_omni_messages',
        python_callable=run_async,
    )
    validate_messages