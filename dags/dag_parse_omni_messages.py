# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import asyncio
import aiohttp
import asyncpg
# Конфиг
from config import (DB_CONFIG, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG,
                    WORKERS, OFFSET_VALUE, OFFSET_SKEW, QUEUE_SIZE, GLOBAL_PAUSE)
# Классы
from classes.ratelimiter import RateLimiter
from classes.omni_message_processor import OmniMessageProcessor
# Запросы к БД
from queries import queries_select as qs
# Функции
from functions import functions_general as fg, function_logging as fl


async def worker(queue, session, pool):
    """
    Воркер, обрабатывающий case_id по очереди через OmniMessageProcessor.
    """
    while True:
        case_id = await queue.get()
        if case_id is None:
            break
        await GLOBAL_PAUSE.wait()  # Если API дал 429, ждём
        await OmniMessageProcessor(case_id, session, pool).process_case()
        queue.task_done()


async def fetch_and_process_messages():
    """
    Основной процесс обработки сообщений.
    Логика работы:
    1. Извлекает case_id из базы данных по временному диапазону.
    2. Обрабатывает каждый case_id через воркеры.
    3. Логирует процесс выполнения.
    """
    # Инициализация логгера
    logger = fl.setup_logger('dag_parse_omni_messages')
    logger.info('-----------------------------------------')
    logger.info('Начало работы DAG dag_parse_omni_messages')

    from_time = qs.select_max_ts('dim_omni_message')
    to_time = fg.next_day(from_time)
    offset_value = OFFSET_VALUE
    offset_skew = OFFSET_SKEW
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)

    # Создаем асинхронные сессии для HTTP-запросов и подключения к базе данных
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        # Создаем воркеров
        workers = [asyncio.create_task(worker(queue, session, pool)) for _ in range(WORKERS)]
        try:
            while True:
                await GLOBAL_PAUSE.wait()  # Ждём, если API дал 429
                await RateLimiter(min_interval=1).wait()  # Ждём между батчами запросов
                async with pool.acquire() as conn:
                    case_ids = await qs.select_case_ids(conn, from_time, to_time, offset_skew, offset_value)

                # Проверяем, есть ли данные для обработки
                if not case_ids:
                    logger.info(f'Данные за период {from_time} - {to_time} собраны. Offset = {offset_value}.')
                    if to_time >= fg.get_today():
                        logger.info('Все данные собраны.')
                        return
                    from_time = to_time
                    to_time = fg.next_day(from_time)
                    offset_value = 0
                    continue

                # Добавляем case_id в очередь
                for case_id in case_ids:
                    await queue.put(case_id)
                offset_value += offset_skew

            # Завершаем работу воркеров
            for _ in range(WORKERS):
                await queue.put(None)

        finally:
            # Сигнализируем воркерам о завершении
            for _ in range(WORKERS):
                await queue.put(None)
            # Ожидаем завершения всех воркеров
            await asyncio.gather(*workers)
            # Закрываем пул соединений с БД
            await pool.close()


def run_async():
    """
    Запускает основную асинхронную функцию fetch_and_process_messages.
    """
    asyncio.run(fetch_and_process_messages())


# Создание DAG для Airflow
with DAG(
    'dag_parse_omni_messages',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
) as dag:
    fetch_messages_task = PythonOperator(
        task_id='parse_omni_messages',
        python_callable=run_async,
    )

    fetch_messages_task
