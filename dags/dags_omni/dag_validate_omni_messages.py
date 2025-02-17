# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import aiohttp
import asyncio
import asyncpg
# Конфиг
from config import (OMNI_DB_CONFIG, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG,
                    GLOBAL_PAUSE, WORKERS, OFFSET_SKEW, QUEUE_SIZE)
# Классы
from classes.ratelimiter import RateLimiter
from classes.omni_message_processor import OmniMessageProcessor
# Запросы к БД
from queries import queries_select as qs
# Функции
from functions import function_logging as fl


async def worker(queue, session, pool):
    """
    Воркер, обрабатывающий case_id по очереди через OmniMessageProcessor.
    """
    while True:
        case_id = await queue.get()
        if case_id is None:
            break
        await GLOBAL_PAUSE.wait()  # Если API вернул 429, ждём 1 минуту
        await OmniMessageProcessor(case_id, session, pool).process_case()
        queue.task_done()


async def fetch_and_process_missing_messages():
    """
    Основной процесс обработки сообщений.
    Логика работы:
    1. Извлекает case_id сообщений, которые не прошли валидацию, из логов в БД.
    2. Обрабатывает каждый case_id через воркеры.
    3. Логирует процесс выполнения.
    """
    # Инициализация логгера
    logger = fl.setup_logger('dag_validate_omni_messages')
    logger.info('--------------------------------------------')
    logger.info('Начало работы DAG dag_validate_omni_messages')

    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    last_page = False

    # Создаем асинхронные сессии для HTTP-запросов и подключения к БД
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**OMNI_DB_CONFIG, min_size=5, max_size=20) as pool:
        # Создаем воркеров
        workers = [asyncio.create_task(worker(queue, session, pool)) for _ in range(WORKERS)]
        try:
            while True:
                await GLOBAL_PAUSE.wait()  # Если API вернул 429, ждём 1 минуту
                await RateLimiter(min_interval=1).wait()  # Ждём 1 секунду между батчами запросов

                # Получаем соединение с БД
                async with pool.acquire() as conn:
                    logger.info('Начало валидации данных сообщений.')
                    case_ids = await qs.select_missing_case_ids(conn, OFFSET_SKEW)

                # Проверяем, есть ли данные для обработки
                if last_page:
                    if not case_ids:
                        logger.info('Успешная проверка сообщений.')
                        break
                    logger.error('В сообщениях не удалось забэкфилить все данные.')
                    raise Exception('В сообщениях не удалось забэкфилить все данные.')

                # Добавляем case_id в очередь
                if case_ids:
                    logger.info(f'Получены обращения для валидации: {case_ids}.')
                    for case_id in case_ids:
                        await queue.put(case_id)

                if len(case_ids) < OFFSET_SKEW:
                    last_page = True

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
    Запускает основную асинхронную функцию fetch_and_process_missing_messages.
    """
    asyncio.run(fetch_and_process_missing_messages())


# Создание DAG для Airflow
with DAG(
    'dag_validate_omni_messages',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['omni']
) as dag:
    validate_messages = PythonOperator(
        task_id='dag_validate_omni_messages',
        python_callable=run_async,
    )

    validate_messages
