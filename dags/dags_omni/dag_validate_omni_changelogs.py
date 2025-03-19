# Airflow
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import asyncio
import aiohttp
import asyncpg
# Классы
from classes.omni_changelog_processor import OmniChangelogProcessor
# Конфиг
from config import (DB_CONFIG, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG,
                    OFFSET_SKEW, GLOBAL_PAUSE, DELAY_BETWEEN_REQUESTS)
# Запросы к БД
from queries import queries_select as qs, queries_insert as qi
# Функции
from functions import function_logging as fl


async def process_case(case_id, session, pool):
    """
    Обрабатывает один case_id.
    """
    await asyncio.sleep(DELAY_BETWEEN_REQUESTS)  # Задержка 100 мс
    await GLOBAL_PAUSE.wait()  # Если API дал 429, ждём
    processor = OmniChangelogProcessor(case_id, session, pool)
    return await processor.process_case()


async def fetch_and_process_missing_changelogs(logger=None):
    """
    Основной процесс обработки сообщений.
    Логика работы:
    1. Извлекает case_id логов, которые не прошли валидацию, из логов в БД.
    2. Обрабатывает каждый case_id параллельно с помощью asyncio.gather.
    3. Собирает данные в батч и выполняет массовую вставку.
    """
    # Инициализация логгера
    if not logger:
        logger = fl.setup_logger('dag_validate_omni_changelogs')
    logger.info('-----------------------------------------')
    logger.info('Начало работы DAG dag_validate_omni_changelogs')

    last_page = False

    # Создаем асинхронные сессии для HTTP-запросов и подключения к базе данных
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:

        try:
            while True:
                await GLOBAL_PAUSE.wait()  # Ждём, если API дал 429
                await asyncio.sleep(2)

                async with pool.acquire() as conn:
                    case_ids = await qs.select_missing_case_ids(conn, 'dim_omni_changelog')

                if last_page:
                    if not case_ids:
                        logger.info('Успешная проверка сообщений.')
                        break
                    logger.error('В сообщениях не удалось забэкфилить все данные.')
                    raise Exception('В сообщениях не удалось забэкфилить все данные.')

                # Проверяем, есть ли данные для обработки
                if case_ids:
                    # Создаем задачи для параллельной обработки case_id
                    tasks = [process_case(case_id, session, pool) for case_id in case_ids]
                    results = await asyncio.gather(*tasks)
                    # Формируем батч данных
                    batch_data = []
                    for result in results:
                        if result:
                            batch_data.extend(result)
                    # Вставка данных батчами
                    if batch_data:
                        async with pool.acquire() as conn:
                            await qi.insert_changelogs(conn, batch_data)
                        logger.info(f'Вставлен батч данных размером {len(batch_data)}.')

                if len(case_ids) < OFFSET_SKEW:
                    last_page = True


        finally:
            # Закрываем пул соединений с БД
            await pool.close()


if __name__ == "__main__":
    asyncio.run(fetch_and_process_missing_changelogs(logger=logging))


def run_async():
    """
    Запускает основную асинхронную функцию fetch_and_process_changelogs.
    """
    asyncio.run(fetch_and_process_missing_changelogs())


# Создание DAG для Airflow
with DAG(
    'dag_validate_omni_changelogs',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['omni']
) as dag:
    validate_changelogs = PythonOperator(
        task_id='validate_omni_changelogs',
        python_callable=run_async,
    )

    validate_changelogs
