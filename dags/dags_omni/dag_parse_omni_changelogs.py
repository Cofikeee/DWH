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
                    OFFSET_VALUE, OFFSET_SKEW, GLOBAL_PAUSE, DELAY_BETWEEN_REQUESTS)
# Запросы к БД
from queries import queries_select as qs, queries_insert as qi
# Функции
from functions import functions_general as fg, function_logging as fl


async def process_case(case_id, session, pool):
    """
    Обрабатывает один case_id.
    """
    await asyncio.sleep(DELAY_BETWEEN_REQUESTS)  # Задержка 100 мс
    await GLOBAL_PAUSE.wait()  # Если API дал 429, ждём
    processor = OmniChangelogProcessor(case_id, session, pool)
    return await processor.process_case()


async def fetch_and_process_changelogs():
    """
    Основной процесс обработки сообщений.
    Логика работы:
    1. Извлекает case_id из базы данных по временному диапазону.
    2. Обрабатывает каждый case_id параллельно с помощью asyncio.gather.
    3. Собирает данные в батч и выполняет массовую вставку.
    """
    # Инициализация логгера
    logger = fl.setup_logger('dag_parse_omni_changelogs')
    logger.info('-----------------------------------------')
    logger.info('Начало работы DAG dag_parse_omni_changelogs')

    offset_value = OFFSET_VALUE
    offset_skew = OFFSET_SKEW

    # Создаем асинхронные сессии для HTTP-запросов и подключения к базе данных
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        async with pool.acquire() as conn:
            from_time = await qs.select_max_value(conn, 'dwh_omni', 'dim_omni_changelog', 'created_date')
        to_time = fg.next_day(from_time)

        try:
            while True:
                await GLOBAL_PAUSE.wait()  # Ждём, если API дал 429
                await asyncio.sleep(2)

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
                        await qi.insert_omni_changelog(conn, batch_data)
                    logger.info(f'Вставлен батч данных размером {len(batch_data)}.')

                # Увеличиваем offset_value
                offset_value += offset_skew

        finally:
            # Закрываем пул соединений с БД
            await pool.close()


if __name__ == "__main__":
    asyncio.run(fetch_and_process_changelogs())


def run_async():
    """
    Запускает основную асинхронную функцию fetch_and_process_changelogs.
    """
    asyncio.run(fetch_and_process_changelogs())


# Создание DAG для Airflow
with DAG(
    'dag_parse_omni_changelogs',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['omni']
) as dag:
    fetch_changelogs_task = PythonOperator(
        task_id='parse_omni_changelogs',
        python_callable=run_async,
    )

    fetch_changelogs_task
