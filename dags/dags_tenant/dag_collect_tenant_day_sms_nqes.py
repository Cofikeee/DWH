# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import asyncio
import asyncpg
from datetime import datetime
import pandas as pd
import io
import csv

from asyncpg.pgproto.pgproto import timedelta

# Конфиг
from config import DB_CONFIG, COLORS, COLORS_SEMAPHORES, DAG_CONFIG, FIRST_DATE
# Запросы к БД
from queries import queries_select as qs, queries_tenant_crawler as qtc
# Функции
from functions import function_logging as fl, functions_general as fg


async def insert_sms_stats_with_copy(conn, data):
    """
    Асинхронная функция для вставки данных в таблицу с использованием метода COPY.
    :param conn: Асинхронное соединение с базой данных.
    :param data: Список кортежей с данными для вставки.
    """
    # Создаем текстовый поток для записи данных
    string_data = io.StringIO()
    writer = csv.writer(string_data, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    # Записываем данные в поток
    writer.writerows(data)  # Запись всех строк сразу
    # Преобразуем текстовые данные в байты
    byte_data = string_data.getvalue().encode("utf-8")
    byte_stream = io.BytesIO(byte_data)

    # Определяем целевую таблицу и столбцы
    table_name = 'agg_tenant_day_sms_and_nqes_notifications'
    columns = ['dim_tenant_name', 'dim_tenant_host', 'dim_day_start', 'agg_cnt_nqes', 'agg_cnt_sms', 'ctl_ts_delta', 'ctl_parsed_date']

    # Выполняем вставку данных с использованием COPY
    await conn.copy_to_table(
        table_name=table_name,
        source=byte_stream,
        columns=columns,
        schema_name='dwh',
        format='csv',
        delimiter='\t'
    )


async def process_tenant(pool, tenant, semaphore, results_accumulator, from_created_date, to_created_date):
    """
    Асинхронная функция для обработки данных одного тенанта.
    :param pool: Пул подключений к базе данных.
    :param tenant: Информация о тенанте (словарь с данными).
    :param semaphore: Семафор для ограничения параллельных задач.
    :param results_accumulator: Список для накопления результатов.
    :param from_created_date: Таймстамп начала парсинга для фильтрации и логгирования.
    :param to_created_date: Таймстамп начала парсинга для фильтрации.
    """
    async with semaphore:
        db_host = tenant['db_host']
        datname = tenant['datname']
        tenant_host = tenant['tenant_host']
        tenant_name = tenant['tenant_name']
        # Получаем соединение с базой данных
        async with pool.acquire() as conn:
            # Извлекаем статистику SMS для текущего тенанта
            results = await qtc.select_sms_stats(conn, datname, db_host, from_created_date, to_created_date)
            if results:
                # Добавляем обработанные данные в аккумулятор результатов
                results_accumulator.extend([
                    (
                        tenant_name,
                        tenant_host,
                        row['start_of_day'],
                        row['cnt_nqes'],
                        row['cnt_sms'],
                        row['ts_delta'],
                        to_created_date
                    )
                    for row in results
                ])



async def process_color(pool, color, tenants, semaphore, logger, from_created_date, to_created_date):
    """
    Асинхронная функция для обработки всех тенантов одного цвета.
    :param pool: Пул подключений к базе данных.
    :param color: Цвет тенантов для обработки.
    :param tenants: Список тенантов данного цвета.
    :param semaphore: Семафор для ограничения параллельных задач.
    :param logger: Инициализированный в main logger.
    :param from_created_date: Таймстамп начала парсинга для фильтрации и логгирования.
    :param to_created_date: Таймстамп начала парсинга для фильтрации.
    """
    # Список для накопления результатов
    results_accumulator = []
    # Создаем задачи для параллельной обработки тенантов
    tasks = [asyncio.create_task(
        process_tenant(pool, tenant, semaphore, results_accumulator, from_created_date, to_created_date)
    ) for tenant in tenants]
    # Ожидаем завершения всех задач
    await asyncio.gather(*tasks)

    # Разделяем данные на партиции для вставки
    chunk_size = 2000
    for i in range(0, len(results_accumulator), chunk_size):
        chunk = results_accumulator[i:i + chunk_size]
        # Вставляем данные в базу данных
        async with pool.acquire() as conn:
            await insert_sms_stats_with_copy(conn, chunk)
    logger.info(f'Инстанс - {color}, передано {len(results_accumulator)} строк.')


async def main():
    """
    Основная асинхронная функция для выполнения DAG.
    Логика работы:
    1. Подключается к базе данных REAPER (public).
    2. Определяет временной период для парсинга
    3. Извлекает список всех тенантов и коннектов до реплик.
    4. Обрабатывает тенанты параллельно по цветам.
    """
    # Инициализация логгера
    logger = fl.setup_logger('dag_collect_tenant_day_sms_nqes')
    logger.info('--------------------------------------------------')
    logger.info('Начало работы DAG dag_collect_tenant_day_sms_nqes')

    # Засекаем время начала выполнения
    start_timestamp = datetime.now()
    pool = None

    try:
        # Создаем пул подключений к базе данных
        pool = await asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=40, timeout=10)
        async with pool.acquire() as conn:
            max_date = await qs.select_max_value(conn, 'dwh.agg_tenant_day_sms_and_nqes_notifications', 'dim_day_start')
            # Получаем последнее число в таблице и округляем до следующих суток
            from_created_date = max_date + timedelta(days=1)
            print('m', max_date, 'f', from_created_date, 't',  fg.get_today().date())
            if from_created_date == fg.get_today().date():
                logger.info(f'Указан период менее одних суток. Обходчик не будет запущен.')
                return
            if from_created_date is None:
                logger.info(f'Таблица пустая, за стартовую дату принимается {FIRST_DATE}.')
                from_created_date = FIRST_DATE
            # Извлекаем список всех тенантов
            all_tenants = await qs.select_tenants(conn)
            to_created_date = fg.get_today()
        df_all_tenants = pd.DataFrame(all_tenants, columns=all_tenants[0].keys())

        # Создаем задачи для обработки тенантов по цветам
        tasks = [
            asyncio.create_task(process_color(
                pool,
                color,
                df_all_tenants[df_all_tenants['color'] == color].to_dict(orient='records'),
                asyncio.Semaphore(COLORS_SEMAPHORES[color]),
                logger,
                from_created_date,
                to_created_date
            ))
            for color in COLORS
        ]
        # Ожидаем завершения всех задач
        await asyncio.gather(*tasks)
    finally:
        # Логируем завершение работы
        logger.info(f'Время работы dag_collect_tenant_day_sms_nqes = {datetime.now() - start_timestamp}.')
        if pool:
            await pool.close()


def run_async_func():
    """
    Запускает основную асинхронную функцию main.
    """
    asyncio.run(main())


# Создание DAG для Airflow
with DAG(
    'dag_collect_tenant_day_sms_nqes',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['tenant']
) as dag:
    collect_sms_stats_task = PythonOperator(
        task_id='collect_tenant_day_sms_nqes',
        python_callable=run_async_func,
    )

    collect_sms_stats_task
