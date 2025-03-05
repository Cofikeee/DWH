# Прочие библиотеки
import asyncio
import asyncpg
from datetime import datetime
import pandas as pd
import io
import csv
from asyncpg.pgproto.pgproto import timedelta
# Конфиг
from config import DB_CONFIG, COLORS, COLORS_SEMAPHORES, TEN_FIRST_DATE
# Запросы к БД
from queries import queries_select as qs, queries_tenant as qt
# Функции
from functions import functions_general as fg

TABLE_QUERY = {
    'agg_c_signing_notification_sms_d': qt.collect_agg_c_signing_day,
                         'agg_n_sms_d': qt.collect_agg_n_sms_day,
                     'agg_s_session_d': qt.collect_agg_s_session_day
}


async def process_tenant(pool, tenant, semaphore, results_accumulator, from_created_date, to_created_date, collect_query):
    """
    Асинхронная функция для обработки данных одного тенанта.

    :param pool: Пул подключений к базе данных.
    :param tenant: Информация о тенанте (словарь с данными).
    :param semaphore: Семафор для ограничения параллельных задач.
    :param results_accumulator: Список для накопления результатов.
    :param from_created_date: Таймстамп начала парсинга для фильтрации и логгирования.
    :param to_created_date: Таймстамп конца парсинга для фильтрации.
    :param collect_query: Функция для выполнения запроса к базе данных.
    """
    async with semaphore:
        db_host = tenant['db_host']
        datname = tenant['datname']
        tenant_id = tenant['tenant_id']

        # Получаем соединение с базой данных
        async with pool.acquire() as conn:
            # Извлекаем статистику для текущего тенанта
            results = await collect_query(conn, datname, db_host, from_created_date, to_created_date)

            if results:
                # Добавляем обработанные данные в аккумулятор результатов
                results_accumulator.extend([
                    (tenant_id, *row.values()) for row in results
                ])


async def process_color(pool, color, tenants, semaphore, logger, from_created_date, to_created_date, collect_query, schema_name, table_name, columns):
    """
    Асинхронная функция для обработки всех тенантов одного цвета.

    :param pool: Пул подключений к базе данных.
    :param color: Цвет тенантов для обработки.
    :param tenants: Список тенантов данного цвета.
    :param semaphore: Семафор для ограничения параллельных задач.
    :param logger: Инициализированный логгер.
    :param from_created_date: Таймстамп начала парсинга для фильтрации и логгирования.
    :param to_created_date: Таймстамп конца парсинга для фильтрации.
    :param collect_query: Функция для выполнения запроса к базе данных.
    :param schema_name: Имя целевой схемы.
    :param table_name: Имя целевой таблицы для вставки данных.
    :param columns: Список названий столбцов для вставки данных.
    """
    # Список для накопления результатов
    results_accumulator = []

    # Создаем задачи для параллельной обработки тенантов
    tasks = [
        asyncio.create_task(
            process_tenant(
                pool, tenant, semaphore, results_accumulator, from_created_date, to_created_date, collect_query
            )
        ) for tenant in tenants
    ]

    # Ожидаем завершения всех задач
    await asyncio.gather(*tasks)

    # Разделяем данные на партиции для вставки
    chunk_size = 10000
    for i in range(0, len(results_accumulator), chunk_size):
        chunk = results_accumulator[i:i + chunk_size]

        # Вставляем данные в базу данных
        async with pool.acquire() as conn:
            await fg.insert_data_with_copy(conn, chunk, schema_name, table_name, columns)

    logger.info(f'Инстанс - {color}, передано {len(results_accumulator)} строк.')


async def crawler(logger, schema_name, table_name, from_created_date=None):
    """
    Основная асинхронная функция для выполнения DAG.

    Логика работы:
    1. Подключается к базе данных REAPER (public).
    2. Определяет временной период для парсинга.
    3. Извлекает список всех тенантов и их коннекты до реплик.
    4. Обрабатывает тенанты параллельно по цветам.

    :param logger: Инициализированный логгер.
    :param schema_name: Имя схемы базы данных.
    :param table_name: Имя целевой таблицы для вставки данных.
    :param from_created_date: Таймстамп начала парсинга для фильтрации и логгирования.
    """
    # Засекаем время начала выполнения
    start_timestamp = datetime.now()
    pool = None

    try:
        # Создаем пул подключений к базе данных
        pool = await asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=25, timeout=10)

        async with pool.acquire() as conn:
            # Получаем максимальную дату из таблицы для определения временного периода
            max_date = await qs.select_max_value(conn, schema_name, table_name, 'dim_start_of_day')
            columns = await qs.select_column_names(conn, f'{schema_name}.{table_name}')

            # Проверка задан ли параметр from_created_date
            if not from_created_date:
                # Если таблица пустая, используем FIRST_DATE как стартовую дату
                if max_date is None:
                    logger.info(f'Таблица пустая, за стартовую дату принимается {TEN_FIRST_DATE}.')
                    from_created_date = TEN_FIRST_DATE
                else:
                    # Округляем последнюю дату до следующих суток
                    from_created_date = max_date + timedelta(days=1)

            logger.info(f'max_date - {max_date}, from_date - {from_created_date}, to_date - {fg.get_today().date()}')

            # Если временной период менее одних суток, прерываем выполнение
            if from_created_date == fg.get_today().date():
                logger.info(f'Указан период менее одних суток. Обходчик не будет запущен.')
                return

            to_created_date = fg.get_today()

            # Извлекаем список всех тенантов
            all_tenants = await qs.select_tenants(conn)

        # Преобразуем список тенантов в DataFrame для удобства фильтрации
        df_all_tenants = pd.DataFrame(all_tenants, columns=all_tenants[0].keys())

        # Создаем задачи для обработки тенантов по цветам
        tasks = [
            asyncio.create_task(
                process_color(
                    pool=pool,
                    color=color,
                    tenants=df_all_tenants[df_all_tenants['color'] == color].to_dict(orient='records'),
                    semaphore=asyncio.Semaphore(COLORS_SEMAPHORES[color]),
                    logger=logger,
                    from_created_date=from_created_date,
                    to_created_date=to_created_date,
                    schema_name=schema_name,
                    table_name=table_name,
                    columns=columns,
                    collect_query=TABLE_QUERY[table_name]
                )
            ) for color in COLORS
        ]

        # Ожидаем завершения всех задач
        await asyncio.gather(*tasks)

    finally:
        # Логируем завершение работы
        logger.info(f'Время запролнения {schema_name}.{table_name} = {datetime.now() - start_timestamp}.')
        if pool:
            await pool.close()
