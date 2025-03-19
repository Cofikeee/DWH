# Прочие библиотеки
import asyncio

import asyncpg
from datetime import datetime
import pandas as pd
import io
import csv
import os
# Конфиг
from config import DB_CONFIG, COLORS, COLORS_SEMAPHORES, TEN_FIRST_DATE, DWH_USER, DWH_PASSWORD, DB_PORT
# Запросы к БД
from queries import queries_select as qs, queries_tenant as qt, queries_log as ql
# Функции
from functions import functions_general as fg

TABLE_QUERY = {
    'agg_c_signing_notification_sms_d': qt.collect_agg_c_signing_day,
                         'agg_n_sms_d': qt.collect_agg_n_sms_day,
                     'agg_s_session_d': qt.collect_agg_s_session_day,
                            'dim_user': qt.collect_user,
                      'dim_user_login': qt.collect_user_login
}


async def process_tenant(tenant, semaphore, csv_writer, from_created_date, to_created_date, table_name):
    """
    Асинхронная функция для обработки данных одного тенанта.
    :param pool: Пул подключений к базе данных.
    :param tenant: Информация о тенанте (словарь с данными).
    :param semaphore: Семафор для ограничения параллельных задач.
    :param csv_writer: CSV-писатель для записи данных в файл.
    :param from_created_date: Таймстамп начала парсинга для фильтрации и логгирования.
    :param to_created_date: Таймстамп конца парсинга для фильтрации.
    :param table_name: Имя целевой таблицы для вставки данных.
    """
    collect_query = TABLE_QUERY[table_name]
    tenant_id = tenant['tenant_id']
    db_name = tenant['datname']
    db_host = tenant['db_host']
    async with semaphore:
        pool = await asyncpg.create_pool(
            host=db_host,
            port=DB_PORT,
            user=DWH_USER,
            database=db_name,
            password=DWH_PASSWORD,
            min_size=1,
            max_size=1,
            timeout=10
        )
        async with pool.acquire() as conn:
            results = await collect_query(conn, from_created_date, to_created_date)
            if results:
                for row in results:
                    csv_writer.writerow([tenant_id, *row.values()])


async def process_color(pool, color, tenants, semaphore, logger, from_created_date, to_created_date, schema_name, table_name, columns):
    """
    Асинхронная функция для обработки всех тенантов одного цвета.

    :param pool: Пул подключений к базе данных.
    :param color: Цвет тенантов для обработки.
    :param tenants: Список тенантов данного цвета.
    :param semaphore: Семафор для ограничения параллельных задач.
    :param logger: Инициализированный логгер.
    :param from_created_date: Таймстамп начала парсинга для фильтрации и логгирования.
    :param to_created_date: Таймстамп конца парсинга для фильтрации.
    :param schema_name: Имя целевой схемы.
    :param table_name: Имя целевой таблицы для вставки данных.
    :param columns: Список названий столбцов для вставки данных.
    """
    temp_csv_filename = f"{color}_temp.csv"
    chunk_counter = 0
    # Создаем пул соединений для инстанса (цвета)

    # Открываем временный CSV-файл для записи
    with open(temp_csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(columns)  # Записываем заголовки

        # Создаем задачи для параллельной обработки тенантов
        tasks = [
            asyncio.create_task(
                process_tenant(
                    tenant, semaphore, csv_writer, from_created_date, to_created_date, table_name
                )
            ) for tenant in tenants
        ]

        # Ожидаем завершения всех задач
        await asyncio.gather(*tasks)

        # Проверяем размер файла и выгружаем данные, если необходимо
        csv_file.flush()  # Сбрасываем буфер записи на диск
        await process_csv_chunk(pool, temp_csv_filename, schema_name, table_name, columns, logger)
        chunk_counter += 1

    logger.info(f'Файл {temp_csv_filename} успешно обработан и выгружен в БД.')


async def process_csv_chunk(pool, csv_filename, schema_name, table_name, columns, logger):
    """
    Асинхронная функция для обработки чанка CSV-файла и выгрузки данных в БД.

    :param pool: Пул подключений к базе данных.
    :param csv_filename: Имя CSV-файла.
    :param schema_name: Имя целевой схемы.
    :param table_name: Имя целевой таблицы для вставки данных.
    :param columns: Список названий столбцов для вставки данных.
    :param logger: Инициализированный логгер.
    """
    with open(csv_filename, mode='r', encoding='utf-8') as csv_file:
        reader = csv.reader(csv_file)
        next(reader)  # Пропускаем заголовок

        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= 1000000:  # Размер мини-чанка для COPY 1kk
                await insert_data_with_copy(pool, chunk, schema_name, table_name, columns)
                chunk = []

        # Выгружаем оставшиеся данные
        if chunk:
            await insert_data_with_copy(pool, chunk, schema_name, table_name, columns)


    # Удаляем временный файл после обработки
    os.remove(csv_filename)


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
        pool = await asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=10, timeout=10)
        async with pool.acquire() as conn:
            # Получаем максимальную дату из таблицы для определения временного периода
            max_date = await qs.select_ten_max_parsed_date(conn, table_name)
            columns = await qs.select_column_names(conn, f'{schema_name}.{table_name}')

            # Проверка задан ли параметр from_created_date
            if not from_created_date:
                # Если таблица пустая, используем FIRST_DATE как стартовую дату
                if max_date is None:
                    logger.info(f'Таблица пустая, за стартовую дату принимается {TEN_FIRST_DATE}.')
                    from_created_date = TEN_FIRST_DATE
                else:
                    # Округляем последнюю дату до следующих суток
                    from_created_date = max_date

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
                    columns=columns
                )
            ) for color in COLORS
        ]

        # Ожидаем завершения всех задач
        await asyncio.gather(*tasks)
        async with pool.acquire() as conn:
            await ql.log_etl_tenants(conn, table_name)

    finally:
        # Логируем завершение работы
        logger.info(f'Время заполнения {schema_name}.{table_name} = {datetime.now() - start_timestamp}.')
        if pool:
            await pool.close()


async def insert_data_with_copy(pool, data, schema_name, table_name, columns):
    """
    Асинхронная функция для вставки данных в таблицу с использованием метода COPY.

    :param pool: Пул подключений к базе данных.
    :param data: Список кортежей с данными для вставки.
    :param schema_name: Имя целевой схемы.
    :param table_name: Имя целевой таблицы для вставки данных.
    :param columns: Список названий столбцов для вставки данных.
    """
    string_data = io.StringIO()
    writer = csv.writer(string_data, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    writer.writerows(data)

    byte_data = string_data.getvalue().encode("utf-8")
    byte_stream = io.BytesIO(byte_data)

    async with pool.acquire() as conn:
        await conn.copy_to_table(
            table_name=table_name,
            source=byte_stream,
            columns=columns,
            schema_name=schema_name,
            format='csv',
            delimiter='\t'
        )
