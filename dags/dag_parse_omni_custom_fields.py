from airflow import DAG
from airflow.operators.python import PythonOperator

import aiohttp
import asyncio
import asyncpg
import json

from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
from functions import functions_general as fg
from queries import queries_log as ql


def field_data_extractor(record):
    return (
        record.get('field_id'),
        record.get('title'),
        record.get('field_type'),
        record.get('field_level'),
        record.get('active'),
        json.dumps(record.get('field_data'))
    )


async def insert_into_db(response_data, conn):
    """
    Вставляет данные о кастом филдах в базу данных.

    Аргументы:
    response_data -- список данных о кастом филдах для вставки.
    conn -- соединение с базой данных.
    """

    query = """
        INSERT INTO lookup_omni_custom_field(
            field_id,
            field_name,
            type,
            level,
            active,
            json_data
        ) 
        VALUES($1, $2, $3, $4, $5, $6) 
        ON CONFLICT (field_id) DO UPDATE
        SET field_name = EXCLUDED.field_name,
            type = EXCLUDED.type,
            level = EXCLUDED.level,
            active = EXCLUDED.active,
            json_data = EXCLUDED.json_data;
    """

    await conn.executemany(query, response_data)  # Выполняет пакетную вставку данных


async def insert_into_dimension(conn, table_name, field_id, id_column, name_column):
    """
    Универсальная функция для вставки категорий в указанные таблицы
    на основе данных из lookup_omni_custom_field.

    Аргументы:
    conn -- соединение с базой данных.
    table_name -- имя таблицы для вставки.
    field_id -- id поля, данные которого нужно извлечь.
    id_column -- имя столбца с id в целевой таблице.
    name_column -- имя столбца с именем в целевой таблице.
    """

    insert_query = f"""
    INSERT INTO {table_name} ({id_column}, {name_column})
    WITH json_data_table AS (
        SELECT json_data
        FROM lookup_omni_custom_field
        WHERE field_id = '{field_id}'
    )
    SELECT key::int AS {id_column}, value AS {name_column}
    FROM json_data_table,
         jsonb_each_text(json_data)
    ON CONFLICT ({id_column}) DO UPDATE
    SET {name_column} = EXCLUDED.{name_column};
    """

    await conn.execute(insert_query)  # Выполняет вставку данных


async def insert_into_all_dimensions(conn):
    print('Заполняю: dim_omni_category')
    await insert_into_dimension(conn, 'dim_omni_category', '6998', 'category_id', 'category_name')
    print('Заполняю: dim_omni_block')
    await insert_into_dimension(conn, 'dim_omni_block', '4605', 'block_id', 'block_name')
    print('Заполняю: dim_omni_topic')
    await insert_into_dimension(conn, 'dim_omni_topic', '8497', 'topic_id', 'topic_name')
    print('Заполняю: dim_omni_task')
    await insert_into_dimension(conn, 'dim_omni_task', '9129', 'task_id', 'task_name')


async def fetch_and_process_custom_fields():
    """
    Основная функция для получения данных о кастом филдах и их вставки в базу данных.
    """

    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        async with pool.acquire() as conn:
            total_count = await fg.get_snapshot(session, 'custom_fields')
            try:
                url = f'{OMNI_URL}/custom_fields.json?limit=100'  # Формируем ссылку для API запроса
                response = await fg.fetch_response(session, url)   # Создание запроса для получения данных
                response_data = fg.fetch_data(response, field_data_extractor, 'custom_field')  # Извлечение данных
                await insert_into_db(response_data, conn)  # Вставка данных в базу.
                await insert_into_all_dimensions(conn)     # Вставка категорий, блоков, тем и заданий в базу.
                print("Все данные обработаны.")

            finally:
                await ql.log_etl_catalogues(conn, 'lookup_omni_custom_field', total_count)
                await conn.close()  # Закрываем соединение с базой данных.


def run_async_func():
    """Запускает асинхронную функцию для получения и вставки данных о кастом филдах."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_process_custom_fields())


# Создаем DAG
with DAG(
        'dag_parse_omni_custom_fields',
        default_args=DAG_CONFIG,
        schedule_interval=None,  # Не запускать автоматически
        catchup=False,
) as dag:

    fetch_custom_field_task = PythonOperator(
        task_id='parse_omni_custom_fields',
        python_callable=run_async_func,
    )

    fetch_custom_field_task
