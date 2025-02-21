import asyncio
import asyncpg
import pandas as pd
from datetime import datetime
from config import *

DB_CONFIG = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": REAPER_DB_NAME
}

async def create_dblink_connection(conn, db_host, conn_name):
    """
    Создает одно соединение через dblink с хостом.
    :param conn: Асинхронное соединение с базой данных.
    :param db_host: Хост для соединения.
    :param conn_name: Уникальное имя соединения.
    """
    connect_query = f"""
    SELECT dblink_connect(
        '{conn_name}', 
        'user={DB_USER} password={DB_PASSWORD} host={db_host} port={DB_PORT} dbname=postgres'
    );
    """
    try:
        await conn.execute(connect_query)
    except Exception as e:
        print(f"Ошибка при создании соединения {conn_name}: {e}")
        raise  # Перебрасываем исключение, чтобы задача завершилась с ошибкой

async def execute_query_on_db(conn, conn_name, dbname, query):
    """
    Выполняет запрос к конкретной базе данных через существующее соединение.
    :param conn: Асинхронное соединение с базой данных.
    :param conn_name: Имя соединения.
    :param dbname: Имя базы данных, к которой выполняется запрос.
    :param query: SQL-запрос для выполнения.
    :return: Результат выполнения запроса.
    """
    full_query = f"""
    SELECT *
    FROM dblink(
        '{conn_name}',
        $$
        SET search_path TO {dbname};
        {query}
        $$
    ) AS t (
        start_of_week DATE,
        cnt_nqes BIGINT,
        cnt_sms BIGINT,
        ts_delta DOUBLE PRECISION
    );pstats
    """
    return await conn.fetch(full_query)

async def process_tenants_in_parallel(pool, tenants, max_parallel=15):
    """
    Обрабатывает запросы к нескольким базам данных на одном хосте через dblink.
    :param pool: Пул соединений с базой данных.
    :param tenants: Список тенантов.
    :param max_parallel: Максимальное количество параллельных запросов.
    :return: Результаты запросов.
    """
    results_accumulator = []
    # Ограничение на количество параллельных запросов
    semaphore = asyncio.Semaphore(max_parallel)

    async def fetch_tenant_data(tenant):
        async with semaphore:  # Ограничиваем количество одновременных запросов
            async with pool.acquire() as conn:
                try:
                    # Создаем уникальное имя соединения для каждого тенанта
                    conn_name = f"conn_{tenant['datname']}"
                    # Создаем соединение с хостом
                    await create_dblink_connection(conn, tenant['db_host'], conn_name)
                    # Формируем запрос
                    query = f"""
                    WITH constants AS (SELECT clock_timestamp() AS ts),
                         combined_data AS (
                             SELECT 'УНЭП' AS sms_type, created_date, (SELECT ts FROM constants) AS ts
                             FROM ekd_ca.ca_document_signing_request
                             WHERE confirmation_channel_type = 'SMS'
                             UNION ALL
                             SELECT 'СМС' AS sms_type,
                                    created_date,
                                    (SELECT ts FROM constants) AS ts
                             FROM ekd_notification.sms_notification
                             WHERE sms_notification.notification_status IN ('QUEUED', 'URL_SHORTENED', 'SENT')
                               AND service_provider = 'pechkin'
                         )
                    SELECT DATE_TRUNC('week', created_date)::date AS start_of_week,
                           COUNT(1) FILTER (WHERE sms_type = 'УНЭП') AS cnt_nqes,
                           COUNT(1) FILTER (WHERE sms_type = 'СМС') AS cnt_sms,
                           EXTRACT(EPOCH FROM clock_timestamp() - MIN(ts)) AS ts_delta
                    FROM combined_data
                    GROUP BY start_of_week
                    ORDER BY start_of_week;
                    """
                    # Выполняем запрос
                    results = await execute_query_on_db(conn, conn_name, tenant['datname'], query)
                    if results:
                        for row in results:
                            results_accumulator.append((
                                row['start_of_week'],
                                row['cnt_nqes'],
                                row['cnt_sms'],
                                row['ts_delta'],
                                tenant['tenant_id'],
                                tenant['tenant_name'],
                                datetime.now()  # Добавляем текущую дату
                            ))
                finally:
                    # Закрываем соединение
                    disconnect_query = f"SELECT dblink_disconnect('{conn_name}');"
                    try:
                        await conn.execute(disconnect_query)
                    except Exception as e:
                        print(f"Ошибка при закрытии соединения {conn_name}: {e}")

    # Выполняем запросы параллельно с ограничением
    tasks = [asyncio.create_task(fetch_tenant_data(tenant)) for tenant in tenants]
    await asyncio.gather(*tasks)

    return results_accumulator

async def select_tenants(conn):
    query = """
    SELECT instance_name as color, db_host, datname, tenant_id, tenant_name
    FROM public.v_tenant_database_replica
    WHERE db_schema = 'ekd_ca'
    ORDER BY datname
    LIMIT 10;
    """
    return await conn.fetch(query)

async def main():
    timestamp = datetime.now()
    colors = ['green', 'blue', 'black', 'pink', 'gold']
    pool = None
    try:
        pool = await asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=30, timeout=10)
        async with pool.acquire() as conn:
            all_tenants = await select_tenants(conn)
        df_all_tenants = pd.DataFrame(all_tenants, columns=all_tenants[0].keys())
        semaphores = {'green': 8,
                      'blue': 12,
                      'black': 10,
                      'pink': 5,
                      'gold': 5}
        tasks = [
            asyncio.create_task(process_tenants_in_parallel(
                pool, df_all_tenants[df_all_tenants['color'] == color].to_dict(orient='records'), max_parallel=semaphores[color])
            )
            for color in colors
        ]
        await asyncio.gather(*tasks)
    finally:
        print(datetime.now())
        print(datetime.now() - timestamp)
        if pool:
            await pool.close()

if __name__ == "__main__":
    asyncio.run(main())