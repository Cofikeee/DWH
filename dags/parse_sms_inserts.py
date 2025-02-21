from config import *
import asyncio
import asyncpg
import pandas as pd
import json
import cProfile
import pstats
import logging

logging.basicConfig(level=logging.INFO)

DB_CONFIG = {
    "user": DB_USER,          # имя пользователя PostgreSQL
    "password": DB_PASSWORD,  # пароль PostgreSQL
    "host": DB_HOST,          # адрес БД
    "port": DB_PORT,          # порт БД
    "database": REAPER_DB_NAME  # имя БД
}

async def insert_sms_stats(conn, data):
    query = """
        INSERT INTO dwh.tenant_sms_and_nqes_notifications_test (
            start_of_week,
            cnt_nqes,
            cnt_sms,
            ts_delta,
            tenant_id,
            tenant_name,
            parsed_date
        ) 
        VALUES($1, $2, $3, $4, $5, $6, now()) 
        ON CONFLICT (tenant_id, start_of_week) DO UPDATE
        SET cnt_nqes = EXCLUDED.cnt_nqes,
            cnt_sms = EXCLUDED.cnt_sms,
            ts_delta = EXCLUDED.ts_delta,
            tenant_name = EXCLUDED.tenant_name,
            parsed_date = EXCLUDED.parsed_date;
    """
    await conn.executemany(query, data)  # Выполняет пакетную вставку данных

async def select_tenants(conn):
    query = f"""
    SELECT instance_name as color, db_host, datname, tenant_id, tenant_name
    FROM public.v_tenant_database_replica
    WHERE db_schema = 'ekd_ca'
    ORDER BY datname
    LIMIT 2000;
    """
    query_result = await conn.fetch(query)
    return query_result

async def select_sms_stats(conn, datname, db_host, created_date=pd.to_datetime('2020-01-01')):
    query = f"""
    SELECT *
    FROM dblink(
                 get_connection_string(
                         a_dbname := $1,
                         a_host := $2,
                         a_password := $3
                 ),
                 $$
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
                 $$
         ) AS t (
                 start_of_week DATE,
                 cnt_nqes BIGINT,
                 cnt_sms BIGINT,
                 ts_delta DOUBLE PRECISION
        );
    """
    query_result = await conn.fetch(query, datname, db_host, DB_PASSWORD)
    return query_result

async def process_tenant(pool, tenant, semaphore, color):
    """Обрабатывает одного тенанта."""
    async with semaphore:  # Ограничиваем количество одновременных запросов
        db_host = tenant['db_host']
        datname = tenant['datname']
        tenant_id = tenant['tenant_id']
        tenant_name = tenant['tenant_name']
        db_config = {
            "user": DB_USER,
            "password": DB_PASSWORD,
            "host": db_host,
            "port": DB_PORT,
            "database": datname
        }
        async with pool.acquire() as conn:
            results = await select_sms_stats(conn, datname, db_host)
            results_accumulator = []
            if results:
                for row in results:
                    # Добавляем данные в общий аккумулятор
                    results_accumulator.append((
                        row['start_of_week'],
                        row['cnt_nqes'],
                        row['cnt_sms'],
                        row['ts_delta'],
                        tenant_id,
                        tenant_name
                        ))
                logging.info(f'success, {color}, {datname}')
                await insert_sms_stats(conn, results_accumulator)

async def process_color(pool, color, tenants, semaphore):
    """Обрабатывает всех тенантов для одного цвета."""
    tasks = []
    for tenant in tenants:
        task = asyncio.create_task(
            process_tenant(pool, tenant, semaphore, color)
        )
        tasks.append(task)
    # Ждем завершения всех задач для данного цвета
    await asyncio.gather(*tasks)
    # Вставка собранных данных в целевую таблицу
 #   async with pool.acquire() as conn:
   #     await insert_sms_stats(conn, results_accumulator)


async def main():
    timestamp = datetime.now()
    colors = ['green', 'blue', 'black', 'pink', 'gold']
    pool = None
    try:
        pool = await asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=30, timeout=10)
        async with pool.acquire() as conn:
            all_tenants = await select_tenants(conn)
        df_all_tenants = pd.DataFrame(all_tenants, columns=all_tenants[0].keys())
        semaphores = {'green': asyncio.Semaphore(8),
                      'blue': asyncio.Semaphore(12),
                      'black': asyncio.Semaphore(8),
                      'pink': asyncio.Semaphore(4),
                      'gold': asyncio.Semaphore(2)}
        tasks = [
            asyncio.create_task(process_color(pool, color, df_all_tenants[df_all_tenants['color'] == color].to_dict(orient='records'), semaphores[color]))
            for color in colors
        ]
        await asyncio.gather(*tasks)
    finally:
        print(datetime.now())
        print(datetime.now() - timestamp)
        if pool:
            await pool.close()


def profile_to_dataframe(profiler):
    """
    Преобразует результаты профилирования в pandas.DataFrame.
    :param profiler: Объект cProfile.Profile.
    :return: pandas.DataFrame с данными о профилировании.
    """
    stats = pstats.Stats(profiler)
    stats.strip_dirs()  # Удаляем пути к файлам для упрощения данных

    # Создаем DataFrame из статистики
    rows = []
    for func, (cc, nc, tt, ct, callers) in stats.stats.items():
        rows.append({
            "function": func,
            "primitive_calls": cc,  # Количество примитивных вызовов
            "total_calls": nc,      # Общее количество вызовов
            "total_time": tt,       # Общее время выполнения
            "cumulative_time": ct   # Накопленное время выполнения
        })

    return pd.DataFrame(rows)


if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    asyncio.run(main())
    profiler.disable()

    # Преобразуем результаты профилирования в DataFrame
    df = profile_to_dataframe(profiler)
    filtered_df = df[df['cumulative_time'] > 1]
    print(filtered_df.to_string())

    # Сохраняем DataFrame в CSV
    filtered_df.to_csv("profiling_results.csv", index=False)