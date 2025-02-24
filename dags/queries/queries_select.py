import asyncpg
from psycopg2 import sql
import psycopg2

from config import OMNI_DB_DSN, REAPER_DB_DSN


async def select_missing_case_ids(conn, offset_skew):
    query = """
        SELECT distinct case_id
        FROM v_message_logs
        WHERE parsed_total <> period_total
        UNION
        SELECT distinct case_id
        FROM fact_omni_case f
        WHERE NOT EXISTS (
            SELECT 1
            FROM dim_omni_message d
            WHERE d.case_id = f.case_id)
        AND NOT EXISTS (
            SELECT 1
            FROM ctl_etl_omni_message l
            WHERE period_total  = 0
            AND l.case_id = f.case_id)
        ORDER BY case_id
        LIMIT $1;
    """
    case_ids = await conn.fetch(query, offset_skew)
    case_ids = [row['case_id'] for row in case_ids]
    return case_ids


async def select_missing_catalogues(conn):
    query = """
        SELECT table_name
        FROM (SELECT table_name, max(parsed_date)
              FROM ctl_etl_omni_catalogue
              WHERE table_name <> 'dim_omni_user' and count_total > parsed_total
              GROUP BY table_name) as t
        ORDER BY table_name;
    """
    catalogues_array = await conn.fetch(query)
    catalogues_array = [row['from_time'] for row in catalogues_array]
    return catalogues_array


async def select_missing_case_dates(conn):
    query = """
        SELECT distinct from_time
        FROM v_case_logs
        WHERE period_total_whitelisted <> parsed_total
        ORDER BY from_time;
    """
    from_time_array = await conn.fetch(query)
    from_time_array = [row['from_time'] for row in from_time_array]
    return from_time_array


async def select_missing_user_dates(conn):
    query = """
        SELECT distinct from_time
        FROM v_user_logs
        WHERE period_total <> parsed_total
        ORDER BY from_time;
    """
    from_time_array = await conn.fetch(query)
    from_time_array = [row['from_time'] for row in from_time_array]
    return from_time_array


async def select_case_ids(conn, from_time, to_time, offset_skew, offset_value):
    query = """
        SELECT case_id FROM fact_omni_case
        WHERE updated_date >= $1
          AND updated_date < $2
        ORDER BY updated_date, created_date
        LIMIT $3 OFFSET $4;
    """
    case_ids = await conn.fetch(query, from_time, to_time, offset_skew, offset_value)
    case_ids = [row['case_id'] for row in case_ids]
    return case_ids


async def select_tenants(conn):
    query = """
    SELECT instance_name as color, db_host, datname, tenant_id, tenant_name, tenant_host
    FROM public.v_tenant_database_replica
    WHERE db_schema = 'ekd_ca'
    ORDER BY datname;
    """
    tenants = await conn.fetch(query)
    return tenants


async def select_max_value(conn, data_table, value_column):
    query = f"""
    SELECT MAX({value_column})
    FROM {data_table};
    """
    max_value = await conn.fetchval(query)
    return max_value


async def select_column_names(conn, table_name):
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = $1;
    """
    result = await conn.fetch(query, table_name)
    # Извлекаем названия колонок из результата
    column_names = [row['column_name'] for row in result]
    return column_names


def select_max_ts(data_table, date_column):
    conn = psycopg2.connect(OMNI_DB_DSN)
    cur = conn.cursor()
    cur.execute(
        sql.SQL("SELECT DATE_TRUNC('second', MAX({})) max_ts FROM {};").format(
            sql.Identifier(date_column), sql.Identifier(data_table),
        )
    )
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res[0][0]


def select_min_ts(data_table):
    conn = psycopg2.connect(OMNI_DB_DSN)
    cur = conn.cursor()
    cur.execute(
        sql.SQL("SELECT DATE_TRUNC('second', MIN(created_date)) min_ts FROM {};").format(
            sql.Identifier(data_table)
        )
    )
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res[0][0]
