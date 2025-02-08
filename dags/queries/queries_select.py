from psycopg2 import sql
import psycopg2

from config import DB_DSN


# Получить case_id, где количество скачанных сообщений не соответствует
"""
select case_id
from ctl_etl_omni_messages
where parsed <> period_total;
"""


async def select_missing_case_ids(conn, offset_skew):
    case_ids = await conn.fetch("""
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
            FROM ctl_etl_omni_messages l
            WHERE period_total  = 0
            AND l.case_id = f.case_id)
        LIMIT $1;
    """, offset_skew)
    case_ids = [row['case_id'] for row in case_ids]
    return case_ids


async def select_missing_catalogues(conn):
    catalogues_array = await conn.fetch("""
        SELECT table_name
        FROM (SELECT table_name, max(parsed_date)
              FROM ctl_etl_omni_catalogues
              WHERE table_name <> 'dim_omni_user' and count_total > parsed_total
              GROUP BY table_name) as t
    """)
    catalogues_array = [row['from_time'] for row in catalogues_array]
    return catalogues_array


async def select_missing_case_dates(conn):
    from_time_array = await conn.fetch("""
        SELECT distinct from_time
        FROM v_case_logs
        WHERE period_total_whitelisted <> parsed_total;
    """)
    from_time_array = [row['from_time'] for row in from_time_array]
    return from_time_array


async def select_missing_user_dates(conn):
    from_time_array = await conn.fetch("""
        SELECT distinct from_time
        FROM v_case_logs
        WHERE period_total_whitelisted <> parsed_total;
    """)
    from_time_array = [row['from_time'] for row in from_time_array]
    return from_time_array


async def select_case_ids(conn, from_time, to_time, offset_skew, offset_value):
    case_ids = await conn.fetch("""
        SELECT case_id FROM fact_omni_case
        WHERE updated_date >= $1
          AND updated_date < $2
        ORDER BY updated_date, created_date
        LIMIT $3 OFFSET $4;
    """, from_time, to_time, offset_skew, offset_value)
    case_ids = [row['case_id'] for row in case_ids]
    return case_ids


def select_max_ts(data_table):
    if data_table == 'dim_omni_message':
        date_metric = 'created_date'
    else:
        date_metric = 'updated_date'
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute(
        sql.SQL("SELECT DATE_TRUNC('second', MAX({})) max_ts FROM {};").format(
            sql.Identifier(date_metric), sql.Identifier(data_table),
        )
    )
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res[0][0]


def select_min_ts(data_table):
    conn = psycopg2.connect(DB_DSN)
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
