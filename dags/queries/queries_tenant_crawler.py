from config import FIRST_DATE, DB_PASSWORD, DWH_PASSWORD
from datetime import datetime
from functions import functions_general as fg


async def select_sms_stats(conn, datname, db_host, from_created_date=FIRST_DATE, to_created_date=fg.get_today()):
    query = f"""
    SELECT *
    FROM dblink(
             get_connection_string(a_dbname := $1, a_host := $2, a_password := $3),
             $$
             WITH constants AS (SELECT CLOCK_TIMESTAMP() AS ts),
                 combined_data AS (
                     SELECT 'УНЭП' AS sms_type, created_date, (SELECT ts FROM constants) AS ts
                     FROM ekd_ca.ca_document_signing_request
                     WHERE confirmation_channel_type = 'SMS'
                       AND created_date >= '{from_created_date}'
                       AND created_date < '{to_created_date}'
                     UNION ALL
                     SELECT 'СМС' AS sms_type, created_date, (SELECT ts FROM constants) AS ts
                     FROM ekd_notification.sms_notification
                     WHERE sms_notification.notification_status IN ('QUEUED', 'URL_SHORTENED', 'SENT')
                       AND service_provider = 'pechkin'
                       AND created_date >= '{from_created_date}'
                       AND created_date < '{to_created_date}'

                     )
             SELECT DATE_TRUNC('day', created_date)::date AS start_of_day
                  , COUNT(1) FILTER (WHERE sms_type = 'УНЭП') AS cnt_nqes
                  , COUNT(1) FILTER (WHERE sms_type = 'СМС') AS cnt_sms
                  , EXTRACT(EPOCH FROM CLOCK_TIMESTAMP() - MIN(ts)) AS ts_delta
             FROM combined_data
             GROUP BY start_of_day
             ORDER BY start_of_day;
             $$
     ) AS t (
             start_of_day DATE,
             cnt_nqes BIGINT,
             cnt_sms BIGINT,
             ts_delta DOUBLE PRECISION
    );
    """
    return await conn.fetch(query, datname, db_host, DWH_PASSWORD)
