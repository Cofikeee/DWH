from config import FIRST_DATE, DWH_PASSWORD, DWH_USER
from functions import functions_general as fg


async def collect_agg_n_sms_day(conn, datname, db_host, from_created_date, to_created_date):
    query = f"""
        SELECT *
        FROM dblink(
            get_connection_string(a_dbname := '{datname}', a_host := '{db_host}', a_password := '{DWH_PASSWORD}'),
            $$
            WITH constants     AS (SELECT CLOCK_TIMESTAMP() AS ts)
            SELECT person.user_id AS dim_user_id,
                   last_name || ' ' || first_name || ' ' || COALESCE(patronymic, ' ') AS dim_person_fio,
                   DATE_TRUNC('day', sms_notification.created_date) AS dim_start_of_day,
                   COUNT(1) AS agg_cnt_all_sms,
                   COUNT(CASE WHEN text ~* 'Код для входа|создания нового пароля|Для принятия приглашения' THEN 1 END) AS agg_cnt_auto_sms,
                   COUNT(CASE WHEN text ~* 'кадровый документ|кадровые документы|Документ на ознакомление|Документы, ожидающие подписания|Документов, ожидающих|для подписания документа' THEN 1 END) AS agg_cnt_doc_sms,
                   COUNT(CASE WHEN text ~* 'заявлени' THEN 1 END) AS agg_cnt_app_sms,
                   COUNT(CASE WHEN text ~* 'Для удаленной идентификации|был изменен канал уведомлений|смену канала уведомлений|пользователь отключ|сотрудник подтвердил|сотрудник отклонил|УЦ сообщил об ошибке|для выпуска|(завершите.*подписи)|канал получения кода|для подтверждения|код подтверждения|смену канала' THEN 1 END) AS agg_cnt_emp_sms,
                   COUNT(CASE WHEN text ~* 'доверенност' THEN 1 END) AS agg_cnt_mchd_sms
                , EXTRACT(EPOCH FROM CLOCK_TIMESTAMP() - (SELECT MIN(ts) FROM constants)) AS ctl_ts_delta

            FROM ekd_notification.sms_notification
                 JOIN ekd_id.person ON person.user_id = sms_notification.user_id
            WHERE sms_notification.notification_status IN ('QUEUED', 'URL_SHORTENED', 'SENT')
              AND sms_notification.created_date >= '{from_created_date}'
              AND sms_notification.created_date < '{to_created_date}'
            GROUP BY dim_start_of_day, dim_user_id, dim_person_fio
            ORDER BY 1;
            $$
        ) AS t (
            dim_user_id UUID,
            dim_person_fio VARCHAR,
            dim_start_of_day DATE,
            agg_cnt_all_sms INTEGER,
            agg_cnt_auto_sms INTEGER,
            agg_cnt_doc_sms INTEGER,
            agg_cnt_app_sms INTEGER,
            agg_cnt_emp_sms INTEGER,
            agg_cnt_mchd_sms INTEGER,
            ctl_ts_delta DOUBLE PRECISION
        );
    """
    return await conn.fetch(query)


async def collect_agg_s_session_day(conn, datname, db_host, from_created_date, to_created_date):
    query = f"""
        SELECT *
        FROM dblink(
            get_connection_string(a_dbname := '{datname}', a_host := '{db_host}', a_password := '{DWH_PASSWORD}'),
            $$
            WITH
                autorization AS (
                    SELECT user_id                         AS dim_user_id
                         , id
                         , created_date
                         , DATE_TRUNC('day', created_date) AS dim_start_of_day
                         , CASE
                               WHEN user_agent ~* 'HRlinkAppAndroid|HRlinkAppIOS' THEN 'Мобильное приложение'
                               WHEN user_agent ~* 'Android|iPhone' THEN 'Телефон'
                               WHEN user_agent ~* 'Windows|Macintosh|(Linux.*[^Android])' THEN 'Компьютер'
                               WHEN user_agent ~* 'iPad' THEN 'IPad'
                               WHEN user_agent ~* 'PostmanRuntime' THEN 'PostmanRuntime'
                               WHEN user_agent ~* '1C' THEN '1C'
                               ELSE 'Неизвестно'
                        END                                AS dim_device
                         , CASE
                               WHEN user_agent ~* 'HRlinkAppAndroid' THEN 'Приложение на Android'
                               WHEN user_agent ~* 'HRlinkAppIOS' THEN 'Приложение на IOS'
                               WHEN user_agent ~* 'Windows' THEN 'Windows'
                               WHEN user_agent ~* 'Macintosh' THEN 'Macintosh'
                               WHEN user_agent ~* 'Linux' AND user_agent !~* 'Android' THEN 'Linux'
                               WHEN user_agent ~* 'Android' THEN 'Android'
                               WHEN user_agent ~* 'iPhone|iPad' THEN 'IOS'
                               ELSE 'Неизвестно'
                        END                                AS dim_os
                         , CASE
                               WHEN user_agent ~* 'Firefox' THEN 'Mozilla Firefox'
                               WHEN user_agent ~* 'SamsungBrowser' THEN 'Samsung Internet'
                               WHEN user_agent ~* 'Opera|OPR' THEN 'Opera'
                               WHEN user_agent ~* 'YaBrowser' THEN 'YaBrowser'
                               WHEN user_agent ~* 'Edge' THEN 'Microsoft Edge (Legacy)'
                               WHEN user_agent ~* 'Edg' THEN 'Microsoft Edge (Chromium)'
                               WHEN user_agent ~* 'Chrome' THEN 'Google Chrome or Chromium'
                               WHEN user_agent ~* 'Safari' THEN 'Apple Safari'
                               WHEN user_agent ~* 'HRlinkAppAndroid' THEN 'Приложение на Android'
                               WHEN user_agent ~* 'HRlinkAppIOS' THEN 'Приложение на IOS'
                               ELSE 'Неизвестно'
                        END                                AS dim_browser
                    FROM ekd_session.cookie_session
                    WHERE created_date >= '{from_created_date}'
                      AND created_date < '{to_created_date}'
                ),
                min_sess     AS (
                    SELECT dim_user_id
                         , dim_start_of_day
                         , dim_device
                         , dim_os
                         , dim_browser
                         , FIRST_VALUE(id) OVER (PARTITION BY dim_user_id, dim_start_of_day, dim_device, dim_os, dim_browser 
                                                     ORDER BY created_date) AS ctl_min_session_id
                    FROM autorization
                )
            
            SELECT dim_user_id
                 , dim_device
                 , dim_os
                 , dim_browser
                 , dim_start_of_day
                 , COUNT(ctl_min_session_id)
                 , ctl_min_session_id
            FROM min_sess
            
            GROUP BY dim_user_id,
                     dim_start_of_day,
                     dim_device,
                     dim_os,
                     dim_browser,
                     ctl_min_session_id;
            $$
        ) AS t (
            dim_user_id UUID,
            dim_device VARCHAR,
            dim_os VARCHAR,
            dim_browser VARCHAR,
            dim_start_of_day DATE,
            agg_cnt_session INTEGER,
            ctl_min_session_id UUID
        );
    """
    return await conn.fetch(query)


async def collect_agg_c_signing_day(conn, datname, db_host, from_created_date, to_created_date):
    query = f"""
        SELECT *
        FROM dblink(
            get_connection_string(a_dbname := '{datname}', a_host := '{db_host}', a_password := '{DWH_PASSWORD}'),
            $$
            WITH constants     AS (SELECT CLOCK_TIMESTAMP() AS ts)
            SELECT person.user_id AS dim_user_id,
                   last_name || ' ' || first_name || ' ' || COALESCE(patronymic, ' ') AS dim_person_fio,
                   DATE_TRUNC('day', ca_document_signing_request.created_date) AS dim_start_of_day,
                   COUNT(DISTINCT ca_document_signing_request.id) AS agg_cnt_all_signing
                , EXTRACT(EPOCH FROM CLOCK_TIMESTAMP() - (SELECT MIN(ts) FROM constants)) AS ctl_ts_delta

            FROM ekd_ca.ca_document_signing_request
                 JOIN ekd_ca.ca_certificate ON LOWER(ca_certificate.fingerprint) = ca_document_signing_request.fingerprint
                 JOIN ekd_ca.nqes_issue_request ON ca_certificate.id = nqes_issue_request.ca_certificate_id
                 JOIN ekd_id.person ON nqes_issue_request.person_id = person.id
            WHERE confirmation_channel_type = 'SMS'
              AND ca_document_signing_request.created_date >= '{from_created_date}'
              AND ca_document_signing_request.created_date < '{to_created_date}'
            GROUP BY dim_start_of_day, dim_user_id, dim_person_fio
            $$
        ) AS t (
            dim_user_id UUID,
            dim_person_fio VARCHAR,
            dim_start_of_day DATE,
            agg_cnt_all_signing INTEGER,
            ctl_ts_delta DOUBLE PRECISION
        );
    """
    return await conn.fetch(query)
