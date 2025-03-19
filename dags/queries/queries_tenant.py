async def collect_agg_n_sms_day(conn, from_created_date, to_created_date):
    query = f"""
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
    """
    return await conn.fetch(query)


async def collect_agg_s_session_day(conn, from_created_date, to_created_date):
    query = f"""
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
    """
    return await conn.fetch(query)


async def collect_agg_c_signing_day(conn, from_created_date, to_created_date):
    query = f"""
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
    """
    return await conn.fetch(query)


async def collect_user(conn, from_modified_date, to_modified_date):
    query = f"""
        WITH
            roles AS (
                SELECT user_id
                     , COALESCE(
                        MAX(CASE
                                WHEN name_key = 'ekd.roles.client.owner.name'
                                    THEN 'Администратор' END),
                        MAX(CASE
                                WHEN name_key = 'ekd.roles.client.newsEditor.name'
                                    THEN 'Новостник' END)) AS role
                     , MAX(cur.modified_date)              AS modified_date
                FROM ekd_ekd.client_user                cu
                     LEFT JOIN ekd_ekd.client_user_role cur ON cu.id = cur.client_user_id
                     LEFT JOIN ekd_ekd.user_role        ur ON cur.user_role_id = ur.id
                WHERE name_key IS NOT NULL
                GROUP BY user_id
            ),
            certs AS (
                SELECT person_id
                     , fingerprint               AS user_fingerprint
                     , confirmation_channel_type AS user_signing_channel_type
                     , confirmation_channel      AS user_signing_channel_value
                     , modified_date
                FROM ekd_id.person_certificate
                WHERE annulled_date IS NULL
                  AND expires_date > NOW()
            )
        SELECT p.user_id                                                                AS user_id
             , user_fingerprint
             , p.last_name || ' ' || p.first_name || ' ' || COALESCE(p.patronymic, ' ') AS user_fio
             , user_signing_channel_type
             , user_signing_channel_value
             , r.role                                                                   AS user_role
             , CASE
                   WHEN u.disabled_date IS NULL AND u.hash IS NOT NULL
                       THEN TRUE
                   ELSE FALSE END                                                       AS user_active
        FROM ekd_id.person      p
             JOIN ekd_id."user" u ON p.user_id = u.id
             LEFT JOIN certs    c ON p.id = c.person_id
             LEFT JOIN roles    r ON u.id = r.user_id 
          AND ((c.modified_date >= '{from_modified_date}' AND c.modified_date < '{to_modified_date}')
           OR  (p.modified_date >= '{from_modified_date}' AND p.modified_date < '{to_modified_date}')
           OR  (u.modified_date >= '{from_modified_date}' AND u.modified_date < '{to_modified_date}')
           OR  (r.modified_date >= '{from_modified_date}' AND r.modified_date < '{to_modified_date}'));
    """
    return await conn.fetch(query)


async def collect_user_login(conn, from_modified_date, to_modified_date):
    query = f"""
        SELECT ul.user_id
             , login_type AS user_channel_type
             , login      AS user_channel_value
             , enabled    AS user_channel_active
        FROM ekd_id.user_login                ul
             JOIN ekd_id.notification_channel nc ON ul.id = nc.user_login_id
        WHERE ((ul.modified_date >= '{from_modified_date}' AND ul.modified_date < '{to_modified_date}')
            OR (nc.modified_date >= '{from_modified_date}' AND nc.modified_date < '{to_modified_date}'));
    """
    return await conn.fetch(query)

