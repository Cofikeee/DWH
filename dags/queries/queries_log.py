from functions import functions_general as fg


async def log_etl_cases(conn, from_time, to_time, page, parsed, blacklisted, period_total):
    page_total = parsed + blacklisted
    query = """
        INSERT INTO dwh_log.ctl_omni_case (from_time, to_time, page, parsed, blacklisted, page_total, period_total, parsed_date)
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        ON CONFLICT (from_time, page) DO UPDATE
        SET to_time = EXCLUDED.to_time,
            parsed = EXCLUDED.parsed, 
            blacklisted = EXCLUDED.blacklisted, 
            page_total = EXCLUDED.page_total,
            period_total = EXCLUDED.period_total,
            parsed_date = NOW();
    """
    await conn.execute(query, from_time, to_time, page, parsed, blacklisted, page_total, period_total)


async def log_etl_users(conn, from_time, to_time, page, parsed, period_total):
    query = """
        INSERT INTO dwh_log.ctl_omni_user (from_time, to_time, page, parsed, period_total, parsed_date)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (from_time, page) DO UPDATE
        SET to_time = EXCLUDED.to_time,
            parsed = EXCLUDED.parsed, 
            period_total = EXCLUDED.period_total,
            parsed_date = NOW();
    """
    await conn.execute(query, from_time, to_time, page, parsed, period_total)


async def log_etl_messages(conn, case_id, page, parsed, period_total):
    query = """
        INSERT INTO dwh_log.ctl_omni_message (case_id, page, parsed, period_total, parsed_date)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (case_id, page) DO UPDATE
        SET parsed = EXCLUDED.parsed,
            period_total = EXCLUDED.period_total,
            parsed_date = NOW();
            """
    await conn.execute(query, case_id, page, parsed, period_total)


async def log_etl_catalogues(conn, table_name, total_count=None):
    # Список разрешенных таблиц
    allowed_tables = {'dim_omni_company', 'dim_omni_group', 'dim_omni_label', 'dim_omni_staff', 'lookup_omni_custom_field', 'dim_omni_user'}

    # Проверка допустимости имени таблицы
    if table_name not in allowed_tables:
        raise ValueError(f"Недопустимое имя таблицы: {table_name}")

    query = f"""
        INSERT INTO dwh_log.ctl_omni_catalogue (table_name, count_total, parsed_total, parsed_date)
        VALUES($1, $2, (SELECT count(1) FROM dwh_omni.{table_name}), date_trunc('day', now()))
        ON CONFLICT (table_name, parsed_date) DO UPDATE
        SET parsed_total = EXCLUDED.parsed_total
    """
    await conn.execute(query, table_name, total_count)


async def log_etl_tenants(conn, table_name):
    query = f"""
        INSERT INTO dwh_log.ctl_ten_cralwer(table_name, parsed_date) 
        VALUES($1, $2)
        ON CONFLICT (table_name, parsed_date) DO NOTHING;
    """
    if table_name.endswith('_m'):
        await conn.execute(query, table_name, fg.get_last_date_of_month())
    else:
        await conn.execute(query, table_name, fg.get_today())
