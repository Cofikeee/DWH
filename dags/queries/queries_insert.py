async def insert_cases(conn, cases_data):
    """Массовая вставка обращений в БД."""
    query = """
        INSERT INTO dwh_omni.fact_omni_case(
            case_id, 
            case_number, 
            subject, 
            omni_user_id, 
            staff_id, 
            group_id, 
            category_id, 
            block_id, 
            topic_id, 
            task_id, 
            status, 
            priority, 
            rating, 
            device, 
            incident, 
            mass_issue, 
            created_date, 
            updated_date, 
            closed_date
        ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) 
        ON CONFLICT (case_id) DO UPDATE
        SET subject = EXCLUDED.subject, 
            staff_id = EXCLUDED.staff_id,
            group_id = EXCLUDED.group_id, 
            category_id = EXCLUDED.category_id,
            block_id = EXCLUDED.block_id, 
            topic_id = EXCLUDED.topic_id,
            task_id = EXCLUDED.task_id, 
            status = EXCLUDED.status,
            priority = EXCLUDED.priority, 
            device = EXCLUDED.device,
            incident = EXCLUDED.incident, 
            mass_issue = EXCLUDED.mass_issue,
            updated_date = EXCLUDED.updated_date, 
            closed_date = EXCLUDED.closed_date;
    """
    if cases_data:
        await conn.executemany(query, cases_data)


async def insert_case_labels(conn, labels_data):
    """Массовая вставка меток обращений в БД."""
    query = """
        INSERT INTO dwh_omni.bridge_omni_case_label (case_id, label_id)
        VALUES ($1, $2)
        ON CONFLICT (case_id, label_id) DO NOTHING;
    """
    values = [(case_id, label) for case_id, labels in labels_data for label in labels]
    if values:
        await conn.executemany(query, values)


async def insert_companies(conn, response_data):
    """Массовая вставка компаний в БД."""
    query = """
        INSERT INTO dwh_omni.dim_omni_company(
            company_id,
            company_name,
            tarif,
            btrx_id,
            responsible,
            active,
            deleted,
            created_date,
            updated_date
        ) 
        VALUES($1, $2, (SELECT json_data::json ->> $3 FROM dwh_omni.lookup_omni_custom_field WHERE field_id = 8963), $4, $5, $6, $7, $8, $9) 
        ON CONFLICT (company_id) DO UPDATE
        SET company_name = EXCLUDED.company_name,
            tarif = EXCLUDED.tarif,
            btrx_id = EXCLUDED.btrx_id,
            responsible = EXCLUDED.responsible,
            active = EXCLUDED.active,
            deleted = EXCLUDED.deleted,
            created_date = EXCLUDED.created_date,
            updated_date = EXCLUDED.updated_date;
    """

    await conn.executemany(query, response_data)  # Выполняет пакетную вставку данных.


async def insert_custom_fields(conn, response_data):
    """Массовая вставка пользовательских полей в БД."""
    query = """
        INSERT INTO dwh_omni.lookup_omni_custom_field(
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


async def insert_dimension(conn, table_name, field_id, id_column, name_column):
    """
    Универсальная функция для вставки категорий в указанные таблицы
    на основе данных из lookup_omni_custom_field.

    Аргументы:
    conn -- соединение с базой данных.
    table_name -- имя таблицы для вставки.
    field_id -- id поля, данные которого нужно извлечь.
    id_column -- id столбца в целевой таблице.
    name_column -- имя столбца в целевой таблице.
    """

    insert_query = f"""
    INSERT INTO dwh_omni.{table_name} ({id_column}, {name_column})
    WITH json_data_table AS (
        SELECT json_data
        FROM dwh_omni.lookup_omni_custom_field
        WHERE field_id = '{field_id}'
    )
    SELECT key::int AS {id_column}, value AS {name_column}
    FROM json_data_table,
         jsonb_each_text(json_data)
    ON CONFLICT ({id_column}) DO UPDATE
    SET {name_column} = EXCLUDED.{name_column};
    """

    await conn.execute(insert_query)  # Выполняет вставку данных


async def insert_groups(conn, response_data):
    """Массовая вставка групп в БД."""
    query = """
        INSERT INTO dwh_omni.dim_omni_group(
            group_id,
            group_name,
            active,
            created_date,
            updated_date
        ) 
        VALUES($1, $2, $3, $4, $5) 
        ON CONFLICT (group_id) DO UPDATE
        SET group_name = EXCLUDED.group_name,
            active = EXCLUDED.active,
            created_date = EXCLUDED.created_date,
            updated_date = EXCLUDED.updated_date;
    """

    await conn.executemany(query, response_data)  # Выполняет пакетную вставку данных.


async def insert_labels(conn, response_data):
    """Массовая вставка меток в БД."""
    query = """
        INSERT INTO dwh_omni.dim_omni_label(
            label_id,
            label_name
        ) 
        VALUES($1, $2) 
        ON CONFLICT (label_id) DO UPDATE
        SET label_name = EXCLUDED.label_name;
    """

    await conn.executemany(query, response_data)  # Выполняет пакетную вставку данных.


async def insert_messages(conn, messages_data):
    """Массовая вставка сообщений в БД."""
    query = """
        INSERT INTO dwh_omni.dim_omni_message (
            message_id, 
            case_id, 
            omni_user_id,
            staff_id,  
            message_type, 
            attachment_type, 
            message_text, 
            created_date
        ) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (message_id, case_id) DO NOTHING;
            """
    await conn.executemany(query, messages_data)



async def insert_staff(conn, response_data):
    """Массовая вставка сотрудников в БД."""
    query = """
        INSERT INTO dwh_omni.dim_omni_staff(
            staff_id,
            full_name,
            email,
            active,
            created_date,
            updated_date
        ) 
        VALUES($1, $2, $3, $4, $5, $6) 
        ON CONFLICT (staff_id) DO UPDATE
        SET full_name = EXCLUDED.full_name,
            email = EXCLUDED.email,
            active = EXCLUDED.active,
            created_date = EXCLUDED.created_date,
            updated_date = EXCLUDED.updated_date;
    """

    await conn.executemany(query, response_data)  # Выполняет пакетную вставку данных


async def insert_users(conn, users_data):
    """Массовая вставка пользователей в БД."""
    query = """
        INSERT INTO dwh_omni.dim_omni_user(
            omni_user_id,
            omni_user_name,
            omni_channel_type,
            omni_channel_value,
            company_id,
            user_id,
            confirmed,
            omni_roles,
            created_date,
            updated_date
        ) 
        VALUES($1, $2, $3, $4, (SELECT min(company_id) FROM dwh_omni.dim_omni_company WHERE company_name = $5), $6, $7, $8, $9, $10) 
        ON CONFLICT (omni_user_id) DO UPDATE
        SET omni_user_name = EXCLUDED.omni_user_name,
            omni_channel_type = EXCLUDED.omni_channel_type,
            omni_channel_value = EXCLUDED.omni_channel_value,
            company_id = EXCLUDED.company_id,
            user_id = EXCLUDED.user_id,
            confirmed = EXCLUDED.confirmed,
            omni_roles = EXCLUDED.omni_roles,
            created_date = EXCLUDED.created_date,
            updated_date = EXCLUDED.updated_date;
    """

    await conn.executemany(query, users_data)


async def insert_changelogs(conn, changelogs_data):
    """Массовая вставка сообщений в БД."""
    query = """
        INSERT INTO dwh_omni.dim_omni_changelog (
            case_id,
            event,
            old_value,
            new_value,
            created_date
        ) 
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (case_id, event, created_date) DO UPDATE 
        SET old_value = EXCLUDED.old_value,
        new_value = EXCLUDED.new_value;
    """
    try:
        await conn.executemany(query, changelogs_data)
    except Exception as e:
        print(f"Ошибка при выполнении SQL-запроса: {e}")
        raise
