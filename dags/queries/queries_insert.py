async def insert_messages(conn, messages_data):
    query = """
        INSERT INTO dim_omni_message (
            message_id, case_id, omni_user_id, staff_id,  
            message_type, attachment_type, message_text, created_date
        ) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (message_id, case_id) DO NOTHING;
            """
    await conn.executemany(query, messages_data)


async def insert_cases(conn, cases_data):
    """Массовая вставка обращений в БД."""
    query = """
        INSERT INTO fact_omni_case(
            case_id, case_number, subject, omni_user_id, staff_id, group_id, 
            category_id, block_id, topic_id, task_id, status, priority, rating, device, 
            incident, mass_issue, created_date, updated_date, closed_date
        ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) 
        ON CONFLICT (case_id) DO UPDATE
        SET subject = EXCLUDED.subject, staff_id = EXCLUDED.staff_id,
            group_id = EXCLUDED.group_id, category_id = EXCLUDED.category_id,
            block_id = EXCLUDED.block_id, topic_id = EXCLUDED.topic_id,
            task_id = EXCLUDED.task_id, status = EXCLUDED.status,
            priority = EXCLUDED.priority, device = EXCLUDED.device,
            incident = EXCLUDED.incident, mass_issue = EXCLUDED.mass_issue,
            updated_date = EXCLUDED.updated_date, closed_date = EXCLUDED.closed_date;
    """
    if cases_data:
        await conn.executemany(query, cases_data)


async def insert_case_labels(conn, labels_data):
    """Массовая вставка меток кейсов в БД."""
    query = """
        INSERT INTO bridge_case_label (case_id, label_id)
        VALUES ($1, $2)
        ON CONFLICT (case_id, label_id) DO NOTHING;
    """
    values = [(case_id, label) for case_id, labels in labels_data for label in labels]
    if values:
        await conn.executemany(query, values)
