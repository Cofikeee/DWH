async def refresh_omni_datamarts(conn):
    query = """
            REFRESH MATERIALIZED VIEW dwh_omni.mv_agg_omni_case_full;
            REFRESH MATERIALIZED VIEW dwh_omni.mv_agg_omni_staff_message_m;
            """
    await conn.execute(query)


async def refresh_ten_datamarts(conn):
    query = """
            REFRESH MATERIALIZED VIEW dwh_ten.mv_person_sms_full_m;
            REFRESH MATERIALIZED VIEW dwh_ten.mv_sms_full_m;
            """
    await conn.execute(query)


async def update_users_linked(conn):
    query = """
            with t as (select omni_user_id,
                              omni_user_name,
                              min(omni_user_id) over (partition by omni_user_name, company_id, updated_date) as master_omni_user_id
                       from dwh_omni.dim_omni_user
                       order by created_date)
            update dwh_omni.dim_omni_user d set master_omni_user_id = t.master_omni_user_id 
            from t 
            where d.omni_user_id = t.omni_user_id;            
            """
    await conn.execute(query)
