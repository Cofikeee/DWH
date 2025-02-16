async def refresh_datamarts(conn):
    query = """
            REFRESH MATERIALIZED VIEW mv_fact_omni_case_metrics;
            """
    await conn.execute(query)


async def update_users_linked(conn):
    query = """
            with t as (select omni_user_id,
                              omni_user_name,
                              min(omni_user_id) over (partition by omni_user_name, company_id, updated_date) as master_omni_user_id
                       from dim_omni_user
                       order by created_date)
            update dim_omni_user d set master_omni_user_id = t.master_omni_user_id 
            from t 
            where d.omni_user_id = t.omni_user_id;            
            """
    await conn.execute(query)
