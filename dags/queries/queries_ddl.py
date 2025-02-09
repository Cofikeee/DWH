async def refresh_datamarts(conn):
    query = """
            REFRESH MATERIALIZED VIEW mv_fact_omni_case_metrics;
            """
    await conn.execute(query)
