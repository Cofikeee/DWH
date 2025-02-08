import asyncio
import asyncpg
from airflow import DAG
from airflow.operators.python import PythonOperator

from config import DB_CONFIG, DAG_CONFIG
from queries import queries_log as ql, queries_select as qs

from dag_parse_omni_companies import fetch_and_process_companies
from dag_parse_omni_groups import fetch_and_process_groups
from dag_parse_omni_labels import fetch_and_process_labels
from dag_parse_omni_staff import fetch_and_process_staff
from dag_parse_omni_custom_fields import fetch_and_process_custom_fields


async def validate_catalogues():
    async with asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        async with pool.acquire() as conn:
            catalogues_array = await qs.select_missing_catalogues(conn)
            if catalogues_array:
                if 'dim_omni_company' in catalogues_array:
                    await fetch_and_process_companies()
                if 'dim_omni_group' in catalogues_array:
                    await fetch_and_process_groups()
                if 'dim_omni_label' in catalogues_array:
                    await fetch_and_process_labels()
                if 'dim_omni_staff' in catalogues_array:
                    await fetch_and_process_staff()
                if 'lookup_omni_custom_field' in catalogues_array:
                    await fetch_and_process_custom_fields()

                catalogues_array = await qs.select_missing_catalogues(conn)
                if catalogues_array:
                    raise Exception('В каталогах не удалось забэкфилить все данные')

            print('Успешная проверка каталогов')
            return


def run_async():
    asyncio.run(validate_catalogues())


with DAG(
        'dag_validate_omni_catalogues',
        default_args=DAG_CONFIG,
        catchup=False,
        schedule_interval=None,
) as dag:
    fetch_validate_task = PythonOperator(
        task_id='validate_omni_catalogues.py',
        python_callable=run_async,
    )
    fetch_validate_task
