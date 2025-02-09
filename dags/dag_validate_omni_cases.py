import asyncpg
import asyncio
from airflow import DAG
from airflow.operators.python import PythonOperator

from queries import queries_select as qs
from config import DB_CONFIG, DAG_CONFIG
from dag_parse_omni_cases import fetch_and_process_cases


async def validate_and_fetch_cases():
    async with asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        async with pool.acquire() as conn:
            from_time_array = await qs.select_missing_case_dates(conn)
            if from_time_array:
                for from_time in from_time_array:
                    await fetch_and_process_cases(from_time=from_time, backfill=True)

                from_time_array_check = await qs.select_missing_case_dates(conn)
                if from_time_array_check:
                    raise Exception('В обращениях не удалось забэкфилить все данные')

            print('Успешная проверка обращений')
            return


def run_async():
    asyncio.run(validate_and_fetch_cases())


with DAG(
    'dag_validate_omni_cases',
    default_args=DAG_CONFIG,
    catchup=False,
    schedule_interval=None,
) as dag:
    validate_cases = PythonOperator(
        task_id='validate_omni_cases',
        python_callable=run_async,
    )

    validate_cases
