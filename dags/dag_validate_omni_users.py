import asyncpg
import asyncio
from airflow import DAG
from airflow.operators.python import PythonOperator


from config import DB_CONFIG, DAG_CONFIG
from queries import queries_select as qs
from dag_parse_omni_users import fetch_and_process_users

from functions.function_logging import setup_logger
logger = setup_logger('dag_validate_omni_users')


async def validate_and_fetch_users():
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        async with pool.acquire() as conn:
            logger.info('Начало валидации данных пользователей')
            from_time_array = await qs.select_missing_user_dates(conn)
            if from_time_array:
                for from_time in from_time_array:
                    await fetch_and_process_users(from_time=from_time, backfill=True)

                from_time_array_check = await qs.select_missing_user_dates(conn)
                if from_time_array_check:
                    logger.error('В пользователях не удалось забэкфилить все данные')
                    raise Exception('В пользователях не удалось забэкфилить все данные')
            logger.info('Успешная валидация пользователей')
            return


def run_async():
    asyncio.run(validate_and_fetch_users())


with DAG(
    'dag_validate_omni_users',
    default_args=DAG_CONFIG,
    catchup=False,
    schedule_interval=None,
) as dag:
    validate_users = PythonOperator(
        task_id='validate_omni_users',
        python_callable=run_async,
    )

    validate_users
