import asyncpg
import asyncio
from airflow import DAG
from airflow.operators.python import PythonOperator


from config import DB_CONFIG, DAG_CONFIG
from queries import queries_select as qs

from functions.function_logging import setup_logger
logger = setup_logger('dag_validate_omni_users')


async def validate_and_fetch_users():
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        async with pool.acquire() as conn:
            logger.info('Начало валидации данных пользователей')

            logger.info('')
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
