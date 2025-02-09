import asyncpg
import asyncio
from airflow import DAG
from airflow.operators.python import PythonOperator


from config import DB_CONFIG, DAG_CONFIG
from queries import queries_ddl as qd

from functions.function_logging import setup_logger
logger = setup_logger('dag_update_omni_datamarts')


async def update_and_fetch_datamarts():
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        async with pool.acquire() as conn:
            logger.info('Начало обновления материализованных витрин данных')
            await qd.refresh_datamarts(conn)
            logger.info('Материализованные витрины данных обновлены')
            return


def run_async():
    asyncio.run(update_and_fetch_datamarts())


with DAG(
    'dag_update_omni_datamarts',
    default_args=DAG_CONFIG,
    catchup=False,
    schedule_interval=None,
) as dag:
    update_datamarts = PythonOperator(
        task_id='update_omni_datamarts',
        python_callable=run_async,
    )

    update_datamarts
