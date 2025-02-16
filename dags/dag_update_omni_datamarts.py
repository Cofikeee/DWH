# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import asyncio
import asyncpg
# Конфиг
from config import DB_CONFIG, DAG_CONFIG
# Запросы к бд
from queries import queries_update as qu
# Функции
from functions import function_logging as fl


async def update_and_fetch_datamarts():
    """
    Асинхронная функция, которая кидает запрос к БД для обновления витрин данных.
    """
    logger = fl.setup_logger('dag_update_omni_datamarts')
    logger.info('--------------------------------------------')
    logger.info('Начало работы DAG update_and_fetch_datamarts')

    # Создаем асинхронные сессии для подключения к базе данных
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        # Получаем соединение с базой данных
        async with pool.acquire() as conn:
            logger.info('Начало обновления витрин данных.')
            await qu.refresh_datamarts(conn)
            logger.info('Витрины данных обновлены.')
            return


def run_async():
    """
    Запускает основную асинхронную функцию update_and_fetch_datamarts.
    """
    asyncio.run(update_and_fetch_datamarts())


with DAG(
    'dag_update_omni_datamarts',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
) as dag:
    update_datamarts = PythonOperator(
        task_id='update_omni_datamarts',
        python_callable=run_async,
    )

    update_datamarts
