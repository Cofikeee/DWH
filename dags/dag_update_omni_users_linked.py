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


async def update_and_fetch_omni_users_linked():
    """
    Асинхронная функция, которая кидает запрос к БД для обновления данных пользователей.
    Исполняется UPDATE запрос, который добавляет значение в поле master_omni_user_id,
    которое является общим для одного физического пользователя для нескольких omni_user_id.
    """
    logger = fl.setup_logger('dag_update_omni_users_linked')
    logger.info('--------------------------------------------')
    logger.info('Начало работы DAG dag_update_omni_users_linked')

    # Создаем асинхронные сессии для подключения к базе данных
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        # Получаем соединение с базой данных
        async with pool.acquire() as conn:
            logger.info('Начало обновления таблицы dim_omni_user.')
            await qu.update_users_linked(conn)
            logger.info('Значения master_omni_user_id в таблице dim_omni_user заполнены.')
            return


def run_async():
    """
    Запускает основную асинхронную функцию update_and_fetch_datamarts.
    """
    asyncio.run(update_and_fetch_omni_users_linked())


with DAG(
    'dag_update_omni_users_linked',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['omni']
) as dag:
    update_users_linked = PythonOperator(
        task_id='update_omni_users_linked',
        python_callable=run_async,
    )

    update_users_linked
