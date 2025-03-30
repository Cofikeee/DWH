# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import asyncio
# Конфиг
from config import DAG_CONFIG
# Функции
from functions import functions_tenant as ft, function_logging as fl


async def main():
    """
    Основная асинхронная функция для выполнения DAG.
    """
    # Инициализация логгера
    logger = fl.setup_logger('dag_collect_ten_user_role')
    logger.info('-------------------------------------------')
    logger.info('Начало работы DAG dag_collect_ten_user_role')
    schema_name = 'dwh_dict'
    table_name = 'dim_user_role'
    await ft.crawler(logger=logger, schema_name=schema_name, table_name=table_name)


def run_async_func():
    """
    Запускает основную асинхронную функцию main.
    """
    asyncio.run(main())


# Создание DAG для Airflow
with DAG(
    'dag_collect_ten_user_role',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['dict']
) as dag:
    collect_user_role = PythonOperator(
        task_id='collect_ten_user_role',
        python_callable=run_async_func,
    )

    collect_user_role
