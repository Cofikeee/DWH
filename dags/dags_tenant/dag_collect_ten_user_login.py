# Airflow
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import asyncio
# Конфиг
from config import DAG_CONFIG
# Функции
from functions import functions_tenant as ft


async def main():
    """
    Основная асинхронная функция для выполнения DAG.
    """
    # Инициализация логгера
    logger = logging
    # logger = fl.setup_logger('dag_collect_ten_user_login')
    logger.info('--------------------------------------')
    logger.info('Начало работы DAG dag_collect_ten_user_login')
    schema_name = 'dwh_dict'
    table_name = 'dim_user_login_new'
    await ft.crawler(logger=logger, schema_name=schema_name, table_name=table_name)

if __name__ == "__main__":
    asyncio.run(main())


def run_async_func():
    """
    Запускает основную асинхронную функцию main.
    """
    asyncio.run(main())


# Создание DAG для Airflow
with DAG(
    'dag_collect_ten_user_login',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['dict']
) as dag:
    collect_user_login = PythonOperator(
        task_id='collect_ten_user_login',
        python_callable=run_async_func,
    )

    collect_user_login
