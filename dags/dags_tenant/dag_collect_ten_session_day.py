# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import asyncio
# Конфиг
from config import DAG_CONFIG
# Функции
from functions import function_logging as fl, functions_tenant as ft


async def main():
    """
    Основная асинхронная функция для выполнения DAG.
    """
    # Инициализация логгера
    import logging
    logger = logging
    #logger = fl.setup_logger('dag_collect_ten_session_day')
    logger.info('---------------------------------------------')
    logger.info('Начало работы DAG dag_collect_ten_session_day')
    schema_name = 'dwh_ten'
    table_name = 'agg_s_session_d'
    await ft.crawler(logger=logger, schema_name=schema_name, table_name=table_name, copy=True)


if __name__ == '__main__':
    asyncio.run(main())



def run_async_func():
    """
    Запускает основную асинхронную функцию main.
    """
    asyncio.run(main())


# Создание DAG для Airflow
with DAG(
    'dag_collect_ten_session_day',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['tenant']
) as dag:
    collect_ten_session_task = PythonOperator(
        task_id='collect_ten_session_day',
        python_callable=run_async_func,
    )

    collect_ten_session_task
