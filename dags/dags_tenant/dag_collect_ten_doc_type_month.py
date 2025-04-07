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
    logger = fl.setup_logger('dag_collect_ten_doc_type_month')
    logger.info('------------------------------------------------')
    logger.info('Начало работы DAG dag_collect_ten_doc_type_month')
    schema_name = 'dwh_ten'
    table_name = 'agg_e_doc_type_m'
    await ft.crawler(logger=logger, schema_name=schema_name, table_name=table_name, copy=True)


def run_async_func():
    """
    Запускает основную асинхронную функцию main.
    """
    asyncio.run(main())


# Создание DAG для Airflow
with DAG(
    'dag_collect_ten_doc_type_month',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['tenant']
) as dag:
    collect_ten_doc_type_month_task = PythonOperator(
        task_id='collect_ten_doc_type_month',
        python_callable=run_async_func,
    )

    collect_ten_doc_type_month_task
