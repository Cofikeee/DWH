# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Прочие библиотеки
import asyncio
import os
from pathlib import Path
# Конфиг
from config import DAG_CONFIG
# Функции
from functions import functions_tenant as ft, function_logging as fl


def read_sql_file(filename):
    """Чтение SQL-файла с правильным определением пути"""
    dag_path = Path(os.path.dirname(os.path.abspath(__file__))).parent
    sql_path = dag_path / "sql" / filename

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    return sql_path.read_text(encoding='utf-8')


async def main():
    """
    Основная асинхронная функция для выполнения DAG.
    """
    # Инициализация логгера
    logger = fl.setup_logger('dag_collect_ten_user_login')
    logger.info('--------------------------------------------')
    logger.info('Начало работы DAG dag_collect_ten_user_login')
    schema_name = 'dwh_dict'
    table_name = 'dim_user_login_scd2_staging'
    await ft.crawler(logger=logger, schema_name=schema_name, table_name=table_name, copy=True)


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
    create_staging_user_login = SQLExecuteQueryOperator(
        task_id='create_staging_user_login',
        conn_id='reaper_connection',
        sql="""
        DROP TABLE IF EXISTS dwh_dict.dim_user_login_scd2_staging;
        CREATE TABLE dwh_dict.dim_user_login_scd2_staging (LIKE dwh_dict.dim_user_login_scd2 INCLUDING ALL);
        """
    )

    collect_user_login = PythonOperator(
        task_id='collect_user_login',
        python_callable=run_async_func,
    )

    insert_user_login = SQLExecuteQueryOperator(
        task_id='insert_user_login',
        conn_id='reaper_connection',
            sql=read_sql_file('insert_dim_user_login_scd2.sql')
    )

    delete_staging_user_login = SQLExecuteQueryOperator(
        task_id='delete_staging_user_login',
        conn_id='reaper_connection',
        sql="""
        DROP TABLE IF EXISTS dwh_dict.dim_user_login_scd2_staging;
        """
    )

    create_staging_user_login >> collect_user_login >> insert_user_login >> delete_staging_user_login


