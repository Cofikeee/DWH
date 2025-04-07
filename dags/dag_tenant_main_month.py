from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from config import DAG_CONFIG

# Определяем основной DAG
with DAG(dag_id='main_tenant_dag_m',
         default_args=DAG_CONFIG,
         schedule_interval='@monthly',
         catchup=False,
         tags=['tenant', 'main']) as main_dag:

    start = EmptyOperator(task_id='start')

    # Триггеры для запуска DAG'ов

    # ОБХОДЧИКИ АГРЕГАЦИИ
    collect_ten_app_type_month = TriggerDagRunOperator(
        task_id='trigger_dag_collect_ten_app_type_month',
        trigger_dag_id='dag_collect_ten_app_type_month',
        wait_for_completion=True,
        poke_interval=20  # Проверяет завершение каждые x секунд
    )

    collect_ten_doc_type_month = TriggerDagRunOperator(
        task_id='trigger_dag_collect_ten_doc_type_month',
        trigger_dag_id='dag_collect_ten_doc_type_month',
        wait_for_completion=True,
        poke_interval=20  # Проверяет завершение каждые x секунд
    )

    end = EmptyOperator(task_id='end')

    # Задаем зависимости
    start >> collect_ten_app_type_month >> collect_ten_doc_type_month >> end
