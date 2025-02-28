from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from config import DAG_CONFIG

# Определяем основной DAG
with DAG(dag_id='main_tenant_dag',
         default_args=DAG_CONFIG,
         schedule_interval='@daily',
         catchup=False,
         tags=['tenant', 'main']) as main_dag:

    start = EmptyOperator(task_id='start')

    # Триггеры для запуска DAG'ов

    # ОБХОДЧИКИ
    collect_ten_sms_day = TriggerDagRunOperator(
        task_id='trigger_dag_collect_ten_sms_day',
        trigger_dag_id='dag_collect_ten_sms_day',
        wait_for_completion=True,
        poke_interval=30  # Проверяет завершение каждые x секунд
    )

    collect_ten_signing_day = TriggerDagRunOperator(
        task_id='trigger_dag_collect_ten_signing_day',
        trigger_dag_id='dag_collect_ten_signing_day',
        wait_for_completion=True,
        poke_interval=30  # Проверяет завершение каждые x секунд
    )

    collect_ten_session_day = TriggerDagRunOperator(
        task_id='trigger_dag_collect_ten_session_day',
        trigger_dag_id='dag_collect_ten_session_day',
        wait_for_completion=True,
        poke_interval=30  # Проверяет завершение каждые x секунд
    )

    update_ten_datamarts = TriggerDagRunOperator(
        task_id='trigger_dag_update_ten_datamarts',
        trigger_dag_id='dag_update_ten_datamarts',
        wait_for_completion=True,
        poke_interval=10
    )

    end = EmptyOperator(task_id='end')

    # Задаем зависимости
    start >> collect_ten_sms_day >> collect_ten_signing_day >> collect_ten_session_day >> update_ten_datamarts >> end
