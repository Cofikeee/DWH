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
    collect_tenant_day_sms_nqes = TriggerDagRunOperator(
        task_id='trigger_dag_collect_tenant_day_sms_nqes',
        trigger_dag_id='dag_collect_tenant_day_sms_nqes',
        wait_for_completion=True,
        poke_interval=10  # Проверяет завершение каждые 10 секунд
    )

    end = EmptyOperator(task_id='end')

    # Задаем зависимости
    start >> collect_tenant_day_sms_nqes >> end
