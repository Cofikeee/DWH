from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from config import DAG_CONFIG


# Определяем основной DAG
with DAG(dag_id='main_tenant_dag_d',
         default_args=DAG_CONFIG,
         schedule_interval='@daily',
         catchup=False,
         tags=['tenant', 'main']) as main_dag:

    start = EmptyOperator(task_id='start')

    # Триггеры для запуска DAG'ов

    with TaskGroup('ten_dicts') as ten_dicts:
        # ОБХОДЧИКИ СЛОВАРИ
        collect_ten_user_role = TriggerDagRunOperator(
            task_id='trigger_dag_collect_ten_user_role',
            trigger_dag_id='dag_collect_ten_user_role',
            wait_for_completion=True,
            poke_interval=20  # Проверяет завершение каждые x секунд
        )

        collect_ten_user_login = TriggerDagRunOperator(
            task_id='trigger_dag_collect_ten_user_login',
            trigger_dag_id='dag_collect_ten_user_login',
            wait_for_completion=True,
            poke_interval=20  # Проверяет завершение каждые x секунд
        )
        collect_ten_user_role >> collect_ten_user_login


    with TaskGroup('ten_aggs') as ten_aggs:
    # ОБХОДЧИКИ АГРЕГАЦИИ
        collect_ten_sms_day = TriggerDagRunOperator(
            task_id='trigger_dag_collect_ten_sms_day',
            trigger_dag_id='dag_collect_ten_sms_day',
            wait_for_completion=True,
            poke_interval=20  # Проверяет завершение каждые x секунд
        )

        collect_ten_signing_day = TriggerDagRunOperator(
            task_id='trigger_dag_collect_ten_signing_day',
            trigger_dag_id='dag_collect_ten_signing_day',
            wait_for_completion=True,
            poke_interval=20  # Проверяет завершение каждые x секунд
        )

        collect_ten_session_day = TriggerDagRunOperator(
            task_id='trigger_dag_collect_ten_session_day',
            trigger_dag_id='dag_collect_ten_session_day',
            wait_for_completion=True,
            poke_interval=20  # Проверяет завершение каждые x секунд
        )
        collect_ten_sms_day >> collect_ten_signing_day >> collect_ten_session_day

    update_ten_datamarts = TriggerDagRunOperator(
        task_id='trigger_dag_update_ten_datamarts',
        trigger_dag_id='dag_update_ten_datamarts',
        wait_for_completion=True,
        poke_interval=20
    )

    end = EmptyOperator(task_id='end')

    # Задаем зависимости
    start >> ten_dicts >> ten_aggs >> update_ten_datamarts >> end
