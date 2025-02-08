from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from config import DAG_CONFIG


# Определяем основной DAG
with DAG(dag_id='_main_dag',
         default_args=DAG_CONFIG,
         schedule_interval='@daily',
         catchup=False) as main_dag:

    start = EmptyOperator(task_id='start')

    # Триггеры для запуска других DAG'ов в порядке
    parse_custom_fields = TriggerDagRunOperator(
        task_id='trigger_dag_parse_omni_custom_fields',
        trigger_dag_id='dag_parse_omni_custom_fields',
        wait_for_completion=True,
        poke_interval=5  # Проверяет завершение каждые 5 секунд

    )

    parse_groups = TriggerDagRunOperator(
        task_id='trigger_dag_parse_omni_groups',
        trigger_dag_id='dag_parse_omni_groups',
        wait_for_completion=True,
        poke_interval=5
    )

    parse_staff = TriggerDagRunOperator(
        task_id='trigger_dag_parse_omni_staff',
        trigger_dag_id='dag_parse_omni_staff',
        wait_for_completion=True,
        poke_interval=5
    )

    parse_labels = TriggerDagRunOperator(
        task_id='trigger_dag_parse_omni_labels',
        trigger_dag_id='dag_parse_omni_labels',
        wait_for_completion=True,
        poke_interval=5
    )

    parse_companies = TriggerDagRunOperator(
        task_id='trigger_dag_parse_omni_companies',
        trigger_dag_id='dag_parse_omni_companies',
        wait_for_completion=True,
        poke_interval=5
    )

    parse_users = TriggerDagRunOperator(
        task_id='trigger_dag_parse_omni_users',
        trigger_dag_id='dag_parse_omni_users',
        wait_for_completion=True,
        poke_interval=10
    )

    parse_cases = TriggerDagRunOperator(
        task_id='trigger_dag_parse_omni_cases',
        trigger_dag_id='dag_parse_omni_cases',
        wait_for_completion=True,
        poke_interval=20
    )

    parse_messages = TriggerDagRunOperator(
        task_id='trigger_dag_parse_omni_messages',
        trigger_dag_id='dag_parse_omni_messages',
        wait_for_completion=True,
        poke_interval=60
    )

    validate_catalogues = TriggerDagRunOperator(
        task_id='trigger_dag_validate_omni_catalogues',
        trigger_dag_id='dag_validate_omni_catalogues',
        wait_for_completion=True,
        poke_interval=5
    )

    validate_cases = TriggerDagRunOperator(
        task_id='trigger_dag_validate_omni_cases',
        trigger_dag_id='dag_validate_omni_cases',
        wait_for_completion=True,
        poke_interval=20

    )

    validate_messages = TriggerDagRunOperator(
        task_id='trigger_dag_validate_omni_messages',
        trigger_dag_id='dag_validate_omni_messages',
        wait_for_completion=True,
        poke_interval=20

    )
    end = EmptyOperator(task_id='end')


    # Задаем зависимости
    parse_catalogues = [parse_labels, parse_groups, parse_staff, parse_custom_fields, parse_companies]

    start >> parse_catalogues >> validate_catalogues >> parse_users >> parse_cases

    parse_cases >> validate_cases >> parse_messages >> validate_messages >> end

