# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import asyncio
import asyncpg
# Конфиг
from config import DB_CONFIG, DAG_CONFIG
# Запросы к БД
from queries import queries_select as qs
# Функции
from functions import function_logging as fl
# Импорты функций для обработки каталогов
from dags_omni.dag_parse_omni_companies import fetch_and_process_companies
from dags_omni.dag_parse_omni_groups import fetch_and_process_groups
from dags_omni.dag_parse_omni_labels import fetch_and_process_labels
from dags_omni.dag_parse_omni_staff import fetch_and_process_staff
from dags_omni.dag_parse_omni_custom_fields import fetch_and_process_custom_fields


async def validate_catalogues():
    """
    Основной процесс проверки и обработки отсутствующих данных в каталогах.
    Логика работы:
    1. Получает список отсутствующих каталогов из базы данных.
    2. Вызывает соответствующие функции для обработки каждого каталога.
    3. Проверяет, остались ли необработанные каталоги после выполнения.
    4. Возвращает текущий статус для каталогов - успешная валидация / успешный бэкфил / ошибка в бэкфиле.
    """
    # Инициализация логгера
    logger = fl.setup_logger('dag_validate_omni_catalogues')
    logger.info('----------------------------------------------')
    logger.info('Начало работы DAG dag_validate_omni_catalogues')

    async with asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        async with pool.acquire() as conn:
            # Получаем список отсутствующих каталогов
            catalogues_array = await qs.select_missing_catalogues(conn)
            if catalogues_array:
                logger.info(f'Обнаружены пропуски в каталогах: {catalogues_array}')

                # Обрабатываем каждый каталог в зависимости от его типа
                if 'dim_omni_company' in catalogues_array:
                    logger.info('Получены компании для валидации.')
                    await fetch_and_process_companies()
                if 'dim_omni_group' in catalogues_array:
                    logger.info('Получены группы для валидации.')
                    await fetch_and_process_groups()
                if 'dim_omni_label' in catalogues_array:
                    logger.info('Получены метки для валидации.')
                    await fetch_and_process_labels()
                if 'dim_omni_staff' in catalogues_array:
                    logger.info('Получены сотрудники для валидации.')
                    await fetch_and_process_staff()
                if 'lookup_omni_custom_field' in catalogues_array:
                    logger.info('Получены пользовательские полея для валидации.')
                    await fetch_and_process_custom_fields()

                # Проверяем, остались ли необработанные каталоги
                catalogues_array = await qs.select_missing_catalogues(conn)
                if catalogues_array:
                    logger.error(f'Не удалось забэкфилить все данные. Остались каталоги: {catalogues_array}.')
                    raise Exception('В каталогах не удалось забэкфилить все данные')

            logger.info('Успешная проверка каталогов.')
            return


def run_async():
    """
    Запускает основную асинхронную функцию validate_catalogues.
    """
    asyncio.run(validate_catalogues())


# Создание DAG для Airflow
with DAG(
    'dag_validate_omni_catalogues',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,  # Не выполнять пропущенные интервалы
    schedule_interval=None,  # Не запускать автоматически
    tags=['omni']
) as dag:
    fetch_validate_task = PythonOperator(
        task_id='validate_omni_catalogues',
        python_callable=run_async,
    )

    fetch_validate_task
