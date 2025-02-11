# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import asyncio
import asyncpg
# Конфиг
from config import DB_CONFIG, DAG_CONFIG
# Запросы к бд
from queries import queries_select as qs
# Функции
from functions import function_logging as fl
# Импорт функции для парсинга обращений
from dag_parse_omni_cases import fetch_and_process_cases


async def validate_and_fetch_cases():
    """
    Асинхронная функция для валидации логов парсера для обращений (cases).

    Логика работы:
    1. Получает даты парсинга, которые не прошли валидацию, из логов в БД.
    2. Обрабатывает каждую такую дату, вызывая fetch_and_process_cases с флагом backfill=True.
    3. После окончания работы fetch_and_process_cases повторно проверяет даты в логах БД.
    4. Возвращает текущий статус для этих данных - успешная валидация / успешный бэкфил / ошибка в бэкфиле.
    """
    # Инициализация логгера
    logger = fl.setup_logger('dag_validate_omni_cases')
    logger.info('------------------------------------------')
    logger.info('Начало работы DAG validate_and_fetch_cases')

    # Создаем асинхронные сессии для подключения к БД
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        # Получаем соединение с БД
        async with pool.acquire() as conn:
            logger.info('Начало валидации данных обращений.')

            # Получаем список дат, которые не прошли валидацию
            from_time_array = await qs.select_missing_case_dates(conn)

            # Проверяем список и парсим данные при необходимости
            if from_time_array:
                logger.info(f'Получены даты для валидации: {from_time_array}.')
                for from_time in from_time_array:
                    await fetch_and_process_cases(from_time=from_time, backfill=True)

                # После вызова fetch_and_process_cases повторно проверяем данные
                from_time_array_check = await qs.select_missing_case_dates(conn)
                if from_time_array_check:
                    logger.error('В обращениях не удалось забэкфилить все данные.')
                    raise Exception('В обращениях не удалось забэкфилить все данные.')
                logger.info('Успешный бэкфил данных.')

            logger.info('Успешная валидация обращений.')
            return


def run_async():
    # Запуск основной асинхронной функции
    asyncio.run(validate_and_fetch_cases())


with DAG(
        'dag_validate_omni_cases',
        default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
        catchup=False,  # Не выполнять пропущенные интервалы
        schedule_interval=None,  # Не запускать автоматически
) as dag:
    validate_cases = PythonOperator(
        task_id='validate_omni_cases',
        python_callable=run_async,
    )

    validate_cases
