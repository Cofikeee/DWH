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
# Импорт функции для парсинга пользователей
from dag_parse_omni_users import fetch_and_process_users


async def validate_and_fetch_users():
    """
    Асинхронная функция для валидации логов парсера для пользователей (users).

    Логика работы:
    1. Получает даты парсинга, которые не прошли валидацию, из логов в БД.
    2. Обрабатывает каждую такую дату, вызывая fetch_and_process_users с флагом backfill=True.
    3. После окончания работы fetch_and_process_users повторно проверяет даты в логах БД.
    4. Возвращает текущий статус для этих данных - успешная валидация / успешный бэкфил / ошибка в бэкфиле.
    """
    # Инициализация логгера
    logger = fl.setup_logger('dag_validate_omni_users')
    logger.info('Начало валидации данных пользователей')

    # Создаем асинхронные сессии для подключения к БД
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        # Получаем соединение с БД
        async with pool.acquire() as conn:
            # Получаем список дат, которые не прошли валидацию
            from_time_array = await qs.select_missing_user_dates(conn)

            # Проверяем список и парсим данные при необходимости
            if from_time_array:
                logger.info(f'Получены даты для валидации: {from_time_array}')
                for from_time in from_time_array:
                    await fetch_and_process_users(from_time=from_time, backfill=True)

                from_time_array_check = await qs.select_missing_user_dates(conn)
                # После вызова fetch_and_process_users повторно проверяем данные
                if from_time_array_check:
                    logger.error('В пользователях не удалось забэкфилить все данные')
                    raise Exception('В пользователях не удалось забэкфилить все данные')
                logger.info('Успешный бэкфил данных')

            logger.info('Успешная валидация пользователей')
            return


def run_async():
    # Запуск основной асинхронной функции
    asyncio.run(validate_and_fetch_users())


with DAG(
    'dag_validate_omni_users',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
) as dag:
    validate_users = PythonOperator(
        task_id='validate_omni_users',
        python_callable=run_async,
    )

    validate_users
