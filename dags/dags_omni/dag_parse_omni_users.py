# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
from dateutil.relativedelta import relativedelta
import aiohttp
import asyncio
import asyncpg
# Конфиг
from config import OMNI_DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
# Классы
from classes.omni_user import OmniUser
# Запросы к БД
from queries import queries_select as qs, queries_log as ql, queries_insert as qi
# Функции
from functions import functions_general as fg, functions_data as fd, function_logging as fl


async def fetch_and_process_users(from_time=qs.select_max_ts('dim_omni_user', 'updated_date'), backfill=False):
    """
    Асинхронная функция для извлечения и обработки пользователей из API Omni.

    Параметры:
    - from_time (datetime): Начальная дата для извлечения данных, default=максимальный updated_date из таблицы в БД.
    - backfill (bool): Флаг для выполнения обратной загрузки данных (backfill), default=False.

    Логика работы:
    1. Извлекает данные страницами из API Omni.
    2. Обрабатывает каждую запись с использованием класса OmniUser.
    3. Вставляет обработанные данные в БД.
    4. Логирует процесс извлечения и обработки данных.
    """
    page = 1
    period_pages = 0
    batch_size = 5  # Размер пакета страниц для параллельной обработки
    to_time = fg.next_day(from_time)  # Устанавливаем конечную дату для текущего периода (00:00 следующего дня)

    # Инициализация логгера
    logger = fl.setup_logger('dag_parse_omni_users')
    logger.info('--------------------------------------')
    logger.info('Начало работы DAG dag_parse_omni_users')

    # Создаем асинхронные сессии для HTTP-запросов и подключения к БД
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**OMNI_DB_CONFIG, min_size=5, max_size=20) as pool:
        # Получаем соединение с БД
        async with pool.acquire() as conn:
            while True:
                # Выходим из цикла, если достигли сегодняшнего дня
                if from_time >= fg.get_today():
                    logger.info("Дошли до сегодняшнего дня.")
                    return

                # Очищаем список для хранения пользователей текущего периода
                batch_users = []
                # Переходим к параллельной обработке
                for i in range(batch_size):
                    # URL для запроса страницы
                    url = f"{OMNI_URL}/users.json?from_updated_time={from_time}&to_updated_time={to_time}&page={page}&limit=100"
                    data = await fg.fetch_response(session, url)
                    # Проверяем полученные данные
                    if not data or len(data) <= 1:
                        if from_time == qs.select_max_ts('dim_omni_user', 'updated_date'):
                            logger.info(f'Нет данных за период {from_time} - {to_time}, страница {page}')
                            break
                        logger.error('Получили неожиданный результат - пустую страницу.')
                        raise Exception('Получили неожиданный результат - пустую страницу.')

                    # Определяем общее количество записей и страниц для текущего периода
                    if page == 1:
                        period_total = int(data.get("total_count", 0))
                        period_pages = (period_total + 99) // 100

                    # Если в периоде больше 500 страниц, делаем to_time = последнее значение на 500ой странице - 1 сек
                    if period_pages > 500:
                        last_page_url = f"{OMNI_URL}/users.json?from_updated_time={from_time}&to_updated_time={to_time}&page=500&limit=100"
                        last_page_data = await fg.fetch_response(session, last_page_url)
                        last_page_record = last_page_data["99"]["user"]["updated_at"]
                        to_time = fd.fix_datetime(last_page_record) - relativedelta(seconds=1)
                        break

                    # Очищаем список данных на текущей странице
                    users_data = []
                    # Обработка данных на текущей странице
                    for item in data.values():
                        if isinstance(item, dict) and "user" in item:
                            user = OmniUser(item["user"])
                            processed_user = user.user_properties()
                            if processed_user:
                                users_data.append(processed_user)

                    # Добавляем обработанные записи в пакет
                    batch_users.extend(users_data)

                    # Логирование текущей страницы
                    await ql.log_etl_users(
                        conn, from_time, to_time, page, len(users_data), period_total
                    )

                    # Переходим к следующей странице или завершаем обработку текущего периода, если страница последняя
                    page += 1
                    if page > period_pages:
                        break

                # Если изначально вернулось больше 500 страниц, перезапускаем цикл с меньшим периодом
                if period_pages > 500:
                    continue

                # Вставка данных в БД
                if batch_users:
                    await qi.insert_users(conn, batch_users)

                # Обновляем временной диапазон для следующего периода, если страница последняя
                if page > period_pages:
                    if backfill:  # Для бэкфилла логика другая, так как в бэкфилле передаются нужные from_time
                        logger.info(f'Забэкфилили пропуски {from_time} - {to_time}.')
                        return
                    logger.info(f'Собраны данные за период {from_time} - {to_time}.')
                    from_time = to_time
                    to_time = fg.next_day(from_time)
                    page = 1


def run_async():
    """
    Запускает основную асинхронную функцию fetch_and_process_users.
    """
    asyncio.run(fetch_and_process_users())


# Создание DAG для Airflow
with DAG(
    'dag_parse_omni_users',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['omni']
) as dag:
    fetch_users_task = PythonOperator(
        task_id='parse_omni_users',
        python_callable=run_async,
    )

    # Добавление задачи в DAG
    fetch_users_task
