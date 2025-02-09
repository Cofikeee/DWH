# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
from dateutil.relativedelta import relativedelta
import aiohttp
import asyncio
import asyncpg
# Конфиг
from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
# Классы
from classes.omni_case import OmniCase
# Запросы к бд
from queries import queries_select as qs, queries_log as ql, queries_insert as qi
# Функции
from functions import functions_general as fg, functions_data as fd, function_logging as fl



async def fetch_and_process_cases(from_time=qs.select_max_ts('fact_omni_case'), backfill=False):
    """
    Асинхронная функция для извлечения и обработки обращений (cases) из API Omni.

    Параметры:
    - from_time (datetime): Начальная дата для извлечения данных, default=максимальный updated_date из таблицы в бд.
    - backfill (bool): Флаг для выполнения обратной загрузки данных (backfill), default=False.

    Логика работы:
    1. Извлекает данные страницами из API Omni.
    2. Обрабатывает каждую запись с использованием класса OmniCase.
    3. Вставляет обработанные данные в базу данных.
    4. Логирует процесс извлечения и обработки данных.
    """
    page = 1
    batch_size = 5  # Размер пакета страниц для параллельной обработки
    to_time = fg.next_day(from_time)  # Устанавливаем конечную дату для текущего периода (00:00 следующего дня)

    # Инициализация логгера
    logger = fl.setup_logger('dag_parse_omni_cases')
    logger.info('Начало работы DAG dag_parse_omni_cases')

    # Создаем асинхронные сессии для HTTP-запросов и подключения к базе данных
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        # Получаем соединение с базой данных
        async with pool.acquire() as conn:
            while True:
                # Выходим из цикла, если достигли сегодняшнего дня
                if from_time >= fg.get_today():
                    logger.info("Дошли до сегодняшнего дня.")
                    return

                # Очищаем списки хранения обращений и меток текущего периода
                batch_cases, batch_labels = [], []
                # Переходим к параллельной обработке
                for i in range(batch_size):
                    # URL для запроса страницы
                    url = f"{OMNI_URL}/cases.json?from_updated_time={from_time}&to_updated_time={to_time}&page={page}&limit=100&sort=updated_at_asc"
                    data = await fg.fetch_response(session, url)

                    # Проверяем полученные данные
                    if not data or len(data) <= 1:
                        if from_time == qs.select_max_ts('fact_omni_case'):
                            logger.info(f'Нет данных за период {from_time} - {to_time}, страница {page}')
                            return
                        logger.error('Получили неожиданный результат - пустую страницу.')
                        raise Exception('Получили неожиданный результат - пустую страницу.')

                    # Определяем общее количество записей и страниц для текущего периода
                    if page == 1:
                        period_total = data.get("total_count", 0)
                        period_pages = (period_total + 99) // 100

                    # Если в периоде больше 500 страниц, делаем to_time = последнее значение на 500-й странице - 1 секунда
                    if period_pages > 500:
                        last_page_url = f"{OMNI_URL}/cases.json?from_updated_time={from_time}&to_updated_time={to_time}&page=500&limit=100&sort=updated_at_asc"
                        last_page_data = await fg.fetch_response(session, last_page_url)
                        last_page_record = last_page_data["99"]["case"]["updated_at"]
                        to_time = fd.fix_datetime(last_page_record) - relativedelta(seconds=1)
                        break

                    # Очищаем список данных на текущей странице и счётчик блэклиста
                    cases_data = []
                    blacklisted_cases = 0
                    # Обработка данных на текущей странице
                    for item in data.values():
                        if isinstance(item, dict) and "case" in item:
                            case = OmniCase(item["case"])
                            processed_case = (case.case_properties())
                            if processed_case:
                                cases_data.append(processed_case)
                            else:
                                blacklisted_cases += 1

                    # Добавляем обработанные записи в пакеты
                    batch_cases.extend([case[:-1] for case in cases_data])  # Обращения без меток
                    batch_labels.extend([(case[0], case[-1]) for case in cases_data])  # Добавляем метки

                    # Логирование текущей страницы
                    await ql.log_etl_cases(
                        conn, from_time, to_time, page, len(cases_data),
                        blacklisted_cases, period_total
                    )

                    # Переходим к следующей странице или завершаем обработку текущего периода, если страница последняя
                    page += 1
                    if page > period_pages:
                        break

                # Если изначально вернулось больше 500 страниц, перезапускаем цикл с меньшим периодом
                if period_pages > 500:
                    continue

                # Вставка данных в базу данных
                if batch_cases:
                    await qi.insert_cases(conn, batch_cases)
                    await qi.insert_case_labels(conn, batch_labels)

                # Обновляем временной диапазон для следующего периода, если страница последняя
                if page > period_pages:
                    if backfill:
                        logger.info(f'Забэкфилили пропуски {from_time} - {to_time}')
                        return
                    logger.info(f'Собраны данные за период {from_time} - {to_time}')
                    from_time = to_time
                    to_time = (from_time + relativedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                    page = 1


def run_async():
    # Запуск основной асинхронной функции
    asyncio.run(fetch_and_process_cases())


with DAG(
    'dag_parse_omni_cases',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
) as dag:
    fetch_cases_task = PythonOperator(
        task_id='parse_omni_cases',
        python_callable=run_async,
    )

    fetch_cases_task
