# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# Прочие библиотеки
import aiohttp
import asyncio
import asyncpg
# Конфиг
from config import DB_CONFIG, OMNI_URL, OMNI_LOGIN, OMNI_PASSWORD, DAG_CONFIG
# Классы
from classes.omni_group import OmniGroup
# Запросы к БД
from queries import queries_log as ql, queries_insert as qi
# Функции
from functions import functions_general as fg
from functions.function_logging import setup_logger


async def fetch_and_process_groups():
    """
    Асинхронная функция для извлечения и обработки данных о группах из API Omni.
    Логика работы:
    1. Извлекает данные страницами из API Omni.
    2. Обрабатывает каждую запись с использованием класса OmniGroup.
    3. Вставляет обработанные данные в базу данных.
    4. Логирует процесс извлечения и обработки данных.
    """
    # Инициализация логгера
    logger = setup_logger('dag_parse_omni_groups')
    logger.info('---------------------------------------')
    logger.info('Начало работы DAG dag_parse_omni_groups')

    # Создаем асинхронные сессии для HTTP-запросов и подключения к БД
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        # Получаем соединение с БД
        async with pool.acquire() as conn:
            # Получаем общее количество групп
            total_count = await fg.get_snapshot(session, 'groups')

            page = 1
            batch_size = 5  # Размер пакета страниц для параллельной обработки
            while True:
                # Очищаем список для хранения групп текущего пакета
                batch_groups = []

                # Переходим к параллельной обработке
                for i in range(batch_size):
                    # URL для запроса страницы
                    url = f'{OMNI_URL}/groups.json?page={page}&limit=100'
                    data = await fg.fetch_response(session, url)

                    # Проверяем полученные данные
                    if not data or len(data) <= 1:
                        logger.info("Все данные обработаны.")
                        return

                    # Определяем общее количество записей и страниц для текущего периода
                    if page == 1:
                        period_total = int(data.get("total_count", 0))
                        period_pages = (period_total + 99) // 100

                    # Очищаем список данных на текущей странице
                    groups_data = []
                    for item in data.values():
                        if isinstance(item, dict) and "group" in item:
                            group = OmniGroup(item["group"])
                            processed_group = group.group_properties()
                            if processed_group:
                                groups_data.append(processed_group)

                    # Добавляем обработанные записи в пакет
                    batch_groups.extend(groups_data)

                    # Переходим к следующей странице или завершаем обработку текущего периода, если страница последняя
                    page += 1
                    if page > period_pages:
                        break

                # Вставка данных в БД
                if batch_groups:
                    await qi.insert_groups(conn, batch_groups)

                # Логируем завершение обработки текущего пакета
                logger.info(f'Собраны данные за пакет страниц ({page-1}/{period_pages}).')

                if page > period_pages:
                    # Передаем в БД снэпшот количества компаний для валидации в дальнейшем
                    await ql.log_etl_catalogues(conn, 'dim_omni_group', total_count)
                    logger.info(f'Собраны все данные по компаниям.')
                    return


def run_async_func():
    """
    Запускает основную асинхронную функцию fetch_and_process_groups.
    """
    asyncio.run(fetch_and_process_groups())


# Создание DAG для Airflow
with DAG(
    'dag_parse_omni_groups',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
) as dag:
    fetch_groups_task = PythonOperator(
        task_id='parse_omni_groups',
        python_callable=run_async_func,
    )

    fetch_groups_task
