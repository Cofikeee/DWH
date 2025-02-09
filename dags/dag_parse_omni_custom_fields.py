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
from classes.omni_custom_field import OmniCustomField
# Запросы к БД
from queries import queries_log as ql, queries_insert as qi
# Функции
from functions import functions_general as fg, function_logging as fl


async def insert_into_all_dimensions(conn, logger):
    """
    Асинхронная функция для передачи данных в таблицы-словари, в которых используются пользовательские поля.
    Логика работы:
    1. Получает данные из таблицы lookup_omni_custom_field по field_id.
    2. Создает индивидуальные записи в dim_ таблицах.
    """
    logger.info('Начало передачи данных в таблицы-словари.')

    await qi.insert_dimension(conn, 'dim_omni_category', '6998', 'category_id', 'category_name')
    logger.info('Переданы данные в dim_omni_category.')

    await qi.insert_dimension(conn, 'dim_omni_block', '4605', 'block_id', 'block_name')
    logger.info('Переданы данные в dim_omni_block.')

    await qi.insert_dimension(conn, 'dim_omni_topic', '8497', 'topic_id', 'topic_name')
    logger.info('Переданы данные в dim_omni_topic.')

    await qi.insert_dimension(conn, 'dim_omni_task', '9129', 'task_id', 'task_name')
    logger.info('Переданы данные в dim_omni_task.')



async def fetch_and_process_custom_fields():
    """
    Асинхронная функция для извлечения и обработки данных о пользовательских полях из API Omni.
    Логика работы:
    1. Извлекает данные страницами из API Omni.
    2. Обрабатывает каждую запись с использованием класса OmniCustomField.
    3. Вставляет обработанные данные в базу данных.
    4. Логирует процесс извлечения и обработки данных.
    """
    page = 1
    batch_size = 5  # Размер пакета страниц для параллельной обработки
    # Инициализация логгера
    logger = fl.setup_logger('dag_parse_omni_custom_fields')
    logger.info('---------------------------------------')
    logger.info('Начало работы DAG dag_parse_omni_custom_fields')

    # Создаем асинхронные сессии для HTTP-запросов и подключения к БД
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        # Получаем соединение с БД
        async with pool.acquire() as conn:
            # Получаем общее количество пользовательских полей
            total_count = await fg.get_snapshot(session, 'custom_fields')

            while True:
                # Очищаем список для хранения пользовательских полей текущего пакета
                batch_custom_fields = []

                # Переходим к параллельной обработке
                for i in range(batch_size):
                    # URL для запроса страницы
                    url = f'{OMNI_URL}/custom_fields.json?page={page}&limit=100'
                    data = await fg.fetch_response(session, url)

                    # Проверяем полученные данные
                    if not data or len(data) <= 1:
                        logger.error('Получили неожиданный результат - пустую страницу.')
                        raise Exception('Получили неожиданный результат - пустую страницу.')

                    # Определяем общее количество записей и страниц для текущего периода
                    if page == 1:
                        period_total = int(data.get("total_count", 0))
                        period_pages = (period_total + 99) // 100

                    # Очищаем список данных на текущей странице
                    custom_fields_data = []
                    for item in data.values():
                        if isinstance(item, dict) and "custom_field" in item:
                            custom_field = OmniCustomField(item["custom_field"])
                            processed_custom_field = custom_field.custom_field_properties()
                            if processed_custom_field:
                                custom_fields_data.append(processed_custom_field)

                    # Добавляем обработанные записи в пакет
                    batch_custom_fields.extend(custom_fields_data)

                    # Переходим к следующей странице или завершаем обработку текущего периода, если страница последняя
                    page += 1
                    if page > period_pages:
                        break

                # Вставка данных в БД
                if batch_custom_fields:
                    await qi.insert_custom_fields(conn, batch_custom_fields)
                    await insert_into_all_dimensions(conn, logger)

                # Логируем завершение обработки текущего пакета
                logger.info(f'Собраны данные за пакет страниц ({page-1}/{period_pages}).')

                if page > period_pages:
                    # Передаем в БД снэпшот количества пользовательских полей для валидации в дальнейшем
                    await ql.log_etl_catalogues(conn, 'lookup_omni_custom_field', total_count)
                    logger.info(f'Собраны все данные по пользовательским полям.')
                    return


def run_async_func():
    """
    Запускает основную асинхронную функцию fetch_and_process_custom_fields.
    """
    asyncio.run(fetch_and_process_custom_fields())


# Создание DAG для Airflow
with DAG(
    'dag_parse_omni_custom_fields',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
) as dag:
    fetch_custom_fields_task = PythonOperator(
        task_id='parse_omni_custom_fields',
        python_callable=run_async_func,
    )

    fetch_custom_fields_task
