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
from classes.omni_label import OmniLabel
# Запросы к БД
from queries import queries_log as ql, queries_insert as qi
# Функции
from functions import functions_general as fg, function_logging as fl


async def fetch_and_process_labels():
    """
    Асинхронная функция для извлечения и обработки данных о метках из API Omni.
    Логика работы:
    1. Извлекает данные страницами из API Omni.
    2. Обрабатывает каждую запись с использованием класса OmniLabel.
    3. Вставляет обработанные данные в базу данных.
    4. Логирует процесс извлечения и обработки данных.
    """
    page = 1
    batch_size = 5  # Размер пакета страниц для параллельной обработки

    # Инициализация логгера
    logger = fl.setup_logger('dag_parse_omni_labels')
    logger.info('---------------------------------------')
    logger.info('Начало работы DAG dag_parse_omni_labels')

    # Создаем асинхронные сессии для HTTP-запросов и подключения к БД
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(OMNI_LOGIN, OMNI_PASSWORD)) as session, \
            asyncpg.create_pool(**DB_CONFIG, min_size=5, max_size=20) as pool:
        # Получаем соединение с БД
        async with pool.acquire() as conn:
            # Получаем общее количество меток
            total_count = await fg.get_snapshot(session, 'labels')

            while True:
                # Очищаем список для хранения меток текущего пакета
                batch_labels = []

                # Переходим к параллельной обработке
                for i in range(batch_size):
                    # URL для запроса страницы
                    url = f'{OMNI_URL}/labels.json?page={page}&limit=100'
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
                    labels_data = []
                    for item in data.values():
                        if isinstance(item, dict) and "label" in item:
                            label = OmniLabel(item["label"])
                            processed_label = label.label_properties()
                            if processed_label:
                                labels_data.append(processed_label)

                    # Добавляем обработанные записи в пакет
                    batch_labels.extend(labels_data)

                    # Переходим к следующей странице или завершаем обработку текущего периода, если страница последняя
                    page += 1
                    if page > period_pages:
                        break

                # Вставка данных в БД
                if batch_labels:
                    await qi.insert_omni_label(conn, batch_labels)

                # Логируем завершение обработки текущего пакета
                logger.info(f'Собраны данные за пакет страниц ({page-1}/{period_pages}).')

                if page > period_pages:
                    # Передаем в БД снэпшот количества меток для валидации в дальнейшем
                    await ql.log_etl_catalogues(conn, 'dim_omni_label', total_count)
                    logger.info(f'Собраны все данные по меткам.')
                    return


def run_async_func():
    """
    Запускает основную асинхронную функцию fetch_and_process_labels.
    """
    asyncio.run(fetch_and_process_labels())


# Создание DAG для Airflow
with DAG(
    'dag_parse_omni_labels',
    default_args=DAG_CONFIG,  # Подгружаем настройки из конфига
    catchup=False,            # Не выполнять пропущенные интервалы
    schedule_interval=None,   # Не запускать автоматически
    tags=['omni']

) as dag:
    fetch_labels_task = PythonOperator(
        task_id='parse_omni_labels',
        python_callable=run_async_func,
    )

    fetch_labels_task
