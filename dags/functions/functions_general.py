import asyncio
from datetime import datetime
from dateutil.relativedelta import relativedelta

from config import DB_DSN, BLACKLIST, OMNI_URL, GLOBAL_PAUSE # noqa
from classes.ratelimiter import RateLimiter
from functions import functions_data as fd


def get_today():
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


async def get_snapshot(session, table):
    if table == 'users':
        total_count = 0
        to_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        while True:
            from_time = to_time - relativedelta(days=364)
            url = f'{OMNI_URL}/{table}.json?from_updated_time={from_time}&to_updated_time={to_time}'
            print(url)
            data = await fetch_response(session, url)
            data_total = int(data.get("total_count", 0))
            total_count += data_total
            to_time = from_time
            if to_time <= datetime.strptime('2020-11-03 00:00:00', '%Y-%m-%d %H:%M:%S'):
                return total_count

    url = f'{OMNI_URL}/{table}.json'
    data = await fetch_response(session, url)
    return data.get("total_count", 0)




async def fetch_response(session, url, max_retries=5):
    """Асинхронное получение JSON-данных с повторными попытками при ошибке 429."""
    limiter = RateLimiter()

    for attempt in range(max_retries):
        await limiter.wait()
        async with session.get(url) as response:
            if response.status == 429:
                print(f"{datetime.now()} - Получен 429, пауза на 60 сек.")
                GLOBAL_PAUSE.clear()  # Останавливаем все воркеры
                await asyncio.sleep(60)  # Ждём минуту
                GLOBAL_PAUSE.set()  # Возобновляем работу
                continue

            response.raise_for_status()
            return await response.json()

    raise Exception(f"Превышено количество попыток для URL: {url}")



def fetch_data(response, data_extractor, table):
    """
    Извлекает и преобразует данные о пользователях из ответа API.

    Аргументы:
    response -- ответ API (JSON), содержащий данные о пользователях.
    data_extractor -- функция для извлечения данных о пользователе.

    Возвращает:
    response_data -- список данных о пользователях для вставки в базу данных.
    """
    response_data = []  # Создаем пустой массив

    for response_value in response.values():  # Проходим по всем записям со страницы
        if isinstance(response_value, int) or isinstance(response_value, str):
            break  # Прерываем обработку, если на странице закончились записи.


        record = response_value.get(table, {})  # Одна запись из ответа API

        if record.get('updated_at'):
            try:
                if fd.fix_datetime(record.get('updated_at')) >= get_today():
                    continue  # Скипаем сегодняшние записи
            except TypeError:
                pass  # На случай если в бд еще нет записей
        # Используем переданную функцию для извлечения данных
        response_data.append(data_extractor(record))
    return response_data  # Возвращаем одну страницу данных


'''

async def log_etl_messages_batch(conn, logs):
    """Массовая запись логов в БД."""
    query = """
        INSERT INTO ctl_etl_omni_messages (case_id, page, parsed, period_total, parsed_date)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (case_id, page) DO UPDATE
        SET parsed = EXCLUDED.parsed,
            period_total = EXCLUDED.period_total,
            parsed_date = NOW();
    """
    if logs:
        await conn.executemany(query, logs)
'''

