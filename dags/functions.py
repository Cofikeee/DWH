from datetime import datetime
import psycopg2
import re
from psycopg2 import sql
from dateutil.relativedelta import relativedelta
import asyncio
import random
from config import DB_DSN, BLACKLIST
from classes import RateLimiter




def select_max_ts(data_table):
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute(
        sql.SQL("SELECT DATE_TRUNC('second', MAX(updated_date)) max_ts FROM {}").format(
            sql.Identifier(data_table)
        )
    )
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res[0][0]


def select_min_ts(data_table):
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute(
        sql.SQL("SELECT DATE_TRUNC('second', MIN(created_date)) min_ts FROM {}").format(
            sql.Identifier(data_table)
        )
    )
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res[0][0]


def fix_datetime(timestamp):
    if timestamp:
        if not timestamp == '-':
            dt = datetime.strptime(timestamp, '%a, %d %b %Y %H:%M:%S %z')
            return dt.replace(tzinfo=None)
        return None
    return None


def fix_bool(response):
    if response:
        if not response == '':
            return True
        return False
    return False


def fix_null(response):
    if response:
        if not response == '':
            return response.strip()
        return None
    return None


def fix_int(response):
    if type(response) is str:
        return int(response) if response else None
    elif type(response) is list:
        int_array = []
        for r in response:
            int_array += [int(r)]
        return int_array


def fix_channel(response):
    channel_mapping = {
        'phone': 'user_phone',
        'telegram': 'telegram_id',
        'email': 'user_email',
        'vkontakte': 'vkontakte_id',
        'facebook': 'facebook_id',
        'wa': 'wa_id',
        'mattermost': 'mattermost_id'
    }
    return channel_mapping.get(response, response)


def fix_message_type(record):
    if record.get('message_type') in ['note_regular', 'note_fwd_message']:
        return 'note'
    return record.get('message_type') if record.get('message_type') != '' else 'autoreply'


def fix_message_text(record):
    if len(record.get('content')) == 0:
        raw_html = record.get('content_html')
        if len(raw_html) == 0:
            return None
    else:
        raw_html = record.get('content')

    cleanr = re.compile("<.*?>")
    cleantext = re.sub(cleanr, " ", raw_html)
    cleantext = re.sub(r"\s+", " ", cleantext).strip()
    cleantext = re.sub(r"&nbsp;", " ", cleantext).strip()
    return cleantext if cleantext != '' else None


def fix_message_attachment(record):
    if not record.get('attachments'):
        return None
    return record.get('attachments')[0].get('mime_type').split(sep='/')[0]


def get_today():
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def get_user_roles_array(user):
    roles = {
        'cf_7012': 'Администратор',
        'cf_7011': 'Кадровик',
        'cf_7010': 'Руководитель',
        'cf_7013': 'Делопроизводитель',
    }
    roles_array = []
    for cf_key, value in roles.items():
        if user.get('custom_fields', {}).get(cf_key):
            roles_array += [value]
    return roles_array  # if len(roles_array) > 0 else None


def blacklist_check(record):
    match_result = re.match(BLACKLIST, record.get('subject'))
    # Фильтруем результаты из груп Бухгалтерия и Спам
    if record.get('group_id') in [80218, 65740]:
        return True
    # Возвращаем значения, которые не попали в блэклист
    if not match_result or match_result.group() == '':
        return False
    # Скипаем значения, которые попали в блэклист
    return True


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

        if table == 'user_id':
            record = response.get('user', {})  # Одна запись из ответа API
            response_data.append(data_extractor(record))
            return response_data  # Возвращаем одну страницу данных
        else:
            record = response_value.get(table, {})  # Одна запись из ответа API

        if record.get('updated_at'):
            try:
                if fix_datetime(record.get('updated_at')) >= get_today():
                    continue  # Скипаем сегодняшние записи
            except TypeError:
                pass  # На случай если в бд еще нет записей
        # Используем переданную функцию для извлечения данных
        response_data.append(data_extractor(record))
    return response_data  # Возвращаем одну страницу данных



async def fetch_response(session, url, max_retries=5):
    retry = 0
    limiter = RateLimiter(min_interval=0.04)  # 20 мс
    """Асинхронное получение JSON-данных с повторными попытками при ошибке 429."""
    for attempt in range(max_retries):
        await limiter.wait()  # Гарантируем минимальный интервал
        async with session.get(url) as response:
            if response.status == 429:  # Обработка слишком частых запросов
                print(datetime.now(), 'Получен 429', url)
                retry += 1
                retry_after = 60 * retry
                await asyncio.sleep(retry_after)
                continue

            api_calls_left = int(response.headers.get("api_calls_left", 0))
            rate_limit_per_minute = int(response.headers.get("rate_limit_per_minute", 1))

            if api_calls_left / rate_limit_per_minute < 0.1:
                delay = 30
            else:
                delay = 0

            if delay > 0:
                print(f"Осталось меньше 10% вызовов API. Задержка на {delay} секунд.")
                await asyncio.sleep(delay)

            response.raise_for_status()
            return await response.json()

    raise Exception(f"Превышено количество попыток для URL: {url}")





async def log_etl_statistics(conn, table_name, from_time, to_time, page, parsed, blacklisted, period_total):
    page_total = parsed + blacklisted
    query = """
        INSERT INTO ctl_omni_etl_log (table_name, from_time, to_time, page, parsed, blacklisted, page_total, period_total, parsed_date)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (table_name, from_time, page) DO UPDATE
        SET to_time = EXCLUDED.to_time,
            parsed = EXCLUDED.parsed, 
            blacklisted = EXCLUDED.blacklisted, 
            page_total = EXCLUDED.page_total,
            period_total = EXCLUDED.period_total,
            parsed_date = NOW();
    """
    await conn.execute(query, table_name, from_time, to_time, page, parsed, blacklisted, page_total, period_total)


async def log_etl_messages(conn, case_id, page, parsed, period_total):

    query = """
        INSERT INTO ctl_etl_omni_messages (case_id, page, parsed, period_total, parsed_date)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (case_id, page) DO UPDATE
        SET parsed = EXCLUDED.parsed,
            period_total = EXCLUDED.period_total,
            parsed_date = NOW();
    """
    await conn.execute(query, case_id, page, parsed, period_total)

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
