import os
from datetime import datetime
from dotenv import load_dotenv
import asyncio
import re

GLOBAL_PAUSE = asyncio.Event()  # Глобальный объект для паузы воркеров
GLOBAL_PAUSE.set()  # Изначально установим в "работает"

load_dotenv()

placeholder_date = datetime(2024, 1, 1)

# omni
OMNI_USER = os.getenv("OMNI_USER")
OMNI_LOGIN = os.getenv("OMNI_LOGIN")
OMNI_PASSWORD = os.getenv("OMNI_PASSWORD")

# database
OMNI_DB_NAME = os.getenv("OMNI_DB_NAME")
REAPER_DB_NAME = os.getenv("REAPER_DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

OMNI_DB_CONFIG = {
    "user": DB_USER,          # имя пользователя PostgreSQL
    "password": DB_PASSWORD,  # пароль PostgreSQL
    "host": DB_HOST,          # адрес БД
    "port": DB_PORT,          # порт БД
    "database": OMNI_DB_NAME  # имя БД
}

OMNI_DB_DSN = f"user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT} dbname={OMNI_DB_NAME}"


DAG_CONFIG = {
    'owner': '@pbushmanov',
    'start_date': datetime(2025, 1, 1),  # Дата начала
}

BLACKLIST_LIST = [
    "ILOVEHRLINK4EVER&FORMOFRESPONSEABOUTHRLINK",
    "Поступление на счёт в валюте счёта",
    "Добро пожаловать в Omnidesk",
    "Автоматический ответ:.*",  # Оставляем так, как есть
    "Automatic reply:.*",
    "Заявка на выпуск УНЭП:.*",
    "Ф: Заявка на выпуск УНЭП:.*",
    "INTEGRTP-.*",
    "Недоставленное сообщение",
    r"\[АСофт Трекер\].*",  # Исключение для всего текста после
    "Support:.*",
    "Бухгалтерия"
]

BLACKLIST = re.compile("|".join(BLACKLIST_LIST))

# urls
OMNI_URL = "https://hr-link.omnidesk.ru/api"

# parser settings
DELAY_BETWEEN_REQUESTS = 0.08
WORKERS = 5
OFFSET_VALUE = 0
OFFSET_SKEW = 20
QUEUE_SIZE = 10
