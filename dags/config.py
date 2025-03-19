import os
from datetime import datetime
from dotenv import load_dotenv
import asyncio
import re

GLOBAL_PAUSE = asyncio.Event()  # Глобальный объект для паузы воркеров
GLOBAL_PAUSE.set()  # Изначально установим в "работает"

load_dotenv()

# КРЕДЫ ПОЛЬЗОВАТЕЛЯ "OMNI"
OMNI_USER = os.getenv("OMNI_USER")
OMNI_LOGIN = os.getenv("OMNI_LOGIN")
OMNI_PASSWORD = os.getenv("OMNI_PASSWORD")

# МОИ КРЕДЫ
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# КРЕДЫ DWH
DWH_USER = os.getenv("DWH_USER")
DWH_PASSWORD = os.getenv("DWH_PASSWORD")

# REAPER
REAPER_DB_NAME = os.getenv("REAPER_DB_NAME")
DB_CONFIG = {
    "user": DWH_USER,
    "password": DWH_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": REAPER_DB_NAME
}
REAPER_DB_DSN = f"user={DWH_USER} password={DWH_PASSWORD} host={DB_HOST} port={DB_PORT} dbname={REAPER_DB_NAME}"

# OMNIDESK
# OMNI_DB_NAME = os.getenv("OMNI_DB_NAME")
# REAPER_DB_DSN = f"user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT} dbname={OMNI_DB_NAME}"

# КОНФИГИ DAG-ОВ
DAG_CONFIG = {
    'owner': '@pbushmanov',
    'start_date': datetime(2025, 1, 1),  # Дата начала
}

TEN_FIRST_DATE = datetime(2023, 1, 1)
FIRST_DATE = datetime(2020, 9, 1)

# КОНФИГИ ОБХОДЧИКОВ
COLORS = ['green', 'blue', 'black', 'pink', 'gold']
COLORS_SEMAPHORES = {'green': 10,
                     'blue': 10,
                     'black': 10,
                     'pink': 10,
                     'gold': 10}

# КОНФИГИ OMNIDESK
OMNI_URL = "https://hr-link.omnidesk.ru/api"
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

# КОНФИГ ДЛЯ КОНТРОЛЯ ЛИМИТОВ ПАРСЕРА OMNIDESK
DELAY_BETWEEN_REQUESTS = 0.1
WORKERS = 5
OFFSET_VALUE = 0
OFFSET_SKEW = 20
QUEUE_SIZE = 20
