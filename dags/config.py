import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

placeholder_date = datetime(2024, 1, 1)

# omni
OMNI_USER = os.getenv("OMNI_USER")
OMNI_LOGIN = os.getenv("OMNI_LOGIN")
OMNI_PASSWORD = os.getenv("OMNI_PASSWORD")

# database
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

DB_CONFIG = {
    "user": DB_USER,          # имя пользователя PostgreSQL
    "password": DB_PASSWORD,  # пароль PostgreSQL
    "host": DB_HOST,          # адрес БД
    "port": DB_PORT,          # порт БД
    "database": DB_NAME       # имя БД
}

DB_DSN = f"user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT} dbname={DB_NAME}"


DAG_CONFIG = {
    'owner': '@pbushmanov',
    'email': 'pbushmanov@hr-link.ru',
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

BLACKLIST = "|".join(BLACKLIST_LIST)



# delay in sec between case request
PAUSE_IN_SEC = 1
# urls
OMNI_URL = "https://hr-link.omnidesk.ru/api"
USERS_URL = "https://hr-link.omnidesk.ru/api/users.json"
GROUP_URL = "https://hr-link.omnidesk.ru/api/groups.json"
LABEL_URL = "https://hr-link.omnidesk.ru/api/labels.json"
CUSTOM_FIELD_URL = "https://hr-link.omnidesk.ru/api/custom_fields.json"
CASES_URL = "https://hr-link.omnidesk.ru/api/cases.json"
COMPANY_URL = "https://hr-link.omnidesk.ru/api/companies.json"
STAFF_URL = "https://hr-link.omnidesk.ru/api/staff.json"
CASE_MESSAGE_URL = "https://hr-link.omnidesk.ru/api/cases/{id}/messages.json"
