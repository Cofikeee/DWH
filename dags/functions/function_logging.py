import logging
from logging.handlers import RotatingFileHandler


# Функция для добавления 3 часов к времени
def add_3_hours(record):
    record.created += 3 * 60 * 60  # Добавляем 3 часа в секундах
    return record


def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Создание обработчиков
    console_handler = logging.StreamHandler()

    file_handler = RotatingFileHandler(f'/opt/airflow/logs/etl_logs/{name[4:]}.log', maxBytes=1024 * 1024 * 5, backupCount=3)

    # Установка уровня логирования
    console_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.DEBUG)

    # Создание форматтера
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # Применение форматтера
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Добавление обработчиков
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
