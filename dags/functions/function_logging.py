import logging
from logging.handlers import RotatingFileHandler


def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Создание обработчиков
    console_handler = logging.StreamHandler()
    file_handler = RotatingFileHandler('/opt/airflow/logs/omni_etl_logs.log', maxBytes=1024 * 1024 * 5, backupCount=3)

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
