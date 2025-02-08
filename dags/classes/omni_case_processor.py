import aiohttp
import asyncpg
import re
from typing import Dict, Any

from classes.omni_case import OmniCase
from config import BLACKLIST
from queries import queries_insert as qi


class OmniCaseProcessor:
    def __init__(self, case_id: int, session: aiohttp.ClientSession, pool: asyncpg.Pool):
        """
        Инициализация процессора случая.

        :param case_id: ID случая.
        :param session: HTTP-сессия.
        :param pool: Пул соединений с базой данных.
        """
        self.case_id = case_id
        self.session = session
        self.pool = pool
        self.all_cases = []

    async def process_case(self, record: Dict[str, Any]) -> None:
        """
        Обработка одного случая.

        :param record: Сырые данные о случае.
        """
        if self.blacklist_check(record):
            return  # Игнорируем записи из блэк-листа

        case = OmniCase(record)
        processed_case = (
            case.case_id,
            case.case_number,
            case.subject,
            case.user_id,
            case.staff_id,
            case.group_id,
            case.category_id,
            case.block_id,
            case.topic_id,
            case.task_id,
            case.status,
            case.priority,
            case.rating,
            case.device,
            case.incident,
            case.mass_issue,
            case.created_at,
            case.updated_at,
            case.closed_at,
            case.labels
        )

        self.all_cases.append(processed_case)


    async def insert_cases(self) -> None:
        """
        Вставка обработанных случаев в базу данных.
        """
        if self.all_cases:
            async with self.pool.acquire() as conn:
                await qi.insert_cases(conn, [case[:-1] for case in self.all_cases])  # Убираем метки
                await qi.insert_case_labels(conn, [(case[0], case[-1]) for case in self.all_cases])  # Добавляем метки


