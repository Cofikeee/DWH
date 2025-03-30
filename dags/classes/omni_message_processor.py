from typing import List, Tuple, Any

from config import OMNI_URL, GLOBAL_PAUSE
import functions.functions_general as fg
import queries.queries_log as ql
import queries.queries_insert as qi
from classes.ratelimiter import RateLimiter
from classes.omni_message import OmniMessage


class OmniMessageProcessor:
    def __init__(self, case_id: int, session: Any, pool: Any):
        """
        Инициализация процессора сообщений.

        :param case_id: ID случая.
        :param session: HTTP-сессия.
        :param pool: Пул соединений с базой данных.
        """
        self.case_id = case_id
        self.session = session
        self.pool = pool
        self.page = 1
        self.all_messages: List[Tuple] = []

    async def process_case(self) -> None:
        """
        Обработка одного case_id.
        """
        while True:
            await GLOBAL_PAUSE.wait()  # Ждём, если стоит глобальная пауза
            await RateLimiter().wait()

            page_url = f'{OMNI_URL}/cases/{self.case_id}/messages.json?page={self.page}'
            try:
                data = await fg.fetch_response(self.session, page_url)  # Запрос к API
            except Exception as e:
                print(f'Ошибка при запросе {page_url}: {e}')
                return

            if not data:
                await ql.log_etl_messages(self.pool, self.case_id, self.page, 0, 0)
                break

            period_total = data.get("total_count", 0)
            period_pages = (period_total + 99) // 100
            messages_data = []

            for item in data.values():
                if isinstance(item, dict) and "message" in item:
                    message = OmniMessage(item["message"])  # Создаём объект класса Message
                    messages_data.append((
                        message.message_id,
                        self.case_id,
                        message.user_id,
                        message.staff_id,
                        message.message_type,
                        message.attachment_type,
                        message.message_text,
                        message.created_at
                    ))

            self.all_messages.extend(messages_data)
            await ql.log_etl_messages(self.pool, self.case_id, self.page, len(messages_data), period_total)

            if self.page == period_pages:
                break

            self.page += 1

        if self.all_messages:
            async with self.pool.acquire() as conn:
                await qi.insert_omni_message(conn, self.all_messages)
