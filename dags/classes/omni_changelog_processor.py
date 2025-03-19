from datetime import datetime
from typing import List, Tuple, Any
from aiohttp import ClientResponseError  # Для обработки HTTP-ошибок

from config import OMNI_URL, GLOBAL_PAUSE
import functions.functions_general as fg
from classes.ratelimiter import RateLimiter
from classes.omnI_changelog import OmniChangelog


class OmniChangelogProcessor:
    def __init__(self, case_id: int, session: Any, conn: Any):
        """
        Инициализация процессора сообщений.

        :param case_id: ID случая.
        :param session: HTTP-сессия.
        :param pool: Пул соединений с базой данных.
        """
        self.case_id = case_id
        self.session = session
        self.conn = conn
        self.all_changelogs: List[Tuple] = []

    async def process_case(self) -> List[Tuple] or None:
        """
        Обработка одного case_id.
        """
        await GLOBAL_PAUSE.wait()  # Ждём, если стоит глобальная пауза
        await RateLimiter().wait()

        page_url = f'{OMNI_URL}/cases/{self.case_id}/changelog.json'
        try:
            data = await fg.fetch_response(self.session, page_url)  # Запрос к API
        except ClientResponseError as e:  # Обработка HTTP-ошибок
            changelog_data = [(self.case_id,
                               'deleted',
                               None,
                               None,
                               datetime(1970, 1, 1))]
            return changelog_data
        except Exception as e:
            print(f'Ошибка при запросе {page_url}: {e}')
            return

        if not data:
            changelog_data = [(self.case_id,
                               'deleted',
                               None,
                               None,
                               datetime(1970, 1, 1))]
            return changelog_data

        changelog_data = []
        for item in list(data.values())[0]:
            changelog = OmniChangelog(item)  # Создаём объект класса Changelog
            if changelog.event:
                processed_changelog_data = changelog.changelog_properties(self.case_id)
                if processed_changelog_data:
                    changelog_data.append(processed_changelog_data)
        if not changelog_data:
            changelog_data.append((self.case_id,
                                   None,
                                   None,
                                   None,
                                   datetime(1970, 1, 1)
                                   ))
        return changelog_data
