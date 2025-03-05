import re
from dataclasses import dataclass
from typing import Dict, Any, Optional

from functions import functions_data as fd


@dataclass
class OmniChangelog:
    """
    Класс для представления сообщения.
    """
    response: Dict[str, Any]

    @property
    def event(self) -> Optional[str]:
        response = self.response.get("event")
        if response in ('status', 'group', 'priority'):
            return self.response.get("event")
        return None

    @property
    def old_value(self) -> Optional[str]:
        return self.response.get("old_value")

    @property
    def new_value(self) -> Optional[str]:
        return self.response.get("value")

    @property
    def created_at(self) -> Optional[str]:
        return fd.fix_datetime(self.response.get("created_at"))


    def changelog_properties(self, case_id) -> Optional[tuple]:
        """
        Возвращает кортеж с обработанными данными.
        """
        if self.old_value == self.new_value:
            return None
        return (
            case_id,
            self.event,
            self.old_value,
            self.new_value,
            self.created_at
        )
