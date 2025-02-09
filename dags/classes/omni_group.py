from dataclasses import dataclass
from typing import Dict, Any, Optional
from functions import functions_data as fd


@dataclass
class OmniGroup:
    """
    Класс для представления группы (group).
    """
    raw_data: Dict[str, Any]

    @property
    def group_id(self) -> Optional[int]:
        """ID группы."""
        return self.raw_data.get("group_id")

    @property
    def group_title(self) -> Optional[str]:
        """Название группы."""
        return self.raw_data.get("group_title")

    @property
    def active(self) -> Optional[bool]:
        """Статус активности группы."""
        return self.raw_data.get("active")

    @property
    def created_at(self) -> Optional[str]:
        """Дата создания записи."""
        return fd.fix_datetime(self.raw_data.get("created_at"))

    @property
    def updated_at(self) -> Optional[str]:
        """Дата последнего обновления записи."""
        return fd.fix_datetime(self.raw_data.get("updated_at"))

    def group_properties(self) -> tuple:
        """
        Возвращает кортеж с обработанными данными группы.
        """
        return (
            self.group_id,
            self.group_title,
            self.active,
            self.created_at,
            self.updated_at
        )
