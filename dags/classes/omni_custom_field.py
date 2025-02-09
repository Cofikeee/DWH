from dataclasses import dataclass
from typing import Dict, Any, Optional
import json


@dataclass
class OmniCustomField:
    """
    Класс для представления пользовательского поля (custom field).
    """
    raw_data: Dict[str, Any]

    @property
    def field_id(self) -> Optional[int]:
        """ID поля."""
        return self.raw_data.get("field_id")

    @property
    def title(self) -> Optional[str]:
        """Название поля."""
        return self.raw_data.get("title")

    @property
    def field_type(self) -> Optional[str]:
        """Тип поля."""
        return self.raw_data.get("field_type")

    @property
    def field_level(self) -> Optional[str]:
        """Уровень поля."""
        return self.raw_data.get("field_level")

    @property
    def active(self) -> Optional[bool]:
        """Статус активности поля."""
        return self.raw_data.get("active")

    @property
    def field_data(self) -> str:
        """Данные поля в формате JSON."""
        return json.dumps(self.raw_data.get("field_data"))

    def custom_field_properties(self) -> tuple:
        """
        Возвращает кортеж с обработанными данными пользовательского поля.
        """
        return (
            self.field_id,
            self.title,
            self.field_type,
            self.field_level,
            self.active,
            self.field_data
        )
