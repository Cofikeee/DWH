from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class OmniLabel:
    """
    Класс для представления метки (label).
    """
    raw_data: Dict[str, Any]

    @property
    def label_id(self) -> Optional[int]:
        """ID метки."""
        return self.raw_data.get("label_id")

    @property
    def label_title(self) -> Optional[str]:
        """Название метки."""
        return self.raw_data.get("label_title")

    def label_properties(self) -> tuple:
        """
        Возвращает кортеж с обработанными данными метки.
        """
        return (
            self.label_id,
            self.label_title
        )
