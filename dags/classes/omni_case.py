import re
from config import BLACKLIST
from dataclasses import dataclass
from typing import Dict, Optional, List, Any

from functions import functions_data as fd


@dataclass
class OmniCase:
    """
    Класс для представления обращения (case).
    """
    raw_data: Dict[str, Any]

    @property
    def case_id(self) -> int:
        """ID обращения."""
        return self.raw_data.get("case_id")

    @property
    def case_number(self) -> Optional[str]:
        """Номер обращения."""
        return self.raw_data.get("case_number")

    @property
    def subject(self) -> Optional[str]:
        """Тема обращения."""
        return fd.fix_null(self.raw_data.get("subject"))

    @property
    def user_id(self) -> Optional[int]:
        """ID пользователя."""
        return self.raw_data.get("user_id")

    @property
    def staff_id(self) -> Optional[int]:
        """ID сотрудника."""
        return self.raw_data.get("staff_id")

    @property
    def group_id(self) -> Optional[int]:
        """ID группы."""
        return self.raw_data.get("group_id")

    @property
    def category_id(self) -> Optional[int]:
        """ID категории."""
        return fd.fix_int(self.raw_data.get("custom_fields", {}).get("cf_6998"))

    @property
    def block_id(self) -> Optional[int]:
        """ID блока."""
        return fd.fix_int(self.raw_data.get("custom_fields", {}).get("cf_4605"))

    @property
    def topic_id(self) -> Optional[int]:
        """ID темы."""
        return fd.fix_int(self.raw_data.get("custom_fields", {}).get("cf_8497"))

    @property
    def task_id(self) -> Optional[int]:
        """ID задачи."""
        return fd.fix_int(self.raw_data.get("custom_fields", {}).get("cf_9129"))

    @property
    def status(self) -> Optional[str]:
        """Статус обращения."""
        return self.raw_data.get("status")

    @property
    def priority(self) -> Optional[str]:
        """Приоритет обращения."""
        return self.raw_data.get("priority")

    @property
    def rating(self) -> Optional[str]:
        """Рейтинг обращения."""
        return fd.fix_null(self.raw_data.get("rating"))

    @property
    def device(self) -> Optional[str]:
        """Устройство."""
        return fd.fix_null(self.raw_data.get("custom_fields", {}).get("cf_9112"))

    @property
    def incident(self) -> Optional[bool]:
        """Флаг инцидента."""
        return fd.fix_bool(self.raw_data.get("custom_fields", {}).get("cf_8522"))

    @property
    def mass_issue(self) -> Optional[bool]:
        """Флаг массовой проблемы."""
        return fd.fix_bool(self.raw_data.get("custom_fields", {}).get("cf_5916"))

    @property
    def created_at(self) -> Optional[str]:
        """Дата создания."""
        return fd.fix_datetime(self.raw_data.get("created_at"))

    @property
    def updated_at(self) -> Optional[str]:
        """Дата последнего обновления."""
        return fd.fix_datetime(self.raw_data.get("updated_at"))

    @property
    def closed_at(self) -> Optional[str]:
        """Дата закрытия."""
        return fd.fix_datetime(self.raw_data.get("closed_at"))

    @property
    def labels(self) -> List[str]:
        """Метки обращения."""
        return self.raw_data.get("labels") or []


    def case_properties(self) -> Optional[tuple]:
        """
        Возвращает кортеж с обработанными данными.
        """
        if self.blacklist_check():
            return None  # Пропускаем записи из блэк-листа
        return (
            self.case_id,
            self.case_number,
            self.subject,
            self.user_id,
            self.staff_id,
            self.group_id,
            self.category_id,
            self.block_id,
            self.topic_id,
            self.task_id,
            self.status,
            self.priority,
            self.rating,
            self.device,
            self.incident,
            self.mass_issue,
            self.created_at,
            self.updated_at,
            self.closed_at,
            self.labels
        )


    def blacklist_check(self):
        # Фильтруем результаты из груп Бухгалтерия и Спам
        if self.group_id in [80218, 65740]:
            return True

        # Возвращаем значения, которые не попали в блэклист
        match_result = BLACKLIST.match(self.subject or "")
        return bool(match_result and match_result.group())

