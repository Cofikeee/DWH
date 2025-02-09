from dataclasses import dataclass
from typing import Dict, Any, Optional
from functions import functions_data as fd


@dataclass
class OmniStaff:
    """
    Класс для представления сотрудника (staff).
    """
    raw_data: Dict[str, Any]

    @property
    def staff_id(self) -> Optional[int]:
        """ID сотрудника."""
        return self.raw_data.get("staff_id")

    @property
    def staff_full_name(self) -> Optional[str]:
        """Полное имя сотрудника."""
        return fd.fix_null(self.raw_data.get("staff_full_name"))

    @property
    def staff_email(self) -> Optional[str]:
        """Email сотрудника."""
        return fd.fix_null(self.raw_data.get("staff_email"))

    @property
    def active(self) -> Optional[bool]:
        """Статус активности сотрудника."""
        return self.raw_data.get("active")

    @property
    def created_at(self) -> Optional[str]:
        """Дата создания записи."""
        return fd.fix_datetime(self.raw_data.get("created_at"))

    @property
    def updated_at(self) -> Optional[str]:
        """Дата последнего обновления записи."""
        return fd.fix_datetime(self.raw_data.get("updated_at"))

    def staff_properties(self) -> tuple:
        """
        Возвращает кортеж с обработанными данными сотрудника.
        """
        return (
            self.staff_id,
            self.staff_full_name,
            self.staff_email,
            self.active,
            self.created_at,
            self.updated_at
        )
