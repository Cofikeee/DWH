from dataclasses import dataclass
from typing import Dict, Any, Optional
from functions import functions_data as fd
import json


@dataclass
class OmniCompany:
    """
    Класс для представления компании (company).
    """
    raw_data: Dict[str, Any]

    @property
    def company_id(self) -> Optional[int]:
        """ID компании."""
        return self.raw_data.get("company_id")

    @property
    def company_name(self) -> Optional[str]:
        """Название компании."""
        return fd.fix_null(self.raw_data.get("company_name"))

    @property
    def tarif(self) -> Optional[str]:
        """Тариф компании."""
        return fd.fix_null(self.raw_data.get("custom_fields", {}).get("cf_8963"))

    @property
    def btrx_id(self) -> Optional[int]:
        """ID BTRX компании."""
        return fd.fix_int(self.raw_data.get("custom_fields", {}).get("cf_8591"))

    @property
    def responsible(self) -> Optional[str]:
        """Ответственный за компанию."""
        return fd.fix_null(self.raw_data.get("custom_fields", {}).get("cf_8602"))

    @property
    def active(self) -> Optional[bool]:
        """Статус активности компании."""
        return self.raw_data.get("active")

    @property
    def deleted(self) -> Optional[bool]:
        """Статус удаления компании."""
        return self.raw_data.get("deleted")

    @property
    def created_at(self) -> Optional[str]:
        """Дата создания записи."""
        return fd.fix_datetime(self.raw_data.get("created_at"))

    @property
    def updated_at(self) -> Optional[str]:
        """Дата последнего обновления записи."""
        return fd.fix_datetime(self.raw_data.get("updated_at"))

    def company_properties(self) -> tuple:
        """
        Возвращает кортеж с обработанными данными компании.
        """
        return (
            self.company_id,
            self.company_name,
            self.tarif,
            self.btrx_id,
            self.responsible,
            self.active,
            self.deleted,
            self.created_at,
            self.updated_at
        )
