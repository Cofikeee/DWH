from dataclasses import dataclass
from typing import Dict, Any, Optional, List

from functions import functions_data as fd


@dataclass
class OmniUser:
    """
    Класс для представления пользователя (user).
    """
    raw_data: Dict[str, Any]

    ROLES_MAPPING = {
        'cf_7012': 'Администратор',
        'cf_7011': 'Кадровик',
        'cf_7010': 'Руководитель',
        'cf_7013': 'Делопроизводитель',
    }
    CHANNEL_MAPPING = {
        'phone': 'user_phone',
        'telegram': 'telegram_id',
        'email': 'user_email',
        'vkontakte': 'vkontakte_id',
        'facebook': 'facebook_id',
        'wa': 'wa_id',
        'mattermost': 'mattermost_id'
    }

    @property
    def omni_user_id(self) -> int:
        """ID пользователя."""
        return self.raw_data.get("user_id")

    @property
    def channel_type(self) -> str:
        """Тип канала."""
        return self.raw_data.get("type")

    @property
    def channel_value(self) -> str:
        """
        Метод для преобразования типа канала в соответствующее поле.
        """
        response = self.raw_data.get("type")
        return self.raw_data.get(self.CHANNEL_MAPPING.get(response, response))

    @property
    def company_name(self) -> Optional[str]:
        """Название компании."""
        return self.raw_data.get("company_name")

    @property
    def user_id(self) -> Optional[int]:
        """Пользовательский ID из поля hrl_user_id."""
        return fd.fix_null(self.raw_data.get("custom_fields", {}).get("cf_6439"))

    @property
    def comfirmed(self) -> Optional[bool]:
        """Булево значение из поля employee."""
        return fd.fix_bool(self.raw_data.get("custom_fields", {}).get("cf_7017"))

    @property
    def user_roles(self) -> List[str]:
        """
        Метод для получения списка ролей пользователя на основе custom_fields.
        """
        roles_array = []
        for cf_key, role_name in self.ROLES_MAPPING.items():
            if self.raw_data.get('custom_fields', {}).get(cf_key):
                roles_array.append(role_name)
        return roles_array

    @property
    def created_at(self) -> Optional[str]:
        """Дата создания записи."""
        return fd.fix_datetime(self.raw_data.get("created_at"))

    @property
    def updated_at(self) -> Optional[str]:
        """Дата последнего обновления записи."""
        return fd.fix_datetime(self.raw_data.get("updated_at"))

    def user_properties(self) -> Optional[tuple]:
        """
        Возвращает кортеж с обработанными данными пользователя.
        """
        return (
            self.omni_user_id,
            self.channel_type,
            self.channel_value,
            self.company_name,
            self.user_id,
            self.comfirmed,
            self.user_roles,
            self.created_at,
            self.updated_at
        )






