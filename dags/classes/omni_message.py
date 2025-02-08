import re
from dataclasses import dataclass
from typing import Dict, Any, Optional

from functions import functions_data as fd


@dataclass
class OmniMessage:
    """
    Класс для представления сообщения.
    """
    response: Dict[str, Any]

    @property
    def message_id(self) -> int:
        return self.response.get("message_id")

    @property
    def user_id(self) -> int:
        return self.response.get("user_id")

    @property
    def staff_id(self) -> int:
        return self.response.get("staff_id")

    @property
    def message_type(self) -> str:
        if self.response.get('message_type') in ['note_regular', 'note_fwd_message']:
            return 'note'
        return self.response.get('message_type') or 'autoreply'

    @property
    def message_text(self) -> Optional[str]:
        raw_html = self.response.get('content') or self.response.get('content_html', '')
        if not raw_html:
            return None

        cleanr = re.compile("<.*?>")
        cleantext = re.sub(cleanr, " ", raw_html)
        cleantext = re.sub(r"\s+", " ", cleantext).strip()
        cleantext = re.sub(r"&nbsp;", " ", cleantext).strip()
        return cleantext if cleantext else None

    @property
    def attachment_type(self) -> Optional[str]:
        if not self.response.get('attachments'):
            return None
        return self.response.get('attachments')[0].get('mime_type', '').split(sep='/')[0]

    @property
    def created_at(self) -> Optional[str]:
        return fd.fix_datetime(self.response.get("created_at"))


