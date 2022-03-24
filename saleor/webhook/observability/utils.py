from dataclasses import InitVar, dataclass
from datetime import datetime, timedelta
from json import JSONEncoder
from json.encoder import ESCAPE_ASCII, ESCAPE_DCT  # type: ignore
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from django.utils import timezone

from ...core.auth import DEFAULT_AUTH_HEADER, SALEOR_AUTH_HEADER
from ..event_types import WebhookEventAsyncType, WebhookEventSyncType
from ..models import Webhook

if TYPE_CHECKING:
    from celery.exceptions import Retry

SENSITIVE_ENV_KEYS = (SALEOR_AUTH_HEADER, DEFAULT_AUTH_HEADER)
SENSITIVE_HEADERS = tuple(x[5:] for x in SENSITIVE_ENV_KEYS if x.startswith("HTTP_"))


def _get_webhooks_for_event(event_type, webhooks=None):
    """Get active webhooks from the database for an event."""
    permissions = {}
    required_permission = WebhookEventAsyncType.PERMISSIONS.get(
        event_type, WebhookEventSyncType.PERMISSIONS.get(event_type)
    )
    if required_permission:
        app_label, codename = required_permission.value.split(".")
        permissions["app__permissions__content_type__app_label"] = app_label
        permissions["app__permissions__codename"] = codename

    if webhooks is None:
        webhooks = Webhook.objects.all()

    webhooks = webhooks.filter(
        is_active=True,
        app__is_active=True,
        events__event_type__in=[event_type, WebhookEventAsyncType.ANY],
        **permissions,
    )
    webhooks = webhooks.select_related("app").prefetch_related(
        "app__permissions__content_type"
    )
    return webhooks


def hide_sensitive_headers(
    headers: Dict[str, str], sensitive_headers: Tuple[str, ...] = SENSITIVE_HEADERS
) -> Dict[str, str]:
    return {
        key: val if key.upper().replace("-", "_") not in sensitive_headers else "***"
        for key, val in headers.items()
    }


def task_next_retry_date(retry_error: "Retry") -> Optional[datetime]:
    if isinstance(retry_error.when, (int, float)):
        return timezone.now() + timedelta(seconds=retry_error.when)
    elif isinstance(retry_error.when, datetime):
        return retry_error.when
    return None


@dataclass
class JsonTruncText:
    text: str = ""
    truncated: bool = False
    added_bytes: InitVar[int] = 0
    ensure_ascii: InitVar[bool] = True

    def __post_init__(self, added_bytes, ensure_ascii):
        self._added_bytes = added_bytes
        self._ensure_ascii = ensure_ascii

    @property
    def byte_size(self) -> int:
        return len(self.text) + self._added_bytes

    @staticmethod
    def json_char_len(char: str, ensure_ascii=True) -> int:
        try:
            return len(ESCAPE_DCT[char])
        except KeyError:
            if ensure_ascii:
                return 6 if ord(char) < 0x10000 else 12
            return len(char.encode())

    @classmethod
    def truncate(cls, s: str, limit: int, ensure_ascii=True):
        limit = max(limit, 0)
        s_init_len = len(s)
        s = s[:limit]
        added_bytes = 0

        for match in ESCAPE_ASCII.finditer(s):
            start, end = match.span(0)
            markup = cls.json_char_len(match.group(0), ensure_ascii) - 1
            added_bytes += markup
            if end + added_bytes > limit:
                return cls(
                    text=s[:start],
                    truncated=True,
                    added_bytes=added_bytes - markup,
                    ensure_ascii=ensure_ascii,
                )
            elif end + added_bytes == limit:
                s = s[:end]
                return cls(
                    text=s,
                    truncated=len(s) < s_init_len,
                    added_bytes=added_bytes,
                    ensure_ascii=ensure_ascii,
                )
        return cls(
            text=s,
            truncated=len(s) < s_init_len,
            added_bytes=added_bytes,
            ensure_ascii=ensure_ascii,
        )


class CustomJsonEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, JsonTruncText):
            return {"text": o.text, "truncated": o.truncated}
        return super().default(o)
