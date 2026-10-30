"""Logging JSON a stderr. No incluye secretos ni datos personales."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

REDACTED_KEYS = {
    "password",
    "token",
    "authorization",
    "card_no",
    "cardno",
    "employee_no",
    "employeeno",
    "name",
    "label",
    "raw",
    "pictureurl",
    "picture_url",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            for key, value in fields.items():
                payload[str(key)] = _redact(str(key), value)
        if record.exc_info:
            payload["error_type"] = record.exc_info[0].__name__ if record.exc_info[0] else "error"
        return json.dumps(payload, ensure_ascii=False, default=str)


def _redact(key: str, value: Any) -> Any:
    if key.casefold() in REDACTED_KEYS:
        return "[redactado]"
    return value


def configure_logging(level: str) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level.upper())
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def log(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    logger.log(level, message, extra={"fields": fields})
