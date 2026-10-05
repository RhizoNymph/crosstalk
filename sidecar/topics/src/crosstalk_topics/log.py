"""Structured JSON logging: one object per line on stdout.

Every record carries `ts` (RFC 3339 UTC, microseconds), `level`, `logger`
and `msg`, plus the key-value fields passed to `event`.
"""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

_FIELDS = "crosstalk_fields"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        stamp = datetime.fromtimestamp(record.created, UTC)
        entry: dict[str, Any] = {
            "ts": stamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        fields = getattr(record, _FIELDS, None)
        if isinstance(fields, dict):
            for key, value in fields.items():
                entry.setdefault(key, value)
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str, separators=(",", ":"))


def configure(level: int) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)


def event(logger: logging.Logger, level: int, msg: str, /, **fields: Any) -> None:
    """Log `msg` with key-value `fields`."""
    logger.log(level, msg, extra={_FIELDS: fields})


def exception(logger: logging.Logger, msg: str, /, **fields: Any) -> None:
    """Log `msg` at error level with the active exception and `fields`."""
    logger.error(msg, exc_info=True, extra={_FIELDS: fields})
