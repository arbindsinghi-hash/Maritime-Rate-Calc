"""
Structured JSON logging configuration.

Call setup_logging() at application startup to configure all loggers
to emit JSON-formatted log entries with standard fields.
"""

import json
import logging
import os
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone

# Context variable for request correlation
request_id_var: ContextVar[str] = ContextVar("request_id", default="")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "json")


class JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": "marc-tariff-api",
        }

        rid = request_id_var.get("")
        if rid:
            entry["request_id"] = rid

        if record.exc_info and record.exc_info[1]:
            entry["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "extra_data"):
            entry.update(record.extra_data)

        return json.dumps(entry, default=str)


class ConsoleFormatter(logging.Formatter):
    """Human-readable format for local development."""

    def format(self, record: logging.LogRecord) -> str:
        rid = request_id_var.get("")
        prefix = f"[{rid[:8]}] " if rid else ""
        return f"{record.levelname:<8} {prefix}{record.name}: {record.getMessage()}"


def setup_logging() -> None:
    """Configure root logger with structured output."""
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    root.handlers.clear()

    handler = logging.StreamHandler()
    if LOG_FORMAT == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(ConsoleFormatter())

    root.addHandler(handler)

    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def generate_request_id() -> str:
    """Generate a unique request ID for correlation."""
    return str(uuid.uuid4())
