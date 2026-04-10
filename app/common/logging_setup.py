"""
JSON structured logging configuration for all services.
Ensures that all logs include: timestamp, service, request_id, severity, message.
No sensitive data (passwords, tokens) should be logged.
"""

import logging
import json
import sys
from datetime import datetime
from typing import Any


class JSONFormatter(logging.Formatter):
    """
    Custom formatter that outputs structured JSON logs.
    This allows centralized log aggregation and querying.
    """

    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": self.service_name,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add request_id if present in record
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id

        # Add extra fields if present
        if hasattr(record, "extra_data"):
            log_data.update(record.extra_data)

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logging(service_name: str, level: str = "INFO") -> logging.Logger:
    """
    Configure structured JSON logging for a service.

    Args:
        service_name: Name of the service (e.g., "customer-facing")
        level: Log level (INFO, DEBUG, WARNING, ERROR)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(service_name)
    logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers to prevent duplicates
    logger.handlers.clear()

    # Create console handler with JSON formatter
    handler = logging.StreamHandler(sys.stdout)
    formatter = JSONFormatter(service_name)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    # Prevent propagation to avoid duplicate logs
    logger.propagate = False

    return logger


def get_logger(service_name: str) -> logging.Logger:
    """Get logger for a service. Configure if not already done."""
    logger = logging.getLogger(service_name)
    if not logger.handlers:
        setup_logging(service_name)
    return logger
