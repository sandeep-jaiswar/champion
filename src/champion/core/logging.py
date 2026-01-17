"""Unified logging and observability module.

Provides structured logging with tracing capabilities.
"""

from __future__ import annotations

import logging
import sys
import uuid
from contextvars import ContextVar

import structlog

# Context variables for request tracing
_request_id_var: ContextVar[str] = ContextVar("request_id", default="")
_user_id_var: ContextVar[str] = ContextVar("user_id", default="")


def set_request_id(request_id: str) -> str:
    """Set request ID for current context."""
    _request_id_var.set(request_id)
    return request_id


def get_request_id() -> str:
    """Get current request ID or generate new one."""
    request_id = _request_id_var.get()
    if not request_id:
        request_id = str(uuid.uuid4())
        set_request_id(request_id)
    return request_id


def get_logger(name: str) -> structlog.BoundLogger:
    """Get structured logger instance.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Structlog bound logger with request context
    """
    logger = structlog.get_logger(name)
    # Bind context automatically
    return logger.bind(
        request_id=get_request_id(),
    )


def configure_logging(
    level: str = "INFO",
    format: str = "json",
    handlers: list | None = None,
) -> None:
    """Configure logging for Champion platform.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format: Output format (json, console)
        handlers: Optional additional handlers
    """
    # Configure Python standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )

    # Configure structlog
    if format == "json":
        processors = [
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    else:  # console format
        processors = [
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ]

    structlog.configure(
        processors=processors,  # type: ignore[arg-type]
        wrapper_class=structlog.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


# Initialize logging with defaults
configure_logging()
