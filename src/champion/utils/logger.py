"""Structured logging utilities with trace ID support."""

import logging
import sys
import uuid
from contextvars import ContextVar
from typing import Any

import structlog
from structlog.typing import FilteringBoundLogger, Processor

# Context variable to store trace ID across async operations
_trace_id_var: ContextVar[str | None] = ContextVar("trace_id", default=None)


def set_trace_id(trace_id: str | None = None) -> str:
    """Set trace ID for the current context.

    Args:
        trace_id: Optional trace ID. If not provided, generates a new UUID.

    Returns:
        The trace ID that was set
    """
    if trace_id is None:
        trace_id = str(uuid.uuid4())
    _trace_id_var.set(trace_id)
    return trace_id


def get_trace_id() -> str | None:
    """Get the current trace ID from context.

    Returns:
        Current trace ID or None if not set
    """
    return _trace_id_var.get()


def clear_trace_id() -> None:
    """Clear the trace ID from the current context."""
    _trace_id_var.set(None)


def add_trace_id(
    logger: logging.Logger, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Processor to add trace ID to log records.

    Args:
        logger: Logger instance
        method_name: Logging method name
        event_dict: Event dictionary

    Returns:
        Updated event dictionary with trace_id
    """
    trace_id = get_trace_id()
    if trace_id is not None:
        event_dict["trace_id"] = trace_id
    return event_dict


def get_logger(name: str) -> FilteringBoundLogger:
    """Get a structured logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


def configure_logging(log_level: str = "INFO", log_format: str = "json") -> None:
    """Configure application-wide logging.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Output format ('json' or 'console')

    Raises:
        ValueError: If log_level is not a valid logging level
    """
    # Validate log level
    valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    log_level_upper = log_level.upper()
    if log_level_upper not in valid_levels:
        raise ValueError(
            f"Invalid log_level '{log_level}'. Must be one of: {', '.join(valid_levels)}"
        )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level_upper),
    )

    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        add_trace_id,  # Add trace ID to all log records
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    if log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, log_level_upper)),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
