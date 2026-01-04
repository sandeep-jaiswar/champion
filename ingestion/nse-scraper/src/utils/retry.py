"""Retry utilities with exponential backoff."""

from collections.abc import Callable
from typing import Any, TypeVar

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.utils.logger import get_logger

logger = get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def retry_on_network_error(
    max_attempts: int = 3, min_wait: int = 1, max_wait: int = 60
) -> Callable[[F], F]:
    """Decorator to retry on network errors with exponential backoff.

    Args:
        max_attempts: Maximum retry attempts
        min_wait: Minimum wait time between retries (seconds)
        max_wait: Maximum wait time between retries (seconds)

    Returns:
        Decorated function with retry logic
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=lambda retry_state: logger.warning(
            "Retrying after error",
            attempt=retry_state.attempt_number,
            wait=retry_state.next_action.sleep,
        ),
    )
