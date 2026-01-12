"""Circuit breaker pattern implementation for data sources.

This module provides a circuit breaker to prevent cascading failures
when data sources (NSE/BSE) go down. It implements three states:
- CLOSED: Normal operation
- OPEN: Source is down, fail fast
- HALF_OPEN: Testing recovery

Example:
    >>> breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=300)
    >>> result = breaker.call(scraper.scrape, trade_date)
"""

import time
from collections.abc import Callable
from enum import Enum
from typing import Any, TypeVar

from champion.utils.logger import get_logger
from champion.utils.metrics import (
    circuit_breaker_failures,
    circuit_breaker_state,
    circuit_breaker_state_transitions,
)

logger = get_logger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Source is down, fail fast
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open."""

    def __init__(self, message: str = "Circuit breaker is open - source unavailable"):
        """Initialize exception.

        Args:
            message: Error message
        """
        self.message = message
        super().__init__(self.message)


class CircuitBreaker:
    """Circuit breaker for preventing cascading failures.

    The circuit breaker monitors failures and transitions between states:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Too many failures, requests fail immediately
    - HALF_OPEN: Testing if service recovered

    Attributes:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Seconds to wait before testing recovery
        state: Current circuit state
        failure_count: Current consecutive failure count
        last_failure_time: Timestamp of last failure
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 300,
    ):
        """Initialize circuit breaker.

        Args:
            name: Name of the circuit (for logging/metrics)
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: float | None = None
        self.logger = get_logger(f"{__name__}.{name}")

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute function with circuit breaker protection.

        Args:
            func: Function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Result from func

        Raises:
            CircuitBreakerOpen: If circuit is open and recovery timeout not reached
            Exception: Any exception raised by func
        """
        # Check if we should attempt recovery
        if self.state == CircuitState.OPEN:
            if (
                self.last_failure_time
                and time.time() - self.last_failure_time > self.recovery_timeout
            ):
                self.logger.info(
                    "circuit_breaker_attempting_recovery",
                    state="HALF_OPEN",
                    recovery_timeout=self.recovery_timeout,
                )
                self.state = CircuitState.HALF_OPEN

                # Update metrics
                circuit_breaker_state.labels(source=self.name).set(1)  # 1 = HALF_OPEN
                circuit_breaker_state_transitions.labels(
                    source=self.name, from_state="open", to_state="half_open"
                ).inc()
            else:
                self.logger.warning(
                    "circuit_breaker_open_failing_fast",
                    state="OPEN",
                    failure_count=self.failure_count,
                )
                raise CircuitBreakerOpen(
                    f"Circuit breaker '{self.name}' is open - source unavailable"
                )

        # Attempt the call
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure(e)
            raise

    def _on_success(self) -> None:
        """Handle successful call - reset circuit to closed state."""
        old_state = self.state
        if self.state != CircuitState.CLOSED:
            self.logger.info(
                "circuit_breaker_recovered",
                old_state=self.state.value,
                new_state="CLOSED",
            )
            # Update metrics for state transition
            circuit_breaker_state_transitions.labels(
                source=self.name, from_state=old_state.value, to_state="closed"
            ).inc()

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None

        # Update state metric
        circuit_breaker_state.labels(source=self.name).set(0)  # 0 = CLOSED

    def _on_failure(self, error: Exception) -> None:
        """Handle failed call - increment failures and potentially open circuit.

        Args:
            error: Exception that occurred
        """
        self.failure_count += 1
        self.last_failure_time = time.time()

        # Track failure in metrics
        circuit_breaker_failures.labels(source=self.name).inc()

        self.logger.warning(
            "circuit_breaker_failure",
            state=self.state.value,
            failure_count=self.failure_count,
            threshold=self.failure_threshold,
            error=str(error),
        )

        if self.failure_count >= self.failure_threshold:
            old_state = self.state
            self.logger.critical(
                "circuit_breaker_opened",
                state="OPEN",
                failure_count=self.failure_count,
                threshold=self.failure_threshold,
            )
            self.state = CircuitState.OPEN

            # Update metrics
            circuit_breaker_state.labels(source=self.name).set(2)  # 2 = OPEN
            circuit_breaker_state_transitions.labels(
                source=self.name, from_state=old_state.value, to_state="open"
            ).inc()

    def reset(self) -> None:
        """Manually reset circuit breaker to closed state."""
        self.logger.info("circuit_breaker_manual_reset", old_state=self.state.value)
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None

    @property
    def is_open(self) -> bool:
        """Check if circuit breaker is open.

        Returns:
            True if circuit is open, False otherwise
        """
        return self.state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        """Check if circuit breaker is closed.

        Returns:
            True if circuit is closed, False otherwise
        """
        return self.state == CircuitState.CLOSED
