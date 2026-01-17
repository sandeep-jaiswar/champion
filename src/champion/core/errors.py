"""Champion platform error hierarchy.

Provides domain-specific exceptions with recovery strategies.
"""

from __future__ import annotations
from typing import Optional


class ChampionError(Exception):
    """Base exception for all Champion platform errors.
    
    Attributes:
        message: Human-readable error description
        code: Machine-readable error code for logging/monitoring
        retryable: Whether operation can be safely retried
        recovery_hint: Suggested recovery action
    """

    def __init__(
        self,
        message: str,
        code: str = "INTERNAL_ERROR",
        retryable: bool = False,
        recovery_hint: Optional[str] = None,
    ):
        self.message = message
        self.code = code
        self.retryable = retryable
        self.recovery_hint = recovery_hint
        super().__init__(message)

    def __str__(self) -> str:
        base = f"[{self.code}] {self.message}"
        if self.recovery_hint:
            base += f" ({self.recovery_hint})"
        return base


class ValidationError(ChampionError):
    """Raised when data validation fails.
    
    Examples:
        - JSON schema violation
        - Data type mismatch
        - Business logic violation
    """

    def __init__(
        self,
        message: str,
        details: Optional[dict] = None,
        recovery_hint: Optional[str] = None,
    ):
        super().__init__(
            message,
            code="VALIDATION_ERROR",
            retryable=False,
            recovery_hint=recovery_hint or "Review data format and schema",
        )
        self.details = details or {}


class DataError(ChampionError):
    """Raised on data processing failures.
    
    Examples:
        - Read/write failures
        - Encoding issues
        - Memory limits
    """

    def __init__(
        self,
        message: str,
        retryable: bool = True,
        recovery_hint: Optional[str] = None,
    ):
        super().__init__(
            message,
            code="DATA_ERROR",
            retryable=retryable,
            recovery_hint=recovery_hint or "Check data source and disk space",
        )


class IntegrationError(ChampionError):
    """Raised on external integration failures.
    
    Examples:
        - HTTP request failures
        - Database connection errors
        - Kafka broker unreachable
    """

    def __init__(
        self,
        service: str,
        message: str,
        retryable: bool = True,
        recovery_hint: Optional[str] = None,
    ):
        super().__init__(
            f"{service}: {message}",
            code="INTEGRATION_ERROR",
            retryable=retryable,
            recovery_hint=recovery_hint or f"Check {service} connectivity",
        )
        self.service = service


class ConfigError(ChampionError):
    """Raised on configuration errors."""

    def __init__(self, message: str, recovery_hint: Optional[str] = None):
        super().__init__(
            message,
            code="CONFIG_ERROR",
            retryable=False,
            recovery_hint=recovery_hint or "Check environment variables and config files",
        )


class NotImplementedError(ChampionError):
    """Raised when feature is not implemented."""

    def __init__(self, feature: str):
        super().__init__(
            f"{feature} not implemented",
            code="NOT_IMPLEMENTED",
            retryable=False,
        )
