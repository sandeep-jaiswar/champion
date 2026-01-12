"""Circuit breaker registry for managing data source circuit breakers.

This module provides a centralized registry for circuit breakers
used across different data sources (NSE, BSE, etc.).
"""

from champion.config import config
from champion.utils.circuit_breaker import CircuitBreaker

# Global circuit breaker instances for each data source
nse_breaker = CircuitBreaker(
    name="nse",
    failure_threshold=config.circuit_breaker.nse_failure_threshold,
    recovery_timeout=config.circuit_breaker.nse_recovery_timeout,
)

bse_breaker = CircuitBreaker(
    name="bse",
    failure_threshold=config.circuit_breaker.bse_failure_threshold,
    recovery_timeout=config.circuit_breaker.bse_recovery_timeout,
)

__all__ = ["nse_breaker", "bse_breaker"]
