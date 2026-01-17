"""Integration tests for circuit breaker with scraper tasks."""

import pytest
from champion.utils.circuit_breaker import CircuitBreakerOpen


class TestCircuitBreakerIntegrationWithTasks:
    """Integration tests for circuit breaker functionality."""

    def test_circuit_breaker_registry_isolation(self):
        """Test that NSE and BSE circuit breakers are isolated."""
        from champion.utils.circuit_breaker_registry import bse_breaker, nse_breaker

        # Reset both
        nse_breaker.reset()
        bse_breaker.reset()

        # Fail NSE breaker
        def nse_failing():
            raise RuntimeError("NSE error")

        for _ in range(5):
            try:
                nse_breaker.call(nse_failing)
            except RuntimeError:
                pass

        # NSE should be open, BSE should be closed
        assert nse_breaker.state.value == "open"
        assert bse_breaker.state.value == "closed"

        # BSE should still work
        def bse_success():
            return "BSE works"

        result = bse_breaker.call(bse_success)
        assert result == "BSE works"
        assert bse_breaker.state.value == "closed"

    def test_circuit_breaker_configuration(self):
        """Test that circuit breaker uses configuration values."""
        from champion.config import config
        from champion.utils.circuit_breaker_registry import bse_breaker, nse_breaker

        # Verify NSE breaker configuration
        assert nse_breaker.name == "nse"
        assert nse_breaker.failure_threshold == config.circuit_breaker.nse_failure_threshold
        assert nse_breaker.recovery_timeout == config.circuit_breaker.nse_recovery_timeout

        # Verify BSE breaker configuration
        assert bse_breaker.name == "bse"
        assert bse_breaker.failure_threshold == config.circuit_breaker.bse_failure_threshold
        assert bse_breaker.recovery_timeout == config.circuit_breaker.bse_recovery_timeout

    def test_circuit_breaker_fail_fast_behavior(self):
        """Test that circuit breaker fails fast when open."""
        from champion.utils.circuit_breaker_registry import nse_breaker

        # Reset breaker
        nse_breaker.reset()

        # Fail enough times to open circuit
        def failing_func():
            raise RuntimeError("Source is down")

        for _ in range(nse_breaker.failure_threshold):
            with pytest.raises(RuntimeError):
                nse_breaker.call(failing_func)

        assert nse_breaker.is_open

        # Next call should fail fast without executing the function
        call_count = 0

        def should_not_be_called():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("This should not be called")

        with pytest.raises(CircuitBreakerOpen, match="Circuit breaker 'nse' is open"):
            nse_breaker.call(should_not_be_called)

        # Function should not have been called
        assert call_count == 0
