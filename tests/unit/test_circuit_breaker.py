"""Tests for circuit breaker functionality."""

import time
from unittest.mock import MagicMock

import pytest

from champion.utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpen, CircuitState


class TestCircuitBreaker:
    """Test suite for CircuitBreaker class."""

    def test_circuit_breaker_initialization(self):
        """Test that circuit breaker initializes with correct defaults."""
        breaker = CircuitBreaker(name="test", failure_threshold=3, recovery_timeout=60)

        assert breaker.name == "test"
        assert breaker.failure_threshold == 3
        assert breaker.recovery_timeout == 60
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0
        assert breaker.last_failure_time is None

    def test_circuit_breaker_successful_call(self):
        """Test that successful calls work normally in closed state."""
        breaker = CircuitBreaker(name="test", failure_threshold=3, recovery_timeout=60)

        def successful_func():
            return "success"

        result = breaker.call(successful_func)

        assert result == "success"
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0

    def test_circuit_breaker_single_failure(self):
        """Test that single failure increments counter but doesn't open circuit."""
        breaker = CircuitBreaker(name="test", failure_threshold=3, recovery_timeout=60)

        def failing_func():
            raise RuntimeError("Test error")

        with pytest.raises(RuntimeError, match="Test error"):
            breaker.call(failing_func)

        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 1
        assert breaker.last_failure_time is not None

    def test_circuit_breaker_opens_after_threshold(self):
        """Test that circuit opens after reaching failure threshold."""
        breaker = CircuitBreaker(name="test", failure_threshold=3, recovery_timeout=60)

        def failing_func():
            raise RuntimeError("Test error")

        # Trigger failures up to threshold
        for _ in range(3):
            with pytest.raises(RuntimeError):
                breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN
        assert breaker.failure_count == 3

    def test_circuit_breaker_fails_fast_when_open(self):
        """Test that circuit breaker fails fast when open."""
        breaker = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=60)

        def failing_func():
            raise RuntimeError("Test error")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(RuntimeError):
                breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN

        # Now it should fail fast without calling the function
        def should_not_be_called():
            pytest.fail("Function should not be called when circuit is open")

        with pytest.raises(CircuitBreakerOpen, match="Circuit breaker 'test' is open"):
            breaker.call(should_not_be_called)

    def test_circuit_breaker_transitions_to_half_open(self):
        """Test that circuit transitions to half-open after recovery timeout."""
        breaker = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=1)

        def failing_func():
            raise RuntimeError("Test error")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(RuntimeError):
                breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN

        # Wait for recovery timeout
        time.sleep(1.1)

        # Next call should transition to half-open
        def successful_func():
            return "recovered"

        result = breaker.call(successful_func)

        assert result == "recovered"
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0

    def test_circuit_breaker_reopens_on_half_open_failure(self):
        """Test that circuit reopens if call fails in half-open state."""
        breaker = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=1)

        def failing_func():
            raise RuntimeError("Test error")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(RuntimeError):
                breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN

        # Wait for recovery timeout
        time.sleep(1.1)

        # Next call should transition to half-open, but fail
        with pytest.raises(RuntimeError):
            breaker.call(failing_func)

        # Circuit should be open again (failure count was already at threshold)
        assert breaker.state == CircuitState.OPEN

    def test_circuit_breaker_recovery_after_half_open_success(self):
        """Test that circuit closes after successful call in half-open state."""
        breaker = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=1)

        def failing_func():
            raise RuntimeError("Test error")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(RuntimeError):
                breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN
        assert breaker.failure_count == 2

        # Wait for recovery timeout
        time.sleep(1.1)

        # Successful call should close the circuit
        def successful_func():
            return "recovered"

        result = breaker.call(successful_func)

        assert result == "recovered"
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0
        assert breaker.last_failure_time is None

    def test_circuit_breaker_manual_reset(self):
        """Test manual reset of circuit breaker."""
        breaker = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=60)

        def failing_func():
            raise RuntimeError("Test error")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(RuntimeError):
                breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN

        # Manual reset
        breaker.reset()

        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0
        assert breaker.last_failure_time is None

    def test_circuit_breaker_is_open_property(self):
        """Test is_open property."""
        breaker = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=60)

        assert not breaker.is_open

        def failing_func():
            raise RuntimeError("Test error")

        with pytest.raises(RuntimeError):
            breaker.call(failing_func)

        assert breaker.is_open

    def test_circuit_breaker_is_closed_property(self):
        """Test is_closed property."""
        breaker = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=60)

        assert breaker.is_closed

        def failing_func():
            raise RuntimeError("Test error")

        with pytest.raises(RuntimeError):
            breaker.call(failing_func)

        assert not breaker.is_closed

    def test_circuit_breaker_with_args_and_kwargs(self):
        """Test that circuit breaker correctly passes args and kwargs to function."""
        breaker = CircuitBreaker(name="test", failure_threshold=3, recovery_timeout=60)

        def func_with_args(a, b, c=None):
            return f"a={a}, b={b}, c={c}"

        result = breaker.call(func_with_args, 1, 2, c=3)

        assert result == "a=1, b=2, c=3"
        assert breaker.state == CircuitState.CLOSED

    def test_circuit_breaker_exception_propagation(self):
        """Test that exceptions are properly propagated."""
        breaker = CircuitBreaker(name="test", failure_threshold=3, recovery_timeout=60)

        class CustomException(Exception):
            pass

        def failing_func():
            raise CustomException("Custom error message")

        with pytest.raises(CustomException, match="Custom error message"):
            breaker.call(failing_func)

        assert breaker.failure_count == 1


class TestCircuitBreakerIntegration:
    """Integration tests for circuit breaker with mock scrapers."""

    def test_circuit_breaker_with_scraper_mock(self):
        """Test circuit breaker with a mock scraper."""
        breaker = CircuitBreaker(name="test_scraper", failure_threshold=3, recovery_timeout=1)

        mock_scraper = MagicMock()
        mock_scraper.scrape.side_effect = [
            RuntimeError("Network error"),
            RuntimeError("Network error"),
            RuntimeError("Network error"),
        ]

        # First three calls should fail and open the circuit
        for _ in range(3):
            with pytest.raises(RuntimeError):
                breaker.call(mock_scraper.scrape, "2024-01-01")

        assert breaker.state == CircuitState.OPEN
        assert mock_scraper.scrape.call_count == 3

        # Next call should fail fast without calling scraper
        with pytest.raises(CircuitBreakerOpen):
            breaker.call(mock_scraper.scrape, "2024-01-01")

        # Scraper should not have been called again
        assert mock_scraper.scrape.call_count == 3

        # Wait for recovery timeout
        time.sleep(1.1)

        # Now configure scraper to succeed
        mock_scraper.scrape.side_effect = None
        mock_scraper.scrape.return_value = "/path/to/file.csv"

        # Call should succeed and close circuit
        result = breaker.call(mock_scraper.scrape, "2024-01-01")

        assert result == "/path/to/file.csv"
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0

    def test_circuit_breaker_multiple_sources(self):
        """Test that multiple circuit breakers work independently."""
        nse_breaker = CircuitBreaker(name="nse", failure_threshold=2, recovery_timeout=60)
        bse_breaker = CircuitBreaker(name="bse", failure_threshold=2, recovery_timeout=60)

        def nse_failing_func():
            raise RuntimeError("NSE error")

        def bse_successful_func():
            return "BSE success"

        # Fail NSE breaker
        for _ in range(2):
            with pytest.raises(RuntimeError):
                nse_breaker.call(nse_failing_func)

        assert nse_breaker.state == CircuitState.OPEN
        assert bse_breaker.state == CircuitState.CLOSED

        # BSE should still work
        result = bse_breaker.call(bse_successful_func)
        assert result == "BSE success"
        assert bse_breaker.state == CircuitState.CLOSED
