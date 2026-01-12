#!/usr/bin/env python
"""Demo script to show circuit breaker functionality."""

import time
from champion.utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpen, CircuitState


def demo_circuit_breaker():
    """Demonstrate circuit breaker state transitions."""
    print("=" * 60)
    print("Circuit Breaker Pattern Demonstration")
    print("=" * 60)
    print()

    # Create a circuit breaker with low thresholds for demo
    breaker = CircuitBreaker(name="demo", failure_threshold=3, recovery_timeout=2)

    print(f"Initial state: {breaker.state.value}")
    print(f"Failure threshold: {breaker.failure_threshold}")
    print(f"Recovery timeout: {breaker.recovery_timeout}s")
    print()

    # Simulate a failing service
    def failing_service():
        """Simulate a service that always fails."""
        raise RuntimeError("Service unavailable")

    # Test 1: Trigger failures to open circuit
    print("Test 1: Triggering failures to open circuit...")
    print("-" * 60)
    for i in range(1, 4):
        try:
            breaker.call(failing_service)
        except RuntimeError as e:
            print(f"  Attempt {i}: Failed - {e}")
            print(f"  State: {breaker.state.value}, Failures: {breaker.failure_count}")
    print()

    # Test 2: Circuit is now open, should fail fast
    print("Test 2: Circuit is open - attempting call (should fail fast)...")
    print("-" * 60)
    try:
        breaker.call(failing_service)
    except CircuitBreakerOpen as e:
        print(f"  ✓ Failed fast: {e}")
        print(f"  State: {breaker.state.value}")
    print()

    # Test 3: Wait for recovery timeout
    print(f"Test 3: Waiting {breaker.recovery_timeout}s for recovery timeout...")
    print("-" * 60)
    time.sleep(breaker.recovery_timeout + 0.5)
    print(f"  ✓ Recovery timeout passed")
    print()

    # Test 4: Circuit should transition to HALF_OPEN and succeed
    print("Test 4: Service recovered - attempting call...")
    print("-" * 60)

    def working_service():
        """Simulate a service that works."""
        return "Success!"

    try:
        result = breaker.call(working_service)
        print(f"  ✓ Call succeeded: {result}")
        print(f"  State: {breaker.state.value}, Failures: {breaker.failure_count}")
    except Exception as e:
        print(f"  ✗ Call failed: {e}")
    print()

    # Test 5: Circuit should be closed now, multiple successes
    print("Test 5: Circuit closed - multiple successful calls...")
    print("-" * 60)
    for i in range(1, 4):
        result = breaker.call(working_service)
        print(f"  Attempt {i}: {result} - State: {breaker.state.value}")
    print()

    print("=" * 60)
    print("Demo Complete!")
    print("=" * 60)


if __name__ == "__main__":
    demo_circuit_breaker()
