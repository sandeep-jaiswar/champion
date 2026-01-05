# NSE Scraper Tests

This directory contains the test suite for the NSE Scraper project.

## Directory Structure

```text
tests/
├── manual/              # Manual test scripts for development
│   ├── test_all_endpoints.py       # Comprehensive endpoint test
│   ├── test_metrics_server.py      # Metrics server test
│   └── show_parsed_event.py        # Event structure viewer
├── unit/               # Unit tests (pytest)
├── integration/        # Integration tests (pytest)
└── README.md          # This file
```

## Manual Tests

Manual test scripts are located in `tests/manual/` and are meant for interactive testing
during development.

### Run All Endpoints Test

Tests all scraper endpoints and displays metrics:

```bash
python -m tests.manual.test_all_endpoints
```

This will:

- Start Prometheus metrics server on port 9090
- Test bhavcopy scraper with sample data
- Display metrics at <http://localhost:9090/metrics>
- Keep server running until Ctrl+C

### Test Metrics Server

Start standalone metrics server:

```bash
python -m tests.manual.test_metrics_server
```

### View Parsed Events

Display parsed event structure from a bhavcopy file:

```bash
# Default file
python -m tests.manual.show_parsed_event

# Custom file and date
python -m tests.manual.show_parsed_event \
    --file data/BhavCopy_NSE_CM_20240103.csv \
    --date 2024-01-03 \
    --count 3
```

## Unit Tests

Unit tests use pytest and test individual components in isolation.

```bash
# Run all unit tests
pytest tests/unit/

# Run with coverage
pytest --cov=src tests/unit/
```

### Unit Test Structure (Planned)

- `test_parsers.py` - Test CSV parsers (bhavcopy, symbol master, CA)
- `test_config.py` - Test configuration loading and validation
- `test_metrics.py` - Test metrics collection
- `test_retry.py` - Test retry logic

## Integration Tests

Integration tests verify end-to-end workflows.

```bash
# Run all integration tests
pytest tests/integration/

# Requires Docker for Kafka/Schema Registry
docker-compose up -d
pytest tests/integration/
```

### Integration Test Structure (Planned)

- `test_scrape_workflow.py` - Test download → parse → produce pipeline
- `test_kafka_integration.py` - Test Kafka/Schema Registry integration
- `test_backfill.py` - Test date range backfill logic

## Test Data

Sample test data files are located in the `data/` directory:

- `BhavCopy_NSE_CM_20240102.csv` - Sample CM bhavcopy (2658 records)
- `BhavCopy_NSE_CM_20240103.csv` - Sample CM bhavcopy (2660 records)
- `BhavCopy_NSE_CM_20240104.csv` - Sample CM bhavcopy (2668 records)
- `EQUITY_L.csv` - Sample symbol master (2224 symbols)

## Running Tests in CI/CD

Tests are configured to run in GitHub Actions (see `.github/workflows/`):

```yaml
- name: Run tests
  run: |
    poetry install
    poetry run pytest tests/unit/ -v
    poetry run pytest tests/integration/ -v --requires-docker
```

## Writing New Tests

### Unit Test Template

```python
"""Test module for XYZ."""

import pytest
from src.module import function


class TestFunction:
    """Tests for function()."""
    
    def test_valid_input(self):
        """Test with valid input."""
        result = function("valid")
        assert result == expected
    
    def test_invalid_input(self):
        """Test with invalid input."""
        with pytest.raises(ValueError):
            function("invalid")
```

### Integration Test Template

```python
"""Integration test for XYZ workflow."""

import pytest


@pytest.mark.integration
@pytest.mark.requires_docker
class TestWorkflow:
    """Tests for end-to-end workflow."""
    
    def test_complete_workflow(self, kafka_container):
        """Test complete scrape → parse → produce workflow."""
        # Setup
        # Execute
        # Assert
        pass
```

## Best Practices

1. **Isolation**: Unit tests should not depend on external services
2. **Fixtures**: Use pytest fixtures for common test setup
3. **Mocking**: Mock external dependencies (HTTP, Kafka, Schema Registry)
4. **Determinism**: Tests should be deterministic and repeatable
5. **Speed**: Unit tests should be fast (<1s each)
6. **Coverage**: Aim for >80% code coverage
7. **Documentation**: Add docstrings to test classes and methods
