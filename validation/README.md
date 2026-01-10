# Data Validation Utilities

## Overview

This package provides data validation utilities for Parquet datasets in the Champion data platform. It includes:

- JSON Schema validation for Parquet files
- Business logic validation (OHLC consistency, etc.)
- Quarantine functionality for failed records
- Prefect flow integration with alerts

## Installation

```bash
cd validation
poetry install
```

## Usage

### Standalone Validation

```python
from pathlib import Path
from validation.validator import ParquetValidator

# Initialize validator
validator = ParquetValidator(schema_dir=Path("../schemas/parquet"))

# Validate a file
result = validator.validate_file(
    file_path=Path("data/raw_equity_ohlc.parquet"),
    schema_name="raw_equity_ohlc",
    quarantine_dir=Path("data/quarantine")
)

print(f"Total rows: {result.total_rows}")
print(f"Valid rows: {result.valid_rows}")
print(f"Critical failures: {result.critical_failures}")
```

### Prefect Flow Integration

```python
import asyncio
from validation.flows import validate_parquet_dataset

# Run validation flow
result = asyncio.run(
    validate_parquet_dataset(
        file_path="data/raw_equity_ohlc.parquet",
        schema_name="raw_equity_ohlc",
        schema_dir="../schemas/parquet",
        quarantine_dir="data/quarantine",
        fail_on_errors=True,
        max_failure_rate=0.05,
        slack_webhook_block="my-slack-webhook"  # Optional
    )
)
```

### Batch Validation

```python
from validation.flows import validate_parquet_batch

# Validate multiple files
results = asyncio.run(
    validate_parquet_batch(
        file_paths=[
            "data/2024-01-01.parquet",
            "data/2024-01-02.parquet",
            "data/2024-01-03.parquet",
        ],
        schema_name="raw_equity_ohlc",
        schema_dir="../schemas/parquet",
        quarantine_dir="data/quarantine"
    )
)
```

## Validation Rules

### Type Validation

- Validates data types match JSON schema definitions
- Ensures integers, floats, strings, booleans are correct types
- Checks nullable fields are properly handled

### Nullability Validation

- Ensures required fields are non-null
- Validates optional fields can be null or valid values

### Range Validation

- Prices: >= 0 (no negative prices)
- Volumes: >= 0 (no negative volume)
- Adjustment factors: > 0 (strictly positive)
- Timestamps: >= 0 (valid Unix epoch)

### Business Logic Validation

#### OHLC Consistency

- `high >= low` (critical - must pass)
- Other OHLC relationships are not enforced due to gaps

#### Timestamp Validation

- `event_time <= ingest_time + tolerance`
- Tolerance: 5 minutes for EOD, 30 seconds for real-time

## Quarantine Strategy

Failed records are written to separate Parquet files:

```
data/lake/quarantine/
├── raw_equity_ohlc_failures.parquet
├── normalized_equity_ohlc_failures.parquet
└── ...
```

Each quarantined record includes:
- All original fields
- `validation_errors`: Concatenated error messages
- `schema_name`: Schema that failed validation

## Prefect Configuration

### Setup Slack Notifications (Optional)

```python
from prefect.blocks.notifications import SlackWebhook

# Create webhook block
webhook = SlackWebhook(url="https://hooks.slack.com/services/YOUR/WEBHOOK/URL")
await webhook.save("champion-validation-alerts")
```

### Deploy Flow

```python
from validation.flows import validate_parquet_dataset

# Deploy to Prefect server
await validate_parquet_dataset.deploy(
    name="validate-ohlc-data",
    work_pool_name="default",
    parameters={
        "schema_dir": "/home/runner/work/champion/champion/schemas/parquet",
        "quarantine_dir": "/home/runner/work/champion/champion/data/lake/quarantine",
        "fail_on_errors": True,
        "slack_webhook_block": "champion-validation-alerts"
    }
)
```

## Testing

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=validation --cov-report=html

# Run specific test
poetry run pytest tests/test_validator.py::test_validate_raw_ohlc
```

## Development

### Code Quality

```bash
# Format code
poetry run black src/ tests/

# Lint code
poetry run ruff check src/ tests/

# Type check
poetry run mypy src/
```

## Example: Integration in Data Pipeline

```python
from prefect import flow, task
from validation.flows import validate_parquet_dataset

@task
def ingest_nse_data(date: str):
    """Ingest NSE data for a date."""
    # ... ingestion logic
    return "data/raw/2024-01-01.parquet"

@task
def normalize_data(raw_file: str):
    """Normalize raw data."""
    # ... normalization logic
    return "data/normalized/2024-01-01.parquet"

@flow(name="nse-daily-pipeline")
async def nse_daily_pipeline(date: str):
    """Complete NSE data pipeline with validation."""
    
    # Step 1: Ingest raw data
    raw_file = ingest_nse_data(date)
    
    # Step 2: Validate raw data
    await validate_parquet_dataset(
        file_path=raw_file,
        schema_name="raw_equity_ohlc",
        schema_dir="../schemas/parquet",
        quarantine_dir="data/quarantine",
        fail_on_errors=True
    )
    
    # Step 3: Normalize data
    normalized_file = normalize_data(raw_file)
    
    # Step 4: Validate normalized data
    await validate_parquet_dataset(
        file_path=normalized_file,
        schema_name="normalized_equity_ohlc",
        schema_dir="../schemas/parquet",
        quarantine_dir="data/quarantine",
        fail_on_errors=True
    )
    
    return normalized_file
```

## Architecture

```
validation/
├── src/
│   └── validation/
│       ├── __init__.py
│       ├── validator.py    # Core validation logic
│       └── flows.py        # Prefect flow integration
├── tests/
│   ├── __init__.py
│   ├── test_validator.py  # Validator tests
│   └── test_flows.py      # Flow tests
├── pyproject.toml
└── README.md
```

## API Reference

### ParquetValidator

Main validation class for Parquet files.

**Methods:**
- `__init__(schema_dir: Path)`: Initialize with schema directory
- `validate_dataframe(df: pl.DataFrame, schema_name: str) -> ValidationResult`: Validate DataFrame
- `validate_file(file_path: Path, schema_name: str, quarantine_dir: Optional[Path]) -> ValidationResult`: Validate file

### ValidationResult

Dataclass containing validation results.

**Fields:**
- `total_rows: int`: Total number of rows
- `valid_rows: int`: Number of valid rows
- `critical_failures: int`: Number of critical failures
- `warnings: int`: Number of warnings
- `error_details: List[Dict]`: Detailed error information

### Prefect Flows

- `validate_parquet_dataset`: Validate single file with alerts
- `validate_parquet_batch`: Validate multiple files

## License

Internal use only - Champion team.
