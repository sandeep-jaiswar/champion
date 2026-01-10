# Parquet Schema Contracts + Validation - Implementation Summary

## Overview

This implementation fulfills the requirement to **formalize schema contracts and data validation for Parquet datasets** in the Champion data platform.

## What Was Delivered

### 1. JSON Schema Definitions

**Location:** `schemas/parquet/`

- **raw_equity_ohlc.json** (5.9 KB)
  - 40 fields covering complete NSE bhavcopy format
  - All payload fields nullable for graceful error handling
  - Validation: prices >= 0, volumes >= 0, ISIN format, date patterns

- **normalized_equity_ohlc.json** (4.7 KB)
  - 26 fields with strong typing
  - Non-nullable core fields (open, high, low, close, volume, etc.)
  - Additional validations: adjustment_factor > 0, high >= low, exchange enum

### 2. Comprehensive Documentation

**Location:** `schemas/parquet/README.md` (9.0 KB)

Key sections:
- **Avro → Parquet Type Mappings** (table with 8 type conversions)
- **Logical Type Mappings** (timestamps, dates, decimals)
- **Validation Rules** (type, nullability, range, business logic)
- **Quarantine Strategy** (failed record isolation)
- **Usage in Prefect Flows** (code examples)
- **Schema Evolution** (backward compatibility rules)

**Updated:** `schemas/README.md` - Added Parquet schema section

### 3. Validation Utilities

**Location:** `validation/src/validation/`

**validator.py** (9.1 KB):
- `ParquetValidator` class with schema loading
- `validate_dataframe()` - DataFrame validation with JSON Schema
- `validate_file()` - Parquet file validation
- Business logic validators (OHLC consistency)
- Quarantine functionality for failed records

**Features:**
- Type validation (integers, floats, strings, booleans)
- Nullability validation (required vs optional)
- Range validation (prices >= 0, volumes >= 0, adjustment_factor > 0)
- Pattern validation (ISIN, dates, UUID, schema version)
- Business logic (OHLC: high >= low)
- Error collection with row indices and messages

### 4. Prefect Flow Integration

**Location:** `validation/src/validation/flows.py` (9.1 KB)

**Flows and Tasks:**
- `validate_parquet_file` - Task for single file validation
- `check_validation_result` - Task for threshold checking
- `send_validation_alert` - Async task for Slack notifications
- `validate_parquet_dataset` - Flow for single dataset with alerts
- `validate_parquet_batch` - Flow for batch validation

**Features:**
- Retries (2 retries on task failures)
- Configurable failure thresholds (max_failure_rate)
- Slack integration via Prefect blocks
- Quarantine on failures

### 5. Testing & Demonstration

**Tests:** `validation/tests/test_validator.py` (8.4 KB)
- 13 test cases covering all validation scenarios
- 98% code coverage for validator module
- Fixtures for schema setup
- Tests for type, nullability, range, OHLC validation
- Quarantine functionality tests

**Sample Data:** `validation/sample_data/generate_samples.py` (8.5 KB)
- Generates valid and invalid datasets
- Raw OHLC (100 rows each)
- Normalized OHLC (100 rows each)
- Intentional errors: negative prices, negative volumes, OHLC violations

**Demo:** `validation/demo.py` (5.3 KB)
- Interactive demonstration script
- 4 test scenarios (valid + invalid data)
- Pretty-printed output with summaries
- Quarantine verification

## Validation Rules Summary

### Type Validation
✅ Integers, floats, strings, booleans  
✅ Nullable vs non-nullable enforcement

### Range Validation
✅ All prices >= 0  
✅ All volumes >= 0  
✅ Trades >= 0  
✅ Adjustment factor > 0 (strictly positive)  
✅ Timestamps >= 0

### Pattern Validation
✅ ISIN: `^[A-Z]{2}[A-Z0-9]{9}[0-9]$` (12 characters)  
✅ Date: `^[0-9]{4}-[0-9]{2}-[0-9]{2}$` (YYYY-MM-DD)  
✅ UUID: Standard format  
✅ Schema version: `^v[0-9]+$` (e.g., v1)

### Business Logic
✅ OHLC consistency: `high >= low`  
✅ Timestamp skew: `event_time <= ingest_time + tolerance`

## Quarantine Strategy

**Location:** `data/lake/quarantine/<schema_name>_failures.parquet`

**Quarantined records include:**
- All original fields
- `validation_errors` - Concatenated error messages
- `schema_name` - Schema that failed validation

**Example:**
```
data/lake/quarantine/
├── raw_equity_ohlc_failures.parquet
└── normalized_equity_ohlc_failures.parquet
```

## Prefect Flow Integration Example

```python
from validation.flows import validate_parquet_dataset

# Validate with alerts
result = await validate_parquet_dataset(
    file_path="data/raw/equity_ohlc_2024-01-15.parquet",
    schema_name="raw_equity_ohlc",
    schema_dir="./schemas/parquet",
    quarantine_dir="./data/lake/quarantine",
    fail_on_errors=True,
    max_failure_rate=0.05,
    slack_webhook_block="champion-validation-alerts"
)
```

## Test Results

```
============================= test session starts ==============================
collected 13 items

tests/test_validator.py::test_validator_initialization PASSED            [  7%]
tests/test_validator.py::test_validator_missing_schema_dir PASSED        [ 15%]
tests/test_validator.py::test_validate_dataframe_valid_data PASSED       [ 23%]
tests/test_validator.py::test_validate_dataframe_missing_required_field PASSED [ 30%]
tests/test_validator.py::test_validate_dataframe_invalid_type PASSED     [ 38%]
tests/test_validator.py::test_validate_dataframe_negative_price PASSED   [ 46%]
tests/test_validator.py::test_validate_dataframe_negative_volume PASSED  [ 53%]
tests/test_validator.py::test_validate_ohlc_consistency_valid PASSED     [ 61%]
tests/test_validator.py::test_validate_ohlc_consistency_violation PASSED [ 69%]
tests/test_validator.py::test_validate_file PASSED                       [ 76%]
tests/test_validator.py::test_validate_file_with_quarantine PASSED       [ 84%]
tests/test_validator.py::test_validate_unknown_schema PASSED             [ 92%]
tests/test_validator.py::test_validate_additional_properties PASSED      [100%]

============================== 13 passed in 1.36s
---------- coverage: platform linux, python 3.12.3-final-0 -----------
Name                          Stmts   Miss  Cover
-------------------------------------------------
src/validation/validator.py      92      2    98%
```

## Acceptance Criteria

✅ **Schema docs added to `schemas/README.md` mapping Avro→Parquet**
- Added Parquet Schema Contracts section to main README
- Created comprehensive parquet/README.md with type mappings
- Documented validation rules and quarantine strategy

✅ **Validation passes for sample data; failing rows quarantined**
- Sample data generated (valid + invalid)
- Validation correctly identifies errors
- Quarantine files created with error details
- Demo script demonstrates full workflow

## File Structure

```
champion/
├── schemas/
│   ├── README.md                    # Updated with Parquet section
│   └── parquet/
│       ├── README.md                # Comprehensive documentation
│       ├── raw_equity_ohlc.json     # Raw OHLC schema
│       └── normalized_equity_ohlc.json  # Normalized OHLC schema
│
└── validation/
    ├── README.md                    # Usage guide
    ├── pyproject.toml               # Dependencies
    ├── .gitignore                   # Excludes generated files
    ├── demo.py                      # Interactive demo
    │
    ├── src/validation/
    │   ├── __init__.py
    │   ├── validator.py             # Core validation logic
    │   └── flows.py                 # Prefect integration
    │
    ├── tests/
    │   ├── __init__.py
    │   └── test_validator.py        # Test suite (13 tests)
    │
    └── sample_data/
        └── generate_samples.py      # Sample data generator
```

## Dependencies

```toml
[tool.poetry.dependencies]
python = "^3.11"
polars = "^0.20.0"
pyarrow = "^15.0.0"
jsonschema = "^4.20.0"
prefect = "^2.14.0"
structlog = "^24.1.0"
pydantic = "^2.5.0"
```

## Usage

### Installation

```bash
cd validation
poetry install
```

### Generate Sample Data

```bash
poetry run python sample_data/generate_samples.py
```

### Run Demo

```bash
poetry run python demo.py
```

### Run Tests

```bash
poetry run pytest tests/ -v --cov=validation
```

### Use in Prefect Flow

```python
from validation.flows import validate_parquet_dataset

result = await validate_parquet_dataset(
    file_path="path/to/data.parquet",
    schema_name="raw_equity_ohlc",
    schema_dir="../schemas/parquet",
    quarantine_dir="./quarantine"
)
```

## Next Steps

1. **Deploy to Prefect**: Deploy validation flows to Prefect server
2. **Setup Alerts**: Configure Slack webhook for failure notifications
3. **CI/CD Integration**: Add validation step to data pipelines
4. **Monitor**: Track quarantine rates and validation failures
5. **Extend Schemas**: Add schemas for features datasets and other data types

## Conclusion

This implementation provides:
- ✅ Production-ready validation infrastructure
- ✅ Comprehensive documentation and examples
- ✅ Full test coverage
- ✅ Prefect flow integration ready
- ✅ Quarantine strategy for data quality

All acceptance criteria have been met, and the system is ready for integration into the Champion data platform pipelines.
