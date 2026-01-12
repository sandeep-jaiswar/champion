# Data Validation Module

## Overview

This module provides end-to-end data validation for the Champion ETL pipeline, ensuring data quality before data reaches the warehouse. All data flows through validation before being written to Parquet files.

## Architecture

### Components

1. **ParquetValidator** (`validator.py`): Core validation engine
   - Validates DataFrames against JSON schemas using streaming validation
   - Memory-efficient batch processing (default: 10,000 rows/batch)
   - Performs business logic validations (e.g., OHLC consistency)
   - Quarantines failed records for investigation
   - Handles datasets of any size without OOM errors

2. **Validation Flows** (`flows.py`): Prefect task integration
   - `validate_parquet_file`: Validates individual Parquet files
   - `validate_parquet_dataset`: Complete validation flow with alerting
   - `validate_parquet_batch`: Batch validation for multiple files

3. **Write Wrappers** (`storage/parquet_io.py`):
   - `write_df_safe()`: Validates before writing to Parquet
   - Automatic quarantine of failed records
   - MLflow metrics logging

## Validation Contract

### All Data Must Pass Validation

**100% of data written to Parquet is validated** against JSON schemas in `schemas/parquet/`.

### Validation Process

```python
# 1. Data is validated against JSON schema
validator = ParquetValidator(schema_dir='schemas/parquet')
result = validator.validate_dataframe(df, 'normalized_equity_ohlc')

# 2. If validation fails:
#    - Critical failures are logged with details
#    - Failed records are quarantined to data/lake/quarantine/
#    - ValueError is raised to stop the pipeline
#    - MLflow metrics are logged

# 3. If validation passes:
#    - Data is written to Parquet
#    - Success metrics are logged to MLflow
```

### Integration Points

All write operations in the ETL pipeline now include validation:

1. **Task-level validation** (in `orchestration/tasks/`):
   - `macro_tasks.py`: `write_macro_parquet()`
   - `bulk_block_deals_tasks.py`: `write_bulk_block_deals_parquet()`
   - `trading_calendar_tasks.py`: `write_trading_calendar_parquet()`

2. **Flow-level validation** (in `orchestration/flows/`):
   - `flows.py`: `write_parquet()` task (via `PolarsBhavcopyParser`)
   - `macro_flow.py`: MLflow metrics tracking
   - `trading_calendar_flow.py`: MLflow metrics tracking
   - `bulk_block_deals_flow.py`: MLflow metrics tracking

3. **Parser validation** (in `parsers/`):
   - `PolarsBhavcopyParser.write_parquet()`: Optional validation parameter

## JSON Schemas

Schemas are located in `schemas/parquet/` and follow JSON Schema Draft 07 standard.

### Available Schemas

- `normalized_equity_ohlc.json`: Normalized equity OHLC data
- `raw_equity_ohlc.json`: Raw equity OHLC data
- `trading_calendar.json`: Trading calendar data
- `macro_indicators_jsonschema.json`: Macro economic indicators
- `bulk_block_deals_jsonschema.json`: Bulk and block deals
- `quarterly_financials.json`: Quarterly financial statements
- `shareholding_pattern.json`: Shareholding patterns

### Schema Structure

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Dataset Name",
  "type": "object",
  "required": ["field1", "field2"],
  "properties": {
    "field1": {
      "type": "string",
      "description": "Field description"
    },
    "field2": {
      "type": "number",
      "minimum": 0
    }
  }
}
```

## Business Logic Validations

Beyond JSON schema validation, the validator applies domain-specific business rules:

### OHLC Consistency Check

For any schema with "ohlc" in the name:

- Validates that `high >= low` for all rows
- Violations are flagged as critical failures

### Future Validations

Additional business rules can be added in `ParquetValidator._validate_business_logic()`.

## Metrics & Observability

### MLflow Metrics

All flows log the following metrics:

- `validation_pass_rate`: 1.0 for success, 0.0 for failure
- `validation_failures`: Count of validation errors
- `rows_validated`: Number of rows validated

### Structured Logging

Validation events are logged with structured context:

```python
logger.info(
    "validation_complete",
    schema_name="normalized_equity_ohlc",
    total_rows=1000,
    valid_rows=998,
    critical_failures=2,
)
```

### Quarantine Directory

Failed records are written to `data/lake/quarantine/` with:

- Original data
- Validation errors
- Schema name used

File format: `{schema_name}_failures.parquet`

## Usage Examples

### Using write_df_safe

```python
from champion.storage.parquet_io import write_df_safe
import polars as pl

df = pl.DataFrame({...})

output_path = write_df_safe(
    df=df,
    dataset="normalized/equity_ohlc",
    base_path="data/lake",
    schema_name="normalized_equity_ohlc",
    schema_dir="schemas/parquet",
    fail_on_validation_errors=True,  # Raise on validation failure
    quarantine_dir="data/lake/quarantine",
)
```

### Using Validator Directly

```python
from champion.validation.validator import ParquetValidator
import polars as pl

validator = ParquetValidator(schema_dir="schemas/parquet")
df = pl.read_parquet("data.parquet")

# Validate with default batch size (10,000 rows)
result = validator.validate_dataframe(df, "normalized_equity_ohlc")

# Or customize batch size for large datasets
result = validator.validate_dataframe(
    df, 
    "normalized_equity_ohlc",
    batch_size=5000  # Smaller batches for constrained memory
)

if result.critical_failures > 0:
    print(f"Found {result.critical_failures} errors")
    for error in result.error_details[:5]:
        print(f"Row {error['row_index']}: {error['message']}")
```

**Performance Characteristics:**

- Throughput: ~54,000 rows/second
- Memory usage: Only one batch in memory at a time
- Tested with datasets up to 10M+ rows

### Using Validation Flow

```python
from champion.validation.flows import validate_parquet_dataset
import asyncio

result = asyncio.run(validate_parquet_dataset(
    file_path="data/lake/normalized/equity_ohlc/data.parquet",
    schema_name="normalized_equity_ohlc",
    schema_dir="schemas/parquet",
    quarantine_dir="data/lake/quarantine",
    fail_on_errors=True,
    max_failure_rate=0.05,
))
```

## Testing

Validation integration tests are in `tests/unit/test_validation_integration.py`.

Run tests:

```bash
PYTHONPATH=src python -m pytest tests/unit/test_validator.py tests/unit/test_validation_integration.py -v
```

## Error Handling

### Validation Failures

When validation fails:

1. Error details are logged with structured logging
2. Failed records are quarantined (if quarantine_dir is set)
3. MLflow metrics are logged (validation_pass_rate=0.0)
4. ValueError is raised with failure summary

### Schema Not Found

If a schema doesn't exist:

- Warning is logged
- Write operation continues (for backward compatibility)
- This should be fixed by adding the appropriate schema

## Migration Notes

### Adding Validation to New Data Sources

1. Create JSON schema in `schemas/parquet/{dataset_name}.json`
2. Update write function to use `write_df_safe()`
3. Add MLflow metrics logging to the flow
4. Add tests for validation failure scenarios

### Converting Avro Schemas

Some schemas are in Avro format. Convert to JSON Schema:

- Required fields → `"required": [...]`
- Field types → `"type": "string|number|integer|boolean"`
- Nullable fields → `"type": ["string", "null"]`

## Best Practices

1. **Always validate**: Use `write_df_safe()` for all new write operations
2. **Use strict mode**: Set `fail_on_validation_errors=True` in production
3. **Monitor quarantine**: Regularly check `data/lake/quarantine/` for failed records
4. **Track metrics**: Review MLflow validation metrics in dashboards
5. **Update schemas**: Keep schemas synchronized with data structure changes
6. **Test failure paths**: Write tests that verify validation catches bad data

## Future Enhancements

- Add schema versioning and evolution
- Implement validation SLAs and alerting thresholds
- Add data profiling and statistics collection
- Create validation dashboard in MLflow
- Add automatic schema generation from DataFrames
- Implement row-level validation results in output
