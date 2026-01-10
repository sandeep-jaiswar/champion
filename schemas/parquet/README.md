# Parquet Schema Contracts

## Overview

This directory contains JSON Schema definitions for Parquet datasets used in the Champion data platform. These schemas serve as formal contracts for data validation, ensuring data quality and consistency across the pipeline.

## Purpose

- **Data Validation**: Enforce type safety, nullability, and range constraints
- **Documentation**: Provide clear, machine-readable schema definitions
- **Quality Gates**: Enable automated validation in data pipelines
- **Evolution**: Track schema changes and maintain backward compatibility

## Avro → Parquet Mapping

The Parquet schemas are derived from Avro schemas but adapted for columnar storage and validation requirements.

### Type Mappings

| Avro Type | Parquet Type | JSON Schema Type | Notes |
|-----------|--------------|------------------|-------|
| `string` | `STRING` / `UTF8` | `"string"` | Text data |
| `int` | `INT32` | `"integer"` | 32-bit signed integer |
| `long` | `INT64` | `"integer"` | 64-bit signed integer |
| `float` | `FLOAT` | `"number"` | Single precision float |
| `double` | `DOUBLE` | `"number"` | Double precision float |
| `boolean` | `BOOLEAN` | `"boolean"` | True/false values |
| `bytes` | `BYTE_ARRAY` | `"string"` (base64) | Binary data |
| `["null", T]` | Nullable column | `[T, "null"]` | Optional fields |

### Logical Type Mappings

| Avro Logical Type | Parquet Logical Type | JSON Schema | Storage | Notes |
|-------------------|---------------------|-------------|---------|-------|
| `timestamp-millis` (long) | `TIMESTAMP(MILLIS)` | `"integer"` | INT64 | Milliseconds since Unix epoch |
| `timestamp-micros` (long) | `TIMESTAMP(MICROS)` | `"integer"` | INT64 | Microseconds since Unix epoch |
| `date` (int) | `DATE` | `"integer"` | INT32 | Days since Unix epoch (1970-01-01) |
| `time-millis` (int) | `TIME(MILLIS)` | `"integer"` | INT32 | Milliseconds since midnight |
| `decimal` | `DECIMAL` | `"number"` | Fixed/Binary | Precision and scale preserved |

### Envelope Structure

Both Avro and Parquet schemas follow the same envelope pattern:

```
Avro (nested payload):
{
  "event_id": "...",
  "event_time": 123456789,
  ...
  "payload": {
    "field1": "...",
    "field2": 123
  }
}

Parquet (flattened):
{
  "event_id": "...",
  "event_time": 123456789,
  ...
  "field1": "...",
  "field2": 123
}
```

**Key Difference**: Parquet schemas flatten the nested `payload` structure for better columnar access patterns.

## Schema Files

### 1. `raw_equity_ohlc.json`

**Source**: `../market-data/raw_equity_ohlc.avsc`  
**Purpose**: Raw equity OHLC data from NSE bhavcopy  
**Storage**: `data/lake/raw/equity_ohlc/`  
**Partitioning**: `date=YYYY-MM-DD/`

#### Key Characteristics

- **Envelope**: `event_id`, `event_time`, `ingest_time`, `source`, `schema_version`, `entity_id`
- **All payload fields nullable**: Gracefully handles malformed data
- **No transformations**: Mirrors NSE format exactly
- **Validation rules**:
  - Required: `event_id`, `event_time`, `ingest_time`, `source`, `schema_version`, `entity_id`
  - Prices: `>= 0` (when present)
  - Volumes: `>= 0` (when present)
  - Dates: ISO 8601 format `YYYY-MM-DD`
  - ISIN: 12-character alphanumeric (when present)

### 2. `normalized_equity_ohlc.json`

**Source**: `../market-data/normalized_equity_ohlc.avsc`  
**Purpose**: Normalized, CA-adjusted equity OHLC  
**Storage**: `data/lake/normalized/equity_ohlc/`  
**Partitioning**: `year=YYYY/month=MM/day=DD/`

#### Key Characteristics

- **Envelope**: Same as raw schema
- **Strongly typed**: Core fields are non-nullable
- **Corporate action adjusted**: All prices adjusted for splits/bonuses/dividends
- **Validation rules**:
  - Required: `event_id`, `event_time`, `ingest_time`, `source`, `schema_version`, `entity_id`, `instrument_id`, `symbol`, `exchange`, `trade_date`, `open`, `high`, `low`, `close`, `volume`, `turnover`, `adjustment_factor`, `is_trading_day`
  - **Critical constraints**:
    - `high >= low` (OHLC consistency)
    - `open`, `high`, `low`, `close` >= 0
    - `volume >= 0`, `turnover >= 0`
    - `adjustment_factor > 0` (must be positive)
    - `exchange` in ["NSE", "BSE"]
    - `instrument_type` in ["STK", "FUT", "OPT", "IDX"] (when present)

## Validation Rules

### Type Validation

- **Integers**: Must be whole numbers (no decimals)
- **Numbers**: Can be integers or floats
- **Strings**: Text data, validated against patterns when specified
- **Booleans**: Only `true` or `false`

### Nullability Validation

- **Required fields**: Must be present and non-null
- **Nullable fields**: Can be `null` or valid value
- **Type enforcement**: Nullable fields specified as `["type", "null"]` in JSON Schema

### Range Validation

- **Prices**: All price fields >= 0 (no negative prices)
- **Volumes**: >= 0 (no negative volume)
- **Trades**: >= 0 (when present)
- **Adjustment Factor**: > 0 (strictly positive, cannot be zero)
- **Timestamps**: >= 0 (valid Unix epoch times)

### Business Logic Validation

#### OHLC Consistency (Normalized schema only)

```
high >= low               (always)
high >= open              (usually, but not required due to gaps)
high >= close             (usually, but not required due to gaps)
low <= open               (usually, but not required due to gaps)
low <= close              (usually, but not required due to gaps)
```

**Note**: We only enforce `high >= low` as a hard constraint. Opening gaps can cause `open > high` or `close > high` in rare cases.

#### Timestamp Validation

```
event_time <= ingest_time + tolerance
tolerance = 5 minutes for EOD files
tolerance = 30 seconds for real-time
```

### Pattern Validation

- **ISIN**: `^[A-Z]{2}[A-Z0-9]{9}[0-9]$` (12 characters)
- **Date**: `^[0-9]{4}-[0-9]{2}-[0-9]{2}$` (YYYY-MM-DD)
- **UUID**: Standard UUID format
- **Schema Version**: `^v[0-9]+$` (e.g., "v1", "v2")

## Data Quality & Quarantine

### Validation Levels

1. **Critical Failures** (quarantine required):
   - Missing required fields
   - Invalid types (cannot parse)
   - Negative prices/volumes
   - `high < low` (OHLC violation)
   - `adjustment_factor <= 0`

2. **Warnings** (log but accept):
   - Missing optional fields
   - Unexpected string patterns (if parseable)
   - Timestamp skew within tolerance

### Quarantine Strategy

Failed records are written to separate quarantine Parquet files:

```
data/lake/quarantine/
├── raw_equity_ohlc/
│   ├── date=2024-01-15/
│   │   ├── validation_errors.parquet
│   │   └── _metadata.json
```

Each quarantined record includes:
- Original record data
- `validation_error`: Error message
- `validation_timestamp`: When validation failed
- `validation_rule`: Which rule failed

## Usage in Prefect Flows

### Validation Task Example

```python
from validation import validate_parquet_schema

@task
def validate_ohlc_data(file_path: Path, schema_name: str):
    """Validate Parquet file against JSON schema."""
    result = validate_parquet_schema(
        file_path=file_path,
        schema_name=schema_name,
        quarantine_dir="data/lake/quarantine"
    )
    
    if result.critical_failures > 0:
        raise ValueError(
            f"Validation failed: {result.critical_failures} critical errors"
        )
    
    if result.warnings > 0:
        logger.warning(
            f"Validation warnings: {result.warnings} records"
        )
    
    return result
```

### Integration in Flow

```python
@flow
def ingest_and_validate_ohlc(date: str):
    # Ingest raw data
    raw_file = ingest_nse_bhavcopy(date)
    
    # Parse to Parquet
    parquet_file = parse_to_parquet(raw_file)
    
    # Validate
    validation_result = validate_ohlc_data(
        parquet_file,
        schema_name="raw_equity_ohlc"
    )
    
    # Alert on failures
    if validation_result.critical_failures > 0:
        send_alert(
            f"Data validation failed for {date}: "
            f"{validation_result.critical_failures} records quarantined"
        )
```

## Schema Evolution

### Backward-Compatible Changes (Allowed)

- Adding optional (nullable) fields
- Relaxing constraints (e.g., removing `minimum`)
- Adding enum values
- Expanding patterns

### Breaking Changes (Require New Version)

- Removing fields
- Making nullable fields required
- Changing field types
- Tightening constraints
- Removing enum values

When making breaking changes:
1. Create new schema version (e.g., `normalized_equity_ohlc_v2.json`)
2. Update schema_version field in data (e.g., `"v2"`)
3. Maintain old version for backward compatibility
4. Document migration path

## Testing

See `validation/tests/` for validation test suite:

```bash
cd validation
poetry install
poetry run pytest tests/
```

## References

- **Avro Schemas**: `../market-data/`
- **Data Platform Docs**: `../../docs/architecture/data-platform.md`
- **Validation Utilities**: `../../validation/`
- **Parquet Spec**: https://parquet.apache.org/docs/
- **JSON Schema Spec**: https://json-schema.org/
