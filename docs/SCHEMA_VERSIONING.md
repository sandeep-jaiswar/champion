# Schema Versioning Implementation Guide

## Overview

This document describes the schema versioning implementation added to all parsers in the champion platform. Schema versioning helps detect when upstream data sources (like NSE) change their data format, preventing silent data loss.

## Problem Statement

When NSE or other data sources change their CSV column names or structure:

- Parsers using old column names silently fail
- No warnings appear in logs
- Warehouse receives incomplete data
- Schema drift goes undetected

## Solution

All parsers now implement schema versioning with three key components:

1. **SCHEMA_VERSION constant** - Version identifier for tracking
2. **Schema validation** - Detects column mismatches  
3. **Version metadata** - Adds version to output data

## Implementation Details

### 1. SCHEMA_VERSION Constant

Every parser now includes a `SCHEMA_VERSION` class attribute:

```python
class PolarsBhavcopyParser(Parser):
    """High-performance parser for NSE CM Bhavcopy CSV files.
    
    Attributes:
        SCHEMA_VERSION: Parser schema version for tracking compatibility.
    """
    
    SCHEMA_VERSION = "v1.0"
```

**All Parsers with SCHEMA_VERSION:**

- `base_parser.py` (base class)
- `polars_bhavcopy_parser.py`
- `bhavcopy_parser.py`
- `polars_bse_parser.py`
- `symbol_master_parser.py`
- `index_constituent_parser.py`
- `macro_indicator_parser.py`
- `bulk_block_deals_parser.py`
- `ca_parser.py`
- `trading_calendar_parser.py`
- `quarterly_financials_parser.py`
- `shareholding_parser.py`
- `symbol_enrichment.py`

### 2. Schema Validation

Parsers with explicit schema definitions (Polars-based parsers) include a `_validate_schema()` method that runs after reading CSV data:

```python
def _validate_schema(
    self, df: pl.DataFrame, expected_schema: dict[str, Any]
) -> None:
    """Validate that DataFrame columns match expected schema.
    
    Args:
        df: DataFrame to validate
        expected_schema: Expected schema dictionary (column_name -> polars type)
    
    Raises:
        ValueError: If schema mismatch is detected
    """
    actual_cols = set(df.columns)
    expected_cols = set(expected_schema.keys())
    
    if actual_cols != expected_cols:
        missing = expected_cols - actual_cols
        extra = actual_cols - expected_cols
        
        error_msg = f"Schema mismatch (version {self.SCHEMA_VERSION}): "
        if missing:
            error_msg += f"missing columns={sorted(missing)}"
        if extra:
            if missing:
                error_msg += ", "
            error_msg += f"extra columns={sorted(extra)}"
        
        logger.error(
            "Schema validation failed",
            schema_version=self.SCHEMA_VERSION,
            missing=sorted(missing) if missing else [],
            extra=sorted(extra) if extra else [],
        )
        raise ValueError(error_msg)
```

**Parsers with Schema Validation:**

- `PolarsBhavcopyParser` - validates against `BHAVCOPY_SCHEMA`
- `PolarsBseParser` - validates against `BSE_BHAVCOPY_SCHEMA`
- `SymbolMasterParser` - validates against `SYMBOL_MASTER_SCHEMA`

**Usage in parse() method:**

```python
def parse(self, file_path: Path, trade_date: date) -> list[dict[str, Any]]:
    try:
        # Read CSV with explicit schema
        df = pl.read_csv(
            file_path,
            schema_overrides=BHAVCOPY_SCHEMA,
            null_values=["-", "", "null", "NULL", "N/A"],
            ignore_errors=False,
        )
        
        # Validate schema - will raise ValueError on mismatch
        self._validate_schema(df, BHAVCOPY_SCHEMA)
        
        # Continue with parsing...
```

### 3. Version Metadata

#### For DataFrame-based Parsers

The base `Parser` class provides an `add_metadata()` method that adds `_schema_version` to DataFrames:

```python
def add_metadata(self, df: pl.DataFrame, parsed_at: datetime | None = None) -> pl.DataFrame:
    """Add standard metadata columns to DataFrame.
    
    Returns DataFrame with added columns:
    - _schema_version: Version of the parser schema
    - _parsed_at: Timestamp when the data was parsed
    """
    if parsed_at is None:
        parsed_at = datetime.now()
    
    return df.with_columns(
        [
            pl.lit(self.SCHEMA_VERSION).alias("_schema_version"),
            pl.lit(parsed_at).alias("_parsed_at"),
        ]
    )
```

#### For Event-based Parsers

Event-based parsers already include `schema_version` in the event envelope:

```python
event = {
    "event_id": event_id,
    "event_time": event_time,
    "ingest_time": ingest_time,
    "source": "nse_cm_bhavcopy",
    "schema_version": "v1",  # Already present
    "entity_id": entity_id,
    "payload": {...}
}
```

## Error Handling

When a schema mismatch is detected:

1. **ValueError is raised** with descriptive message:

```
Schema mismatch (version v1.0): missing columns=['ClsPric', 'OpnPric'], extra columns=['CLOSE_PRICE', 'OPEN_PRICE']
```

1. **Structured log entry** is created:

```json
{
  "level": "error",
  "event": "Schema validation failed",
  "schema_version": "v1.0",
     "missing": ["ClsPric", "OpnPric"],
     "extra": ["CLOSE_PRICE", "OPEN_PRICE"]
   }
   ```

1. **Parsing fails fast** - no incomplete data reaches the warehouse

## Testing

Comprehensive test suite in `tests/unit/test_schema_validation.py`:

- **Schema version presence** - Verifies all parsers have SCHEMA_VERSION
- **Valid schema** - Confirms validation passes with correct columns
- **Missing columns** - Tests error handling for missing columns
- **Extra columns** - Tests error handling for extra columns
- **Combined mismatches** - Tests both missing and extra columns
- **Parse integration** - Tests schema validation during actual parsing

**Test Coverage:** 21 tests, all passing

## Migration Guide

### When NSE Changes Schema

If NSE changes their data format (e.g., renames columns):

1. **Update schema definition**:

```python
BHAVCOPY_SCHEMA = {
    "TICKER_SYMBOL": pl.Utf8,  # Was "TckrSymb"
    "CLOSE_PRICE": pl.Float64,  # Was "ClsPric"
    # ... rest of schema
}
```

1. **Increment SCHEMA_VERSION**:

```python
SCHEMA_VERSION = "v2.0"  # Was "v1.0"
```

1. **Update column references** in parsing logic:

```python
df = df.filter(pl.col("TICKER_SYMBOL") != "")  # Was "TckrSymb"
```

1. **Test with new data format**

1. **Deploy updated parser**

### For Backward Compatibility

If you need to support both old and new formats temporarily:

```python
def parse(self, file_path: Path, trade_date: date) -> list[dict[str, Any]]:
    try:
        # Try new schema first
        df = pl.read_csv(file_path, schema_overrides=BHAVCOPY_SCHEMA_V2)
        self._validate_schema(df, BHAVCOPY_SCHEMA_V2)
        schema_version = "v2.0"
    except ValueError as e:
        # Fall back to old schema
        logger.warning("New schema failed, trying old schema", error=str(e))
        df = pl.read_csv(file_path, schema_overrides=BHAVCOPY_SCHEMA_V1)
        self._validate_schema(df, BHAVCOPY_SCHEMA_V1)
        schema_version = "v1.0"
    
    # Continue with parsing...
```

## Best Practices

1. **Always validate schema** after reading CSV data
2. **Include version in error messages** for debugging
3. **Log schema mismatches** with structured logging
4. **Increment version** whenever schema changes
5. **Document schema changes** in version history
6. **Test schema validation** with intentionally wrong data

## Benefits

✅ **Early detection** - Schema drift caught immediately  
✅ **Descriptive errors** - Clear messages about what's wrong  
✅ **Version tracking** - Know which schema version processed data  
✅ **Data quality** - Prevents incomplete data in warehouse  
✅ **Debugging aid** - Version info helps troubleshoot issues  
✅ **Migration support** - Smooth transitions between schema versions  

## References

- Base parser implementation: `src/champion/parsers/base_parser.py`
- Example validation: `src/champion/parsers/polars_bhavcopy_parser.py`
- Test suite: `tests/unit/test_schema_validation.py`
