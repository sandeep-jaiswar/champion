# Idempotency Contract

## Overview

All Prefect tasks in the Champion data platform are designed to be **idempotent**, meaning they can be safely retried multiple times without causing duplicate data or inconsistent state. This document describes the idempotency guarantees and how they are implemented.

## Problem Statement

Before implementing idempotency:
- If a task succeeded but the flow crashed before completion, retrying would cause duplicate records
- No transaction boundaries existed for multi-step operations
- ClickHouse loader had no deduplication mechanism

**Example Scenario:**
1. Task writes 10k rows to Parquet ✅
2. Task completes but flow crashes before finishing
3. Flow retries, writes same 10k rows again ❌ **DUPLICATES**
4. Warehouse has 20k rows for same date

## Solution: Idempotency Markers

We implement idempotency using marker files that track successful task completions.

### Marker File Format

Each successful write operation creates a marker file with the following structure:

```json
{
  "timestamp": "2024-01-15T10:30:00.123456",
  "trade_date": "2024-01-15",
  "rows": 10000,
  "file_hash": "a3f8d9e2...",
  "output_file": "/path/to/data.parquet",
  "metadata": {
    "source": "nse_bhavcopy",
    "table": "normalized_equity_ohlc"
  }
}
```

### Marker File Naming

Marker files are stored alongside the output files with the naming pattern:
```
.idempotent.{date_key}.json
```

Examples:
- `.idempotent.2024-01-15.json` - For daily data
- `.idempotent.20240101-20240131.json` - For date ranges
- `.idempotent.2024-01-15-BULK.json` - For deal types

## Idempotent Tasks

### 1. Write Parquet Tasks

**Location:** `src/champion/orchestration/flows/flows.py`

**Task:** `write_parquet`

**Behavior:**
1. Calculate expected output file path from trade_date
2. Check for idempotency marker
3. If marker exists and is valid, return existing file path (skip write)
4. Otherwise, write data to Parquet
5. Create idempotency marker with file hash

**Usage:**
```python
@task(name="write-parquet", retries=2)
def write_parquet(df: pl.DataFrame, trade_date: date, base_path: str | None = None) -> str:
    # Checks idempotency marker before writing
    # Returns existing file if already completed
    # Creates marker after successful write
```

### 2. Macro Indicators

**Location:** `src/champion/orchestration/tasks/macro_tasks.py`

**Function:** `write_macro_parquet`

**Key:** Combines start_date and end_date (e.g., `20240101-20240131`)

### 3. Bulk/Block Deals

**Location:** `src/champion/orchestration/tasks/bulk_block_deals_tasks.py`

**Function:** `write_bulk_block_deals_parquet`

**Key:** Combines date and deal_type (e.g., `2024-01-15-BULK`)

### 4. Index Constituents

**Location:** `src/champion/orchestration/tasks/index_constituent_tasks.py`

**Function:** `write_index_constituents_parquet`

**Key:** Combines effective_date and index_name (e.g., `2024-01-15-NIFTY50`)

## ClickHouse Deduplication

**Location:** `src/champion/orchestration/flows/flows.py`

**Task:** `load_clickhouse`

**Behavior:**
1. Read Parquet file
2. Extract unique trade dates from data
3. **Validate inputs** to prevent SQL injection
4. **Delete existing data** for those dates: `ALTER TABLE {table} DELETE WHERE TradDt = '{trade_date}'`
5. Insert new data
6. Log metrics

**Security Note:** The DELETE operation validates table names and trade dates before constructing queries to prevent SQL injection attacks.

**Configuration:**
```python
@task(name="load-clickhouse", retries=3)
def load_clickhouse(
    parquet_file: str,
    table: str = "normalized_equity_ohlc",
    deduplicate: bool = True,  # Enable deduplication
) -> dict:
```

## API Reference

### Core Functions

#### `create_idempotency_marker`

```python
def create_idempotency_marker(
    output_file: Path,
    trade_date: str,
    rows: int,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Create an idempotency marker file.
    
    Args:
        output_file: Path to the output data file
        trade_date: Trading date in ISO format (YYYY-MM-DD) or custom key
        rows: Number of rows written
        metadata: Optional additional metadata
        
    Returns:
        Path to the created marker file
    """
```

#### `check_idempotency_marker`

```python
def check_idempotency_marker(
    output_file: Path,
    trade_date: str,
    validate_hash: bool = True,
) -> dict[str, Any] | None:
    """Check if an idempotency marker exists and is valid.
    
    Args:
        output_file: Path to the output data file
        trade_date: Trading date in ISO format (YYYY-MM-DD) or custom key
        validate_hash: Whether to validate file hash (default: True)
        
    Returns:
        Marker data dictionary if valid, None otherwise
    """
```

#### `is_task_completed`

```python
def is_task_completed(output_file: Path, trade_date: str) -> bool:
    """Check if a task has already been completed for the given date.
    
    Args:
        output_file: Path to the output data file
        trade_date: Trading date in ISO format (YYYY-MM-DD) or custom key
        
    Returns:
        True if task is already completed, False otherwise
    """
```

## Testing

Comprehensive tests for idempotency are in `tests/unit/test_idempotency.py`:

- ✅ Creating idempotency markers
- ✅ Checking valid markers
- ✅ Handling missing markers
- ✅ Detecting file changes (hash validation)
- ✅ Working with custom metadata
- ✅ Multiple markers in same directory

**Run tests:**
```bash
poetry run pytest tests/unit/test_idempotency.py -v
```

## Best Practices

### 1. Always Use Idempotency Markers for Write Operations

```python
# ✅ GOOD: Check marker before writing
if is_task_completed(output_file, trade_date):
    logger.info("Task already completed, skipping")
    return str(output_file)

# Write data
output_file = write_data(df, trade_date)

# Create marker after successful write
create_idempotency_marker(
    output_file=output_file,
    trade_date=trade_date,
    rows=len(df),
    metadata={"source": "my_source"}
)
```

### 2. Use Descriptive Date Keys

For complex scenarios, create descriptive keys:

```python
# Date range
date_key = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"

# Date + type
date_key = f"{date.isoformat()}-{deal_type.upper()}"

# Date + index
date_key = f"{date.isoformat()}-{index_name}"
```

### 3. Include Relevant Metadata

```python
create_idempotency_marker(
    output_file=output_file,
    trade_date=trade_date,
    rows=len(df),
    metadata={
        "source": "nse_bhavcopy",
        "table": "normalized_equity_ohlc",
        "validation_passed": True,
        "scraper_version": "v1.0",
    }
)
```

### 4. Log Idempotent Skips

```python
if is_task_completed(output_file, trade_date):
    marker_data = check_idempotency_marker(output_file, trade_date)
    logger.info(
        "task_already_completed_skipping",
        output_file=str(output_file),
        trade_date=trade_date,
        rows=marker_data.get("rows", 0) if marker_data else 0,
    )
    return str(output_file)
```

### 5. MLflow Integration

Track idempotent behavior in MLflow:

```python
mlflow.log_param("idempotent_skip", is_skipped)
```

## Acceptance Criteria ✅

- ✅ **Tasks are idempotent** - All write tasks check markers before writing
- ✅ **No duplicate records on flow restart** - Markers prevent re-execution
- ✅ **Idempotency markers stored with Parquet** - Marker files in same directory
- ✅ **ClickHouse loader uses deduplication** - DELETE before INSERT pattern
- ✅ **Comprehensive tests** - All idempotency scenarios covered
- ✅ **Documentation** - This document describes the contract

## Monitoring

Idempotency events are logged with structured logging:

```python
# Marker created
logger.info("idempotency_marker_created", marker_path=str(marker_path), rows=rows)

# Marker valid
logger.info("idempotency_marker_valid", marker_path=str(marker_path), rows=rows)

# Task skipped
logger.info("task_already_completed_skipping", output_file=str(output_file))

# Hash mismatch
logger.warning("idempotency_marker_hash_mismatch", expected_hash=..., current_hash=...)
```

## Troubleshooting

### Marker File Missing But Data Exists

If a marker file is deleted but data exists, the task will overwrite the data. This is safe because:
1. For Parquet writes, the file is overwritten atomically
2. For ClickHouse, the DELETE ensures old data is removed

### Hash Validation Failure

If hash validation fails (file was modified), the marker is considered invalid:
- Task will re-execute
- New marker will be created with updated hash
- Check logs for `idempotency_marker_hash_mismatch` warnings

### Disable Hash Validation

For scenarios where file modification is expected:

```python
marker_data = check_idempotency_marker(
    output_file=output_file,
    trade_date=trade_date,
    validate_hash=False,  # Skip hash validation
)
```

## Future Enhancements

Potential improvements to the idempotency system:

1. **Distributed Locking** - Use Redis/ZooKeeper for distributed task coordination
2. **Marker Cleanup** - Automated cleanup of old markers after retention period
3. **Marker Versioning** - Track schema versions in markers
4. **S3 Marker Storage** - Store markers in S3 for distributed environments
5. **Audit Trail** - Track all retry attempts and their outcomes

## References

- Implementation: `src/champion/utils/idempotency.py`
- Tests: `tests/unit/test_idempotency.py`
- Main flow: `src/champion/orchestration/flows/flows.py`
- Issue: MEDIUM: Add idempotency guarantees to Prefect tasks
