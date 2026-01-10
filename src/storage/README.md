# Storage Utilities

This module provides utilities for managing Parquet datasets in the data lake, including I/O operations, file coalescing, metadata generation, and retention policies.

## Overview

The storage module provides:

- **Parquet I/O**: Write DataFrames with partitioning support
- **File Coalescing**: Merge small files for better query performance
- **Metadata Generation**: Create `_metadata` and `_common_metadata` files
- **Retention Policies**: Clean up old partitions based on age
- **Dataset Statistics**: Get insights into dataset size and structure

## Installation

```bash
cd src/storage
poetry install
```

## Directory Structure

```text
data/lake/
├── raw/              # Raw ingested data
├── normalized/       # Normalized/cleaned data
└── features/         # Feature-engineered data
```

## Usage

### Writing Parquet Datasets

#### Basic Write (No Partitioning)

```python
from storage import write_df
import polars as pl

df = pl.DataFrame({
    'symbol': ['AAPL', 'GOOGL', 'MSFT'],
    'price': [150.0, 2800.0, 350.0],
    'volume': [1000000, 500000, 750000],
    'date': ['2024-01-01', '2024-01-01', '2024-01-01']
})

# Write to raw dataset
dataset_path = write_df(
    df=df,
    dataset='raw',
    base_path='data/lake',
    partitions=None
)
```

#### Write with Date Partitioning

```python
# Partition by date
dataset_path = write_df(
    df=df,
    dataset='raw',
    base_path='data/lake',
    partitions=['date']
)

# Result:
# data/lake/raw/
# ├── date=2024-01-01/
# │   └── part-0.parquet
# └── date=2024-01-02/
#     └── part-0.parquet
```

#### Write with Multi-level Partitioning

```python
# Add year and month columns
df = df.with_columns([
    pl.col('date').str.slice(0, 4).alias('year'),
    pl.col('date').str.slice(5, 2).alias('month')
])

# Partition by year and month
dataset_path = write_df(
    df=df,
    dataset='raw',
    base_path='data/lake',
    partitions=['year', 'month']
)

# Result:
# data/lake/raw/
# ├── year=2024/
# │   ├── month=01/
# │   │   └── part-0.parquet
# │   └── month=02/
# │       └── part-0.parquet
```

#### Compression Options

```python
# Use different compression codecs
write_df(df, 'raw', 'data/lake', compression='snappy')  # Fast (default)
write_df(df, 'raw', 'data/lake', compression='gzip')    # Better compression
write_df(df, 'raw', 'data/lake', compression='zstd')    # Balanced
write_df(df, 'raw', 'data/lake', compression='none')    # No compression
```

### File Coalescing

Small files create metadata overhead and slow down queries. Coalesce them into larger files:

```python
from storage import coalesce_small_files

# Coalesce files smaller than 10MB into 128MB files
files_coalesced = coalesce_small_files(
    dataset_path='data/lake/raw',
    target_file_size_mb=128,
    min_file_size_mb=10,
    dry_run=False
)

print(f"Coalesced {files_coalesced} files")
```

#### Dry Run Mode

Preview what would be coalesced without making changes:

```python
files_to_coalesce = coalesce_small_files(
    dataset_path='data/lake/raw',
    target_file_size_mb=128,
    min_file_size_mb=10,
    dry_run=True  # Only report, don't modify
)

print(f"Would coalesce {files_to_coalesce} files")
```

### Metadata Generation

Generate `_metadata` and `_common_metadata` files for faster query planning:

```python
from storage import generate_dataset_metadata

# Generate metadata files
metadata_file, common_metadata_file = generate_dataset_metadata(
    dataset_path='data/lake/raw'
)

# Force regeneration even if files exist
generate_dataset_metadata(
    dataset_path='data/lake/raw',
    force_regenerate=True
)
```

These metadata files enable query engines (Spark, DuckDB, etc.) to:

- Skip reading individual files for schema information
- Optimize query planning with file-level statistics
- Speed up partition pruning

### Retention Policies

#### Find Old Partitions

```python
from storage import find_old_partitions

# Find partitions older than 90 days
old_partitions = find_old_partitions(
    dataset_path='data/lake/raw',
    retention_days=90
)

print(f"Found {len(old_partitions)} old partitions")
for partition in old_partitions:
    print(f"  - {partition}")
```

#### Clean Up Old Partitions

```python
from storage import cleanup_old_partitions

# Delete partitions older than 90 days
deleted = cleanup_old_partitions(
    dataset_path='data/lake/raw',
    retention_days=90,
    dry_run=False
)

print(f"Deleted {deleted} partitions")
```

#### Calculate Partition Age

```python
from storage import calculate_partition_age
from pathlib import Path

partition_path = Path('data/lake/raw/date=2024-01-01')
age_days = calculate_partition_age(partition_path)
print(f"Partition is {age_days} days old")
```

### Dataset Statistics

```python
from storage import get_dataset_statistics

stats = get_dataset_statistics('data/lake/raw')

print(f"Files: {stats['file_count']}")
print(f"Total size: {stats['total_size_mb']} MB")
print(f"Average file size: {stats['avg_file_size_mb']} MB")
```

## CLI Tools

### Partition Cleanup Script

The `cleanup_partitions.py` script provides a command-line interface for retention policies:

```bash
# Show help
python scripts/cleanup_partitions.py --help

# Dry run (preview what would be deleted)
python scripts/cleanup_partitions.py \
    --dataset data/lake/raw \
    --retention-days 90 \
    --dry-run

# Delete partitions older than 90 days
python scripts/cleanup_partitions.py \
    --dataset data/lake/raw \
    --retention-days 90

# Clean multiple datasets
python scripts/cleanup_partitions.py \
    --dataset data/lake/raw data/lake/normalized \
    --retention-days 90

# Show statistics before and after
python scripts/cleanup_partitions.py \
    --dataset data/lake/raw \
    --retention-days 90 \
    --stats

# Use custom partition pattern (YYYYMMDD instead of YYYY-MM-DD)
python scripts/cleanup_partitions.py \
    --dataset data/lake/raw \
    --retention-days 90 \
    --pattern "%Y%m%d"

# Verbose logging
python scripts/cleanup_partitions.py \
    --dataset data/lake/raw \
    --retention-days 90 \
    --verbose
```

## Testing

```bash
# Run all tests
cd src/storage
poetry run pytest

# Run with coverage
poetry run pytest --cov=storage --cov-report=html

# Run specific test file
poetry run pytest tests/test_parquet_io.py

# Run specific test
poetry run pytest tests/test_parquet_io.py::test_write_df_with_partitions
```

## Code Quality

```bash
# Format code
poetry run black .

# Lint code
poetry run ruff check .

# Type check
poetry run mypy storage/
```

## Integration Example

Complete example integrating all features:

```python
import polars as pl
from storage import (
    write_df,
    coalesce_small_files,
    generate_dataset_metadata,
    cleanup_old_partitions,
    get_dataset_statistics
)

# 1. Write partitioned data
df = pl.read_csv('raw_data.csv')
df = df.with_columns(pl.col('timestamp').cast(pl.Date).alias('date'))

write_df(
    df=df,
    dataset='raw',
    base_path='data/lake',
    partitions=['date'],
    compression='snappy'
)

# 2. Coalesce small files
coalesce_small_files(
    dataset_path='data/lake/raw',
    target_file_size_mb=128,
    min_file_size_mb=10
)

# 3. Generate metadata
generate_dataset_metadata('data/lake/raw')

# 4. Clean up old data
deleted = cleanup_old_partitions(
    dataset_path='data/lake/raw',
    retention_days=90
)

# 5. Get final statistics
stats = get_dataset_statistics('data/lake/raw')
print(f"Dataset has {stats['file_count']} files ({stats['total_size_mb']} MB)")
```

## Best Practices

### Partitioning Strategy

- **Date-based partitioning**: Use for time-series data
  - Daily: `partitions=['date']`
  - Monthly: `partitions=['year', 'month']`
  - Hierarchical: `partitions=['year', 'month', 'day']`

- **Avoid over-partitioning**: Too many partitions create overhead
  - Keep partitions > 100MB when possible
  - Limit partition depth to 2-3 levels

### File Sizes

- **Target file size**: 128-256 MB per file
- **Minimum file size**: 10 MB (coalesce smaller files)
- **Maximum files per partition**: < 1000

### Compression

- **Snappy**: Fast read/write, moderate compression (default)
- **GZIP**: Better compression, slower
- **ZSTD**: Balanced compression and speed
- **None**: Only for already compressed data

### Data Retention Strategy

- **Raw data**: 90-180 days
- **Normalized data**: 365+ days
- **Features**: 30-90 days (recomputable)

### Metadata

- Regenerate `_metadata` after:
  - Writing new partitions
  - Coalescing files
  - Deleting old data

## Architecture

```text
storage/
├── __init__.py
├── parquet_io.py        # I/O and metadata utilities
├── retention.py         # Retention policies
├── pyproject.toml
├── README.md
└── tests/
    ├── __init__.py
    ├── test_parquet_io.py
    └── test_retention.py
```

## API Reference

### write_df()

Write a Polars DataFrame to Parquet with optional partitioning.

**Parameters:**

- `df` (pl.DataFrame): DataFrame to write
- `dataset` (str): Dataset name ('raw', 'normalized', 'features')
- `base_path` (str | Path): Base path for data lake
- `partitions` (Optional[List[str]]): Partition columns
- `max_rows_per_file` (int): Max rows per file (default: 1,000,000)
- `compression` (str): Compression codec (default: 'snappy')

**Returns:** Path to dataset directory

### coalesce_small_files()

Merge small Parquet files into larger ones.

**Parameters:**

- `dataset_path` (str | Path): Path to dataset
- `target_file_size_mb` (int): Target size in MB (default: 128)
- `min_file_size_mb` (int): Min size to coalesce (default: 10)
- `dry_run` (bool): Preview mode (default: False)

**Returns:** Number of files coalesced

### generate_dataset_metadata()

Generate `_metadata` and `_common_metadata` files.

**Parameters:**

- `dataset_path` (str | Path): Path to dataset
- `force_regenerate` (bool): Regenerate even if exists (default: False)

**Returns:** Tuple of (metadata_path, common_metadata_path)

### cleanup_old_partitions()

Remove partitions older than retention period.

**Parameters:**

- `dataset_path` (str | Path): Path to dataset
- `retention_days` (int): Days to retain
- `partition_pattern` (str): Date pattern (default: '%Y-%m-%d')
- `partition_key` (str): Partition key name (default: 'date')
- `dry_run` (bool): Preview mode (default: False)

**Returns:** Number of partitions deleted

### calculate_partition_age()

Calculate partition age in days.

**Parameters:**

- `partition_path` (Path): Path to partition
- `partition_pattern` (str): Date pattern (default: '%Y-%m-%d')

**Returns:** Age in days (int)

### get_dataset_statistics()

Get dataset statistics.

**Parameters:**

- `dataset_path` (str | Path): Path to dataset

**Returns:** Dictionary with statistics

## License

Internal use only - Champion team.
