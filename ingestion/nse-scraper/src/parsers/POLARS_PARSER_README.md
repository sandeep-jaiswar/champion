# Polars Bhavcopy Parser

High-performance bhavcopy parser using Polars and Parquet format.

## Features

- ✅ **Fast**: 2.08x faster than the original CSV parser
- ✅ **Typed**: Explicit schema with robust type casting
- ✅ **Efficient**: Parquet output with Snappy compression
- ✅ **Partitioned**: Hive-style partitioning for efficient queries
- ✅ **ClickHouse-compatible**: Output is readable by ClickHouse

## Performance

Benchmarked on 2,500 rows:

- Parse time: **0.0373s** (✅ < 1s requirement)
- Speedup: **2.08x faster** than old parser
- Output: Parquet with Hive partitioning

## Usage

### Basic Parsing

```python
from datetime import date
from pathlib import Path
from src.parsers.polars_bhavcopy_parser import PolarsBhavcopyParser

parser = PolarsBhavcopyParser()

# Parse to events (for Kafka)
events = parser.parse(
    file_path=Path("data/bhavcopy.csv"),
    trade_date=date(2024, 1, 2),
    output_parquet=False
)

# Parse to DataFrame (for analytics)
df = parser.parse_to_dataframe(
    file_path=Path("data/bhavcopy.csv"),
    trade_date=date(2024, 1, 2)
)

# Write to Parquet
output_file = parser.write_parquet(
    df=df,
    trade_date=date(2024, 1, 2),
    base_path=Path("data/lake")
)
```

### Output Structure

Parquet files are written with Hive-style partitioning:

```text
data/lake/
└── normalized/
    └── ohlc/
        └── year=2024/
            └── month=01/
                └── day=02/
                    └── bhavcopy_20240102.parquet
```

### Schema

The parser enforces explicit types for all columns:

- **Metadata**: event_id, event_time, ingest_time, source, schema_version, entity_id
- **Strings**: TradDt, BizDt, Sgmt, Src, FinInstrmTp, ISIN, TckrSymb, etc.
- **Integers**: FinInstrmId, OpnIntrst, TtlTradgVol, TtlNbOfTxsExctd, etc.
- **Floats**: OpnPric, HghPric, LwPric, ClsPric, LastPric, etc.

### Prefect Integration

Prefect tasks are available in `src/tasks/bhavcopy_tasks.py`:

```python
from src.tasks.bhavcopy_tasks import parse_bhavcopy_to_parquet

# Use in Prefect flow
output_file = parse_bhavcopy_to_parquet(
    csv_file_path="data/bhavcopy.csv",
    trade_date="2024-01-02",
    output_base_path="data/lake"
)
```

**Note**: Prefect must be installed separately to use the tasks.

## Testing

Run unit tests:

```bash
poetry run pytest tests/unit/test_polars_bhavcopy_parser.py -v
```

Run benchmark:

```bash
poetry run python tests/manual/benchmark_parsers.py
```

## Comparison with Old Parser

| Feature | Old Parser | New Parser (Polars) |
|---------|-----------|-------------------|
| Backend | CSV + dict | Polars DataFrame |
| Type Safety | Runtime | Compile-time schema |
| Output Format | list[dict] | Parquet + list[dict] |
| Parse Speed | 0.0776s | 0.0373s (2.08x faster) |
| Partitioning | None | Hive-style |
| ClickHouse Ready | No | Yes |

## Future Enhancements

- [ ] Streaming parser for very large files (>10GB)
- [ ] Incremental updates (append mode)
- [ ] Schema evolution support
- [ ] Compression options (Snappy, Zstd, Gzip)
- [ ] Parallel processing for multiple files
