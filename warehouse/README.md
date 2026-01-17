# ClickHouse Data Warehouse

## Overview

This directory contains the ClickHouse data warehouse infrastructure for the Champion market data platform, including:

- **Docker Compose setup** for local ClickHouse deployment
- **DDL schemas** for raw, normalized, and features tables
- **Batch loader** for loading Parquet data from the data lake
- **Sample data generator** for testing

## Quick Start

### 1. Start ClickHouse

```bash
# From repository root
docker-compose up -d clickhouse

# Check logs
docker-compose logs -f clickhouse

# Wait for healthy status
docker-compose ps
```

ClickHouse will be available at:

- HTTP interface: `http://localhost:8123`
- Native client: `localhost:9000`

### 2. Verify Installation

```bash
# Test connection
docker-compose exec clickhouse clickhouse-client

# Inside ClickHouse client:
SHOW DATABASES;
USE champion_market;
SHOW TABLES;
```

### 3. Generate Sample Data

```bash
# Install dependencies
pip install polars clickhouse-connect

# Generate sample Parquet files
python -m champion.warehouse.clickhouse.generate_sample_data
```

This creates sample data in:

- `data/lake/raw/equity_ohlc/date=2024-01-*/`
- `data/lake/normalized/equity_ohlc/year=2024/month=01/day=*/`
- `data/lake/features/equity_indicators/year=2024/month=01/day=*/`

### 4. Load Data into ClickHouse

```bash
# Load raw equity OHLC data
python -m champion.warehouse.clickhouse.batch_loader \
    --table raw_equity_ohlc \
    --source data/lake/raw/equity_ohlc/ \
    --verify

# Load normalized equity OHLC data
python -m champion.warehouse.clickhouse.batch_loader \
    --table normalized_equity_ohlc \
    --source data/lake/normalized/equity_ohlc/ \
    --verify

# Load features equity indicators data
python -m champion.warehouse.clickhouse.batch_loader \
    --table features_equity_indicators \
    --source data/lake/features/equity_indicators/ \
    --verify
```

### 5. Query Data

```bash
# Connect to ClickHouse
docker-compose exec clickhouse clickhouse-client

# Query raw data
SELECT TckrSymb, TradDt, ClsPric, TtlTradgVol 
FROM champion_market.raw_equity_ohlc 
LIMIT 10;

# Query normalized data
SELECT TckrSymb, TradDt, ClsPric, TtlTradgVol 
FROM champion_market.normalized_equity_ohlc 
WHERE TckrSymb = 'SYMBOL001'
ORDER BY TradDt DESC 
LIMIT 10;

# Query features
SELECT symbol, trade_date, sma_20, rsi_14, macd 
FROM champion_market.features_equity_indicators 
WHERE symbol = 'SYMBOL001'
ORDER BY trade_date DESC 
LIMIT 10;

# Aggregation query
SELECT 
    TradDt as trade_date,
    count() as total_symbols,
    sum(TtlTradgVol) as total_volume,
    avg(ClsPric) as avg_close,
    max(HghPric) as max_high,
    min(LwPric) as min_low
FROM champion_market.normalized_equity_ohlc
GROUP BY TradDt
ORDER BY TradDt DESC;
```

## Architecture

### Database Schema

```text
champion_market/
├── raw_equity_ohlc              # Raw NSE bhavcopy data
│   ├── Partitioned by: YYYYMM
│   ├── Sort key: (TckrSymb, FinInstrmId, TradDt, event_time)
│   └── TTL: 5 years
│
├── normalized_equity_ohlc       # Normalized, CA-adjusted data
│   ├── Partitioned by: YYYYMM
│   ├── Sort key: (TckrSymb, FinInstrmId, TradDt, event_time)
│   ├── Engine: ReplacingMergeTree (supports upserts)
│   └── TTL: 3 years
│
├── features_equity_indicators   # Technical indicators
│   ├── Partitioned by: YYYYMM
│   ├── Sort key: (symbol, trade_date, feature_timestamp)
│   └── TTL: 1 year
│
└── equity_ohlc_daily_summary   # Materialized view (aggregates)
    └── Auto-updated on inserts
```

### Schema Naming Conventions

The Champion platform uses **NSE column names** for raw and normalized equity OHLC data to maintain consistency with source data and reduce transformation complexity. This design decision ensures:

1. **Direct mapping** from NSE bhavcopy CSV files to ClickHouse without column renaming
2. **Audit trail consistency** - raw and normalized layers use same field names
3. **Reduced transformation overhead** in the ETL pipeline
4. **Clear data lineage** - column names match NSE documentation

#### NSE Column Naming (Raw & Normalized Layers)

- `TradDt` - Trade date
- `TckrSymb` - Ticker symbol
- `FinInstrmId` - Financial instrument ID (unique identifier for securities)
- `OpnPric`, `HghPric`, `LwPric`, `ClsPric` - OHLC prices
- `TtlTradgVol` - Total traded volume
- `ISIN` - International Securities Identification Number

#### Normalized Column Naming (Features Layer)

The features layer uses more developer-friendly names:

- `symbol`, `trade_date`, `open`, `high`, `low`, `close`, `volume`

#### Column Name Mapping

The batch loader (`champion.warehouse.clickhouse.batch_loader`) includes a mapping layer that can translate between naming conventions. See `COLUMN_MAPPINGS` constant for supported mappings. This allows loading Parquet files that use either NSE names or normalized names.

### Deduplication Strategy and Sort Keys

#### Why FinInstrmId in ORDER BY?

The `ORDER BY (TckrSymb, FinInstrmId, TradDt, event_time)` clause is **critical** for handling cases where:

1. **Multiple securities share the same ticker symbol** (e.g., NCDs, bonds, different series)
   - Example: `IBULHSGFIN` has 19 different NCD tranches, each with a unique `FinInstrmId`
   - Without `FinInstrmId` in the sort key, these would be incorrectly merged/deduplicated

2. **ReplacingMergeTree deduplication semantics**
   - ClickHouse uses the `ORDER BY` columns to identify duplicate rows
   - Only rows with identical values in ALL `ORDER BY` columns are considered duplicates
   - This ensures each security (identified by `TckrSymb` + `FinInstrmId` combination) maintains separate records

3. **Query performance optimization**
   - Queries filtering by symbol can efficiently use the primary index
   - Adding `FinInstrmId` allows precise security-level queries without full scans
   - Example: `WHERE TckrSymb = 'IBULHSGFIN' AND FinInstrmId = 123456`

#### Example: Multiple Securities Under Same Ticker

```sql
-- Query to see distinct securities under a ticker
SELECT count(), TckrSymb, FinInstrmId, ISIN, FinInstrmNm
FROM champion_market.normalized_equity_ohlc
WHERE TradDt = '2024-01-02' AND TckrSymb = 'IBULHSGFIN'
GROUP BY TckrSymb, FinInstrmId, ISIN, FinInstrmNm
ORDER BY FinInstrmId;

-- Expected result: 19 rows (one for each NCD tranche)
-- Without FinInstrmId in ORDER BY, these would collapse to 1 row (DATA LOSS!)
```

#### Data Integrity Guarantees

With the current schema design:

1. **No unintended deduplication** - Each unique security maintains its own records
2. **Idempotent loads** - Re-running the loader with same data won't create duplicates
3. **Late-arriving data handling** - ReplacingMergeTree(ingest_time) keeps the latest version
4. **Audit trail preservation** - event_time in ORDER BY maintains temporal ordering

### Users and Permissions

| User             | Password      | Permissions           | Purpose                    |
|------------------|---------------|-----------------------|----------------------------|
| `default`        | (empty)       | Full admin            | Administration             |
| `champion_user`  | champion_pass | SELECT, INSERT        | Data loading, queries      |
| `champion_reader`| reader_pass   | SELECT (read-only)    | Analytics, dashboards      |

## Batch Loader

### Usage

```bash
python -m champion.warehouse.clickhouse.batch_loader [OPTIONS]

Required:
  --table TABLE          Target table (raw_equity_ohlc, normalized_equity_ohlc, features_equity_indicators)
  --source SOURCE        Path to Parquet file(s) or directory

Optional:
  --host HOST           ClickHouse host (default: localhost)
  --port PORT           ClickHouse HTTP port (default: 8123)
  --user USER           Database user (default: champion_user)
  --password PASSWORD   Database password (default: champion_pass)
  --database DATABASE   Database name (default: champion_market)
  --batch-size SIZE     Rows per batch (default: 100000)
  --dry-run             Validate without loading
  --verify              Verify after loading
  --verbose             Enable debug logging
```

### Examples

#### Load single date partition

```bash
python -m champion.warehouse.clickhouse.batch_loader \
    --table raw_equity_ohlc \
    --source data/lake/raw/equity_ohlc/date=2024-01-15/
```

#### Load entire dataset

```bash
python -m champion.warehouse.clickhouse.batch_loader \
    --table normalized_equity_ohlc \
    --source data/lake/normalized/equity_ohlc/ \
    --batch-size 50000 \
    --verify
```

#### Dry run (validate only)

```bash
python -m champion.warehouse.clickhouse.batch_loader \
    --table features_equity_indicators \
    --source data/lake/features/equity_indicators/ \
    --dry-run \
    --verbose
```

#### Using environment variables

```bash
export CLICKHOUSE_HOST=clickhouse.example.com
export CLICKHOUSE_USER=champion_user
export CLICKHOUSE_PASSWORD=secure_password

python -m champion.warehouse.clickhouse.batch_loader \
    --table normalized_equity_ohlc \
    --source data/lake/normalized/equity_ohlc/
```

## Sample Queries

### Raw Data Analysis

```sql
-- Top 10 symbols by volume
SELECT 
    TckrSymb,
    sum(TtlTradgVol) as total_volume,
    avg(ClsPric) as avg_price
FROM champion_market.raw_equity_ohlc
WHERE TradDt >= '2024-01-15'
GROUP BY TckrSymb
ORDER BY total_volume DESC
LIMIT 10;

-- Daily market statistics
SELECT 
    TradDt,
    count(DISTINCT TckrSymb) as symbols_traded,
    sum(TtlTradgVol) as total_volume,
    sum(TtlTrfVal) as total_turnover,
    avg(ClsPric) as avg_price
FROM champion_market.raw_equity_ohlc
GROUP BY TradDt
ORDER BY TradDt DESC;
```

### Normalized Data Analysis

```sql
-- Price performance (daily returns)
SELECT 
    TckrSymb,
    TradDt,
    ClsPric,
    PrvsClsgPric,
    ((ClsPric - PrvsClsgPric) / PrvsClsgPric * 100) as daily_return_pct
FROM champion_market.normalized_equity_ohlc
WHERE TckrSymb IN ('SYMBOL001', 'SYMBOL002', 'SYMBOL003')
ORDER BY TckrSymb, TradDt DESC
LIMIT 30;

-- Volume leaders
SELECT 
    TradDt,
    TckrSymb,
    TtlTradgVol as volume,
    TtlTrfVal as turnover,
    ClsPric,
    (ClsPric - PrvsClsgPric) as price_change
FROM champion_market.normalized_equity_ohlc
WHERE TradDt = '2024-01-15'
ORDER BY TtlTradgVol DESC
LIMIT 20;

-- OHLC validation (high >= low check)
SELECT 
    count() as total_rows,
    countIf(HghPric >= LwPric) as valid_rows,
    countIf(HghPric < LwPric) as invalid_rows
FROM champion_market.normalized_equity_ohlc;
```

### Features Analysis

```sql
-- RSI analysis
SELECT 
    symbol,
    trade_date,
    rsi_14,
    CASE
        WHEN rsi_14 > 70 THEN 'Overbought'
        WHEN rsi_14 < 30 THEN 'Oversold'
        ELSE 'Neutral'
    END as rsi_signal
FROM champion_market.features_equity_indicators
WHERE trade_date >= '2024-01-15'
ORDER BY rsi_14 DESC
LIMIT 20;

-- Moving average crossovers
SELECT 
    symbol,
    trade_date,
    sma_20,
    sma_50,
    sma_200,
    CASE
        WHEN sma_20 > sma_50 AND sma_50 > sma_200 THEN 'Bullish'
        WHEN sma_20 < sma_50 AND sma_50 < sma_200 THEN 'Bearish'
        ELSE 'Neutral'
    END as trend
FROM champion_market.features_equity_indicators
WHERE trade_date = (SELECT max(trade_date) FROM champion_market.features_equity_indicators)
ORDER BY symbol;

-- Bollinger Bands analysis
SELECT 
    symbol,
    trade_date,
    bb_lower,
    bb_middle,
    bb_upper,
    bb_width,
    CASE
        WHEN bb_width < 20 THEN 'Consolidating'
        WHEN bb_width > 40 THEN 'Volatile'
        ELSE 'Normal'
    END as volatility_regime
FROM champion_market.features_equity_indicators
WHERE trade_date >= '2024-01-15'
ORDER BY bb_width DESC
LIMIT 20;
```

### Materialized View Queries

```sql
-- Daily market summary
SELECT 
    trade_date,
    exchange,
    total_symbols,
    formatReadableQuantity(total_volume) as volume_formatted,
    round(total_turnover / 1000000, 2) as turnover_millions,
    round(avg_close_price, 2) as avg_price
FROM champion_market.equity_ohlc_daily_summary
ORDER BY trade_date DESC
LIMIT 10;
```

## Performance Tuning

### Query Optimization

```sql
-- Use PREWHERE for early filtering (faster than WHERE)
SELECT * FROM normalized_equity_ohlc
PREWHERE trade_date = '2024-01-15'
WHERE volume > 1000000;

-- Enable parallel processing
SET max_threads = 8;

-- Use sampling for exploratory queries
SELECT * FROM normalized_equity_ohlc
SAMPLE 0.1  -- 10% sample
WHERE trade_date >= '2024-01-01';
```

### Index Usage

```sql
-- Check if bloom filter index is used
EXPLAIN indexes = 1
SELECT * FROM raw_equity_ohlc
WHERE ISIN = 'INE123456789';

-- Check partition pruning
EXPLAIN
SELECT * FROM normalized_equity_ohlc
WHERE trade_date BETWEEN '2024-01-15' AND '2024-01-20';
```

### Table Statistics

```sql
-- Table sizes
SELECT 
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as total_rows,
    count() as partition_count
FROM system.parts
WHERE database = 'champion_market' AND active
GROUP BY table
ORDER BY sum(bytes) DESC;

-- Partition information
SELECT 
    partition,
    sum(rows) as rows,
    formatReadableSize(sum(bytes)) as size
FROM system.parts
WHERE database = 'champion_market' 
    AND table = 'normalized_equity_ohlc' 
    AND active
GROUP BY partition
ORDER BY partition DESC;
```

## Maintenance

### Optimize Tables

```sql
-- Optimize table (merge small parts)
OPTIMIZE TABLE champion_market.normalized_equity_ohlc;

-- Optimize specific partition
OPTIMIZE TABLE champion_market.normalized_equity_ohlc 
PARTITION '202401';
```

### Backup and Restore

```sql
-- Backup partition
ALTER TABLE champion_market.normalized_equity_ohlc 
FREEZE PARTITION '202401' WITH NAME 'backup_202401';

-- Restore from backup
ALTER TABLE champion_market.normalized_equity_ohlc 
ATTACH PARTITION '202401' FROM '/var/lib/clickhouse/shadow/backup_202401/';
```

### Delete Old Data

```sql
-- Delete specific partition
ALTER TABLE champion_market.raw_equity_ohlc 
DROP PARTITION '201901';

-- Check TTL status
SELECT 
    partition,
    min_time,
    max_time
FROM system.parts
WHERE database = 'champion_market' 
    AND table = 'raw_equity_ohlc' 
    AND active
ORDER BY partition DESC;
```

## Troubleshooting

### Connection Issues

```bash
# Check if ClickHouse is running
docker-compose ps clickhouse

# Check logs
docker-compose logs clickhouse

# Restart service
docker-compose restart clickhouse
```

### Query Performance

```sql
-- Check query execution plan
EXPLAIN
SELECT * FROM normalized_equity_ohlc
WHERE TckrSymb = 'SYMBOL001' AND TradDt >= '2024-01-15';

-- Check slow queries
SELECT 
    query,
    event_time,
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) as data_read
FROM system.query_log
WHERE type = 'QueryFinish'
    AND query_duration_ms > 1000
ORDER BY query_duration_ms DESC
LIMIT 10;
```

### Data Validation

```sql
-- Check for duplicate records (considering FinInstrmId for uniqueness)
SELECT 
    TckrSymb,
    FinInstrmId,
    TradDt,
    count(*) as duplicate_count
FROM champion_market.normalized_equity_ohlc
GROUP BY TckrSymb, FinInstrmId, TradDt
HAVING count(*) > 1;

-- Check for null values in required columns
SELECT 
    countIf(event_id IS NULL) as null_event_ids,
    countIf(TckrSymb IS NULL) as null_symbols,
    countIf(TradDt IS NULL) as null_dates,
    countIf(FinInstrmId IS NULL) as null_instrument_ids
FROM champion_market.normalized_equity_ohlc;
```

## Development

### Modify Schema

To modify table schemas:

1. Update `warehouse/clickhouse/init/01_create_tables.sql`
2. Drop and recreate tables (WARNING: deletes data):

```bash
docker-compose exec clickhouse clickhouse-client <<EOF
DROP TABLE IF EXISTS champion_market.raw_equity_ohlc;
DROP TABLE IF EXISTS champion_market.normalized_equity_ohlc;
DROP TABLE IF EXISTS champion_market.features_equity_indicators;
EOF

# Restart to recreate tables
docker-compose restart clickhouse
```

### Add New Tables

1. Add DDL to `warehouse/clickhouse/init/01_create_tables.sql` or create new file
2. Restart ClickHouse to apply changes

### Custom Configuration

To customize ClickHouse settings:

1. Create `warehouse/clickhouse/config.xml`
2. Mount in `docker-compose.yml`:

   ```yaml
   volumes:
     - ./warehouse/clickhouse/config.xml:/etc/clickhouse-server/config.d/config.xml:ro
   ```

## References

- [ClickHouse Documentation](https://clickhouse.com/docs)
- [ClickHouse SQL Reference](https://clickhouse.com/docs/en/sql-reference/)
- [MergeTree Engine](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [ReplacingMergeTree Engine](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
- Champion Data Platform Architecture: `docs/architecture/data-platform.md`
