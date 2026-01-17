# ClickHouse Warehouse Testing Guide

## Overview

This guide provides step-by-step instructions for testing the ClickHouse warehouse implementation.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.9+ with `polars` and `clickhouse-connect` libraries
- At least 2GB of free disk space

## Testing Steps

### Step 1: Start ClickHouse

```bash
# From repository root
cd /home/runner/work/champion/champion

# Start ClickHouse service
docker compose up -d clickhouse

# Wait for initialization (30 seconds)
sleep 30

# Check container status
docker compose ps

# Check logs
docker compose logs clickhouse | tail -20
```

Expected output: Container should be in "healthy" status.

### Step 2: Verify ClickHouse Connection

```bash
# Test connection using clickhouse-client
docker compose exec clickhouse clickhouse-client --query "SELECT 1"

# Expected output: 1
```

### Step 3: Verify Database and Tables

```bash
# Connect to ClickHouse client
docker compose exec clickhouse clickhouse-client

# Inside ClickHouse client, run:
SHOW DATABASES;
USE champion_market;
SHOW TABLES;
DESC raw_equity_ohlc;
DESC normalized_equity_ohlc;
DESC features_equity_indicators;

# Exit client
exit
```

Expected output:

- Database `champion_market` exists
- Three tables: `raw_equity_ohlc`, `normalized_equity_ohlc`, `features_equity_indicators`
- One materialized view: `equity_ohlc_daily_summary`

### Step 4: Generate Sample Data

```bash
# Install dependencies
pip install polars clickhouse-connect

# Generate sample Parquet files
python warehouse/loader/generate_sample_data.py
```

Expected output:

- Files created in `data/lake/raw/equity_ohlc/date=2024-01-*/`
- Files created in `data/lake/normalized/equity_ohlc/year=2024/month=01/day=*/`
- Files created in `data/lake/features/equity_indicators/year=2024/month=01/day=*/`

Verify files were created:

```bash
ls -lh data/lake/raw/equity_ohlc/*/
ls -lh data/lake/normalized/equity_ohlc/*/*/*/*/
ls -lh data/lake/features/equity_indicators/*/*/*/*/
```

### Step 5: Load Data into ClickHouse

#### 5a. Load Raw Data

```bash
python -m champion.warehouse.clickhouse.batch_loader \
    --table raw_equity_ohlc \
    --source data/lake/raw/equity_ohlc/ \
    --verify \
    --verbose
```

Expected output:

- Successfully loads 50 rows (10 symbols × 5 days)
- Verification shows row count matches

#### 5b. Load Normalized Data

```bash
python -m champion.warehouse.clickhouse.batch_loader \
    --table normalized_equity_ohlc \
    --source data/lake/normalized/equity_ohlc/ \
    --verify \
    --verbose
```

Expected output:

- Successfully loads 50 rows (10 symbols × 5 days)
- Verification shows row count matches

#### 5c. Load Features Data

```bash
python -m champion.warehouse.clickhouse.batch_loader \
    --table features_equity_indicators \
    --source data/lake/features/equity_indicators/ \
    --verify \
    --verbose
```

Expected output:

- Successfully loads 50 rows (10 symbols × 5 days)
- Verification shows row count matches

### Step 6: Verify Data with Queries

#### Test Query 1: Count Records

```sql
docker compose exec clickhouse clickhouse-client --query "
SELECT 
    'raw_equity_ohlc' as table_name,
    count() as row_count
FROM champion_market.raw_equity_ohlc

UNION ALL

SELECT 
    'normalized_equity_ohlc' as table_name,
    count() as row_count
FROM champion_market.normalized_equity_ohlc

UNION ALL

SELECT 
    'features_equity_indicators' as table_name,
    count() as row_count
FROM champion_market.features_equity_indicators;
"
```

Expected output:

```text
raw_equity_ohlc              50
normalized_equity_ohlc       50
features_equity_indicators   50
```

#### Test Query 2: Sample Data from Raw Table

```sql
docker compose exec clickhouse clickhouse-client --query "
SELECT 
    TckrSymb as symbol,
    TradDt as trade_date,
    OpnPric as open,
    HghPric as high,
    LwPric as low,
    ClsPric as close,
    TtlTradgVol as volume
FROM champion_market.raw_equity_ohlc
ORDER BY TradDt DESC, TckrSymb
LIMIT 10;
" --format PrettyCompact
```

Expected output: Table with 10 rows showing sample OHLC data.

#### Test Query 3: Symbol/Date Range Query (Normalized)

```sql
docker compose exec clickhouse clickhouse-client --query "
SELECT 
    symbol,
    trade_date,
    close,
    volume,
    (close - prev_close) / prev_close * 100 as daily_return_pct
FROM champion_market.normalized_equity_ohlc
WHERE symbol = 'SYMBOL001'
ORDER BY trade_date DESC;
" --format PrettyCompact
```

Expected output: All 5 days of data for SYMBOL001 with calculated returns.

#### Test Query 4: Aggregation Query

```sql
docker compose exec clickhouse clickhouse-client --query "
SELECT 
    trade_date,
    count(DISTINCT symbol) as symbols,
    sum(volume) as total_volume,
    avg(close) as avg_close,
    max(high) as max_high,
    min(low) as min_low
FROM champion_market.normalized_equity_ohlc
GROUP BY trade_date
ORDER BY trade_date DESC;
" --format PrettyCompact
```

Expected output: 5 rows (one per day) showing aggregate statistics.

#### Test Query 5: Features Query

```sql
docker compose exec clickhouse-clickhouse clickhouse-client --query "
SELECT 
    symbol,
    trade_date,
    sma_20,
    rsi_14,
    macd,
    CASE
        WHEN rsi_14 > 70 THEN 'Overbought'
        WHEN rsi_14 < 30 THEN 'Oversold'
        ELSE 'Neutral'
    END as rsi_signal
FROM champion_market.features_equity_indicators
WHERE symbol IN ('SYMBOL001', 'SYMBOL002', 'SYMBOL003')
ORDER BY symbol, trade_date DESC;
" --format PrettyCompact
```

Expected output: Technical indicators for 3 symbols across 5 days.

### Step 7: Verify Materialized View

```sql
docker compose exec clickhouse clickhouse-client --query "
SELECT 
    trade_date,
    exchange,
    total_symbols,
    total_volume,
    round(avg_close_price, 2) as avg_price
FROM champion_market.equity_ohlc_daily_summary
ORDER BY trade_date DESC;
" --format PrettyCompact
```

Expected output: Aggregated daily statistics from materialized view.

## Acceptance Criteria Checklist

- [ ] ClickHouse service starts successfully via docker compose
- [ ] All three tables are created: `raw_equity_ohlc`, `normalized_equity_ohlc`, `features_equity_indicators`
- [ ] Materialized view `equity_ohlc_daily_summary` is created
- [ ] Sample Parquet data is generated successfully
- [ ] Raw data loads correctly from Parquet (50 rows expected)
- [ ] Normalized data loads correctly from Parquet (50 rows expected)
- [ ] Features data loads correctly from Parquet (50 rows expected)
- [ ] Symbol/date range query returns correct results
- [ ] Aggregation query returns correct row counts
- [ ] Materialized view contains correct aggregated data

## Troubleshooting

### Issue: Container won't start

**Solution:**

```bash
# Check logs
docker compose logs clickhouse

# Try removing volumes and restarting
docker compose down -v
docker compose up -d clickhouse
```

### Issue: Permission denied errors

**Solution:**

```bash
# Check file permissions
ls -la warehouse/clickhouse/

# Ensure init scripts are readable
chmod 644 warehouse/clickhouse/init/*.sql
chmod 644 warehouse/clickhouse/users.xml
```

### Issue: Tables not created

**Solution:**

```bash
# Manually run init SQL
docker compose exec clickhouse clickhouse-client < warehouse/clickhouse/init/01_create_tables.sql
```

### Issue: Python dependencies missing

**Solution:**

```bash
# Install required packages
pip install polars clickhouse-connect
```

### Issue: Data load fails

**Solution:**

```bash
# Run in dry-run mode to see errors
python -m champion.warehouse.clickhouse.batch_loader \
    --table raw_equity_ohlc \
    --source data/lake/raw/equity_ohlc/ \
    --dry-run \
    --verbose

# Check ClickHouse is accessible
curl http://localhost:8123/ping
```

## Cleanup

To stop and remove all resources:

```bash
# Stop containers
docker compose down

# Remove volumes (WARNING: deletes all data)
docker compose down -v

# Remove generated sample data
rm -rf data/lake/raw/equity_ohlc/date=*
rm -rf data/lake/normalized/equity_ohlc/year=*
rm -rf data/lake/features/equity_indicators/year=*
```

## Next Steps

After successful testing:

1. Review the implementation and ensure it meets all requirements
2. Document any issues or improvements needed
3. Consider adding integration tests
4. Plan for production deployment

## References

- ClickHouse Documentation: <https://clickhouse.com/docs>
- Repository README: `/warehouse/README.md`
- Architecture Documentation: `/docs/architecture/data-platform.md`
