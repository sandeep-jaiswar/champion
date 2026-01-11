# BSE Data Integration - Verification & Usage Guide

## Overview

This document provides verification queries and usage instructions for the BSE equity data integration. BSE data serves as a secondary data source for redundancy and coverage of symbols not listed on NSE.

## Key Features

1. **Dual Exchange Support**: Ingests data from both NSE and BSE
2. **ISIN-based Deduplication**: Automatically deduplicates overlapping symbols (NSE takes priority)
3. **Source Tracking**: All records tagged with `source` field for provenance
4. **Fault Tolerance**: Pipeline continues even if one exchange is unavailable
5. **Unified Schema**: BSE data normalized to match NSE schema structure

## Running the ETL Pipeline

### Combined NSE + BSE Pipeline

```bash
# Run for yesterday's data
cd ingestion/nse-scraper
python3 run_combined_etl.py

# Run for specific date
python3 run_combined_etl.py --date 2026-01-09

# Run without ClickHouse loading (Parquet only)
python3 run_combined_etl.py --date 2026-01-09 --no-clickhouse

# Run NSE only (disable BSE)
python3 run_combined_etl.py --date 2026-01-09 --no-bse
```

### BSE-Only Pipeline (for testing)

```bash
cd ingestion/nse-scraper
python3 -c "from champion.orchestration.flows.combined_flows import bse_only_etl_flow; \
from datetime import date; \
bse_only_etl_flow(trade_date=date(2026, 1, 9))"
```

## Verification Queries

### 1. Check Data Availability by Source

**ClickHouse:**

```sql
-- Count records by source for a specific date
SELECT 
    source,
    COUNT(*) as total_records,
    COUNT(DISTINCT TckrSymb) as unique_symbols,
    COUNT(DISTINCT ISIN) as unique_isins
FROM champion_market.normalized_equity_ohlc
WHERE TradDt = '2026-01-09'
GROUP BY source
ORDER BY source;
```

**Expected Output:**

```text
┌─source──────────────┬─total_records─┬─unique_symbols─┬─unique_isins─┐
│ bse_eq_bhavcopy     │          2500 │           2500 │         2450 │
│ nse_cm_bhavcopy     │          1800 │           1800 │         1780 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

### 2. Identify BSE-Only Symbols

**Using Script:**

```bash
# From Parquet files
cd ingestion/nse-scraper
python3 scripts/identify_bse_only_symbols.py --date 2026-01-09

# From ClickHouse
python3 scripts/identify_bse_only_symbols.py --date 2026-01-09 --clickhouse
```

**ClickHouse Query:**

```sql
-- Find symbols listed only on BSE (not on NSE)
SELECT 
    TckrSymb as symbol,
    ISIN,
    FinInstrmNm as company_name,
    ClsPric as close_price,
    TtlTradgVol as volume
FROM champion_market.normalized_equity_ohlc
WHERE source = 'bse_eq_bhavcopy'
  AND TradDt = '2026-01-09'
  AND ISIN IS NOT NULL
  AND ISIN NOT IN (
      SELECT DISTINCT ISIN
      FROM champion_market.normalized_equity_ohlc
      WHERE source = 'nse_cm_bhavcopy'
        AND TradDt = '2026-01-09'
        AND ISIN IS NOT NULL
  )
ORDER BY volume DESC
LIMIT 20;
```

### 3. Verify Deduplication

**Check for ISIN duplicates (should be none after deduplication):**

```sql
-- Count records per ISIN on same date (should all be 1 after dedup)
SELECT 
    ISIN,
    COUNT(*) as record_count,
    groupArray(source) as sources
FROM champion_market.normalized_equity_ohlc
WHERE TradDt = '2026-01-09'
  AND ISIN IS NOT NULL
GROUP BY ISIN
HAVING record_count > 1
ORDER BY record_count DESC;
```

**Expected:** Empty result (no duplicates)

### 4. Overlapping Symbols Analysis

```sql
-- Find symbols present on both exchanges
WITH nse_isins AS (
    SELECT DISTINCT ISIN
    FROM champion_market.normalized_equity_ohlc
    WHERE source = 'nse_cm_bhavcopy'
      AND TradDt = '2026-01-09'
      AND ISIN IS NOT NULL
),
bse_isins AS (
    SELECT DISTINCT ISIN
    FROM champion_market.normalized_equity_ohlc
    WHERE source = 'bse_eq_bhavcopy'
      AND TradDt = '2026-01-09'
      AND ISIN IS NOT NULL
)
SELECT 
    COUNT(*) as overlapping_isins
FROM nse_isins
INNER JOIN bse_isins USING (ISIN);
```

### 5. Data Quality Checks

**Check for null ISINs:**

```sql
SELECT 
    source,
    COUNT(*) as total_records,
    SUM(CASE WHEN ISIN IS NULL THEN 1 ELSE 0 END) as null_isins,
    round(SUM(CASE WHEN ISIN IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
FROM champion_market.normalized_equity_ohlc
WHERE TradDt = '2026-01-09'
GROUP BY source;
```

**Price range validation:**

```sql
-- Verify price data is reasonable
SELECT 
    source,
    MIN(ClsPric) as min_close,
    MAX(ClsPric) as max_close,
    AVG(ClsPric) as avg_close,
    COUNT(CASE WHEN ClsPric <= 0 THEN 1 END) as invalid_prices
FROM champion_market.normalized_equity_ohlc
WHERE TradDt = '2026-01-09'
GROUP BY source;
```

### 6. Compare BSE Symbol with NSE (Spot Check)

```sql
-- Compare same symbol on both exchanges (if listed)
WITH symbol_data AS (
    SELECT 
        source,
        TckrSymb,
        ISIN,
        ClsPric as close_price,
        TtlTradgVol as volume,
        TtlTrfVal as turnover
    FROM champion_market.normalized_equity_ohlc
    WHERE TradDt = '2026-01-09'
      AND ISIN = 'INE002A01018'  -- RELIANCE
)
SELECT * FROM symbol_data;
```

**Expected:** Only one record (NSE) due to deduplication

### 7. Historical Data Coverage

```sql
-- Check data availability over time by source
SELECT 
    TradDt as trade_date,
    source,
    COUNT(*) as records
FROM champion_market.normalized_equity_ohlc
WHERE TradDt >= '2026-01-01'
GROUP BY trade_date, source
ORDER BY trade_date DESC, source;
```

## Data Lake Structure

### Parquet Files Location

```text
data/lake/normalized/equity_ohlc/
├── year=2026/
│   └── month=01/
│       └── day=09/
│           ├── bhavcopy_20260109.parquet      # NSE data
│           └── bhavcopy_bse_20260109.parquet  # BSE data (if available)
```

### Reading Combined Data

```python
import polars as pl

# Read all data for a date
df = pl.read_parquet("data/lake/normalized/equity_ohlc/year=2026/month=01/day=09/*.parquet")

# Filter by source
nse_df = df.filter(pl.col("source") == "nse_cm_bhavcopy")
bse_df = df.filter(pl.col("source") == "bse_eq_bhavcopy")

# Find BSE-only symbols
bse_only = bse_df.filter(
    ~pl.col("ISIN").is_in(nse_df["ISIN"].unique())
)
```

## Monitoring & Observability

### MLflow Experiments

View pipeline runs and metrics:

```bash
# Open MLflow UI
open http://localhost:5000

# Look for experiment: "combined-equity-etl"
```

### Key Metrics

- `nse_rows`: Number of NSE symbols processed
- `bse_rows`: Number of BSE symbols processed  
- `bse_unique_rows`: BSE symbols after deduplication
- `duplicates_removed`: Number of overlapping symbols removed
- `final_rows`: Total rows in output

### Prometheus Metrics

```bash
# View metrics
curl http://localhost:9090/metrics | grep -E "bse|nse"
```

**Available Metrics:**

- `nse_scrape_success_total`
- `nse_parse_rows_total`
- `bse_scrape_success_total` (new)
- `bse_parse_rows_total` (new)

## Troubleshooting

### BSE Scrape Fails

If BSE scraping fails, the pipeline will continue with NSE data only:

```text
2026-01-09 10:00:00 WARNING bse_bhavcopy_scrape_failed trade_date=2026-01-09
2026-01-09 10:00:00 INFO using nse_only
```

**Check:**

1. BSE website availability: `https://www.bseindia.com/`
2. Date format (BSE uses DDMMYY): verify in logs
3. Network connectivity

### No BSE-Only Symbols Found

This is normal if:

- BSE data is unavailable for the date
- All BSE symbols are also listed on NSE
- Pipeline ran with `--no-bse` flag

### Duplicate ISIN Errors

If you find duplicates after deduplication:

```sql
-- Check which source has duplicates
SELECT ISIN, source, COUNT(*)
FROM champion_market.normalized_equity_ohlc
WHERE TradDt = '2026-01-09'
GROUP BY ISIN, source
HAVING COUNT(*) > 1;
```

**Resolution:** Re-run pipeline for that date

## Performance

### Typical Processing Times

- **NSE Scrape**: 2-5 seconds
- **BSE Scrape**: 3-7 seconds
- **NSE Parse**: 1-3 seconds (1800 symbols)
- **BSE Parse**: 2-5 seconds (2500 symbols)
- **Deduplication**: <1 second
- **Parquet Write**: 1-2 seconds
- **ClickHouse Load**: 2-5 seconds

**Total**: ~15-30 seconds for complete pipeline

### Optimization Tips

1. Use `--no-clickhouse` for faster Parquet-only processing
2. Run in parallel for date ranges (separate processes)
3. Adjust ClickHouse batch size for large loads

## Schema Mapping

### BSE to NSE Column Mapping

| BSE Column | NSE Column | Description |
|------------|------------|-------------|
| SC_CODE | FinInstrmId | Scrip code → Instrument ID |
| SC_NAME | TckrSymb | Symbol name |
| ISIN_CODE | ISIN | ISIN identifier |
| OPEN | OpnPric | Opening price |
| HIGH | HghPric | High price |
| LOW | LwPric | Low price |
| CLOSE | ClsPric | Closing price |
| LAST | LastPric | Last traded price |
| PREVCLOSE | PrvsClsgPric | Previous close |
| NO_OF_SHRS | TtlTradgVol | Trading volume |
| NET_TURNOV | TtlTrfVal | Turnover value |
| NO_TRADES | TtlNbOfTxsExctd | Number of trades |

### Source Field Values

- `nse_cm_bhavcopy`: NSE Capital Market data
- `bse_eq_bhavcopy`: BSE Equity data

## Support

For issues or questions:

1. Check logs in MLflow: `http://localhost:5000`
2. Review Prometheus metrics: `http://localhost:9090/metrics`
3. Check Parquet files: `data/lake/normalized/equity_ohlc/`
4. Run verification queries above
