# NSE Index Constituents & Rebalances

## Overview

This module implements ingestion of NSE index membership data for indices like NIFTY50, BANKNIFTY, and others. It captures current constituent lists, historical changes, and rebalance events with accurate effective dates.

## Features

- **Multi-Index Support**: Scrapes data for NIFTY50, BANKNIFTY, NIFTYMIDCAP50, NIFTYIT, and more
- **Historical Tracking**: Captures constituent changes over time with effective dates
- **Rebalance Events**: Records additions, removals, and weight changes
- **Weight Information**: Captures constituent weight in index (if available)
- **Parquet Storage**: Partitioned by index name and effective date for efficient querying
- **ClickHouse Integration**: Fast analytical queries on index membership
- **Prefect Orchestration**: Automated daily checks for constituent changes
- **MLflow Tracking**: Complete observability and metrics logging

## Architecture

```text
NSE Index API
     ↓
[IndexConstituentScraper]
     ↓
JSON Files (Raw)
     ↓
[IndexConstituentParser]
     ↓
Event Structures (with Avro schema)
     ↓
Parquet Files (Partitioned by index/date)
     ↓
ClickHouse (index_constituent table)
```

## Data Model

### Avro Schema

The `index_constituent.avsc` schema defines:

- **index_name**: Index identifier (e.g., 'NIFTY50', 'BANKNIFTY')
- **symbol**: Trading symbol (e.g., 'RELIANCE', 'HDFCBANK')
- **isin**: International Securities Identification Number
- **company_name**: Company name
- **effective_date**: Date when constituent change takes effect
- **action**: Constituent action (ADD, REMOVE, REBALANCE)
- **weight**: Constituent weight in index (percentage, if available)
- **free_float_market_cap**: Free float market cap in INR crores
- **sector**: Sector classification
- **industry**: Industry classification

### ClickHouse Table

```sql
CREATE TABLE champion_market.index_constituent (
    -- Metadata
    event_id String,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    source LowCardinality(String),
    schema_version LowCardinality(String),
    entity_id String,
    
    -- Constituent Data
    index_name LowCardinality(String),
    symbol String,
    isin Nullable(String),
    company_name Nullable(String),
    effective_date Date,
    action LowCardinality(String),  -- ADD, REMOVE, REBALANCE
    weight Nullable(Float64),
    free_float_market_cap Nullable(Float64),
    shares_for_index Nullable(Int64),
    announcement_date Nullable(Date),
    index_category LowCardinality(Nullable(String)),
    sector LowCardinality(Nullable(String)),
    industry LowCardinality(Nullable(String)),
    metadata Map(String, String),
    
    -- Partition columns
    year Int64,
    month Int64,
    day Int64
)
ENGINE = MergeTree()
PARTITION BY (index_name, toYear(effective_date))
ORDER BY (index_name, symbol, effective_date, event_time)
TTL effective_date + INTERVAL 10 YEAR;
```

## Usage

### Quick Start

```bash
# Run ETL pipeline for NIFTY50 and BANKNIFTY (today)
python run_index_etl.py

# Run for specific indices
python run_index_etl.py --indices NIFTY50 BANKNIFTY NIFTYIT

# Run for specific date
python run_index_etl.py --date 2026-01-11

# Skip ClickHouse loading (only create Parquet files)
python run_index_etl.py --no-clickhouse
```

### Programmatic Usage

```python
from datetime import date
from champion.orchestration.flows import index_constituent_etl_flow

# Run ETL flow
result = index_constituent_etl_flow(
    indices=["NIFTY50", "BANKNIFTY"],
    effective_date=date.today(),
    load_to_clickhouse=True,
)

print(f"Status: {result['status']}")
print(f"Duration: {result['duration_seconds']} seconds")
```

### Individual Components

```python
# Just scraping
from champion.scrapers.nse.index_constituent import IndexConstituentScraper

with IndexConstituentScraper() as scraper:
    files = scraper.scrape(indices=["NIFTY50", "BANKNIFTY"])
    print(f"Scraped files: {files}")

# Just parsing
from champion.parsers.index_constituent_parser import IndexConstituentParser
from datetime import date
from pathlib import Path

parser = IndexConstituentParser()
events = parser.parse(
    file_path=Path("data/indices/NIFTY50_constituents.json"),
    index_name="NIFTY50",
    effective_date=date.today(),
    action="ADD",
)
print(f"Parsed {len(events)} constituents")
```

## Query Examples

### Current Index Membership

Get current constituents of NIFTY50:

```sql
SELECT 
    symbol,
    company_name,
    weight,
    sector,
    industry
FROM champion_market.index_constituent
WHERE index_name = 'NIFTY50'
  AND action = 'ADD'
ORDER BY weight DESC NULLS LAST, symbol;
```

### Compare Two Indices

Find common constituents between NIFTY50 and BANKNIFTY:

```sql
SELECT 
    n.symbol,
    n.company_name,
    n.weight as nifty50_weight,
    b.weight as banknifty_weight
FROM (
    SELECT symbol, company_name, weight
    FROM champion_market.index_constituent
    WHERE index_name = 'NIFTY50' AND action = 'ADD'
) n
INNER JOIN (
    SELECT symbol, weight
    FROM champion_market.index_constituent
    WHERE index_name = 'BANKNIFTY' AND action = 'ADD'
) b ON n.symbol = b.symbol
ORDER BY n.symbol;
```

### Historical Changes (Quarterly)

Show constituent additions and removals over the last quarter:

```sql
WITH recent_changes AS (
    SELECT 
        index_name,
        symbol,
        company_name,
        action,
        effective_date,
        weight
    FROM champion_market.index_constituent
    WHERE effective_date >= today() - INTERVAL 90 DAY
      AND index_name IN ('NIFTY50', 'BANKNIFTY')
)
SELECT 
    index_name,
    action,
    symbol,
    company_name,
    effective_date,
    weight
FROM recent_changes
ORDER BY index_name, effective_date DESC, action, symbol;
```

### Index Composition by Sector

Analyze sector distribution in NIFTY50:

```sql
SELECT 
    sector,
    COUNT(*) as num_constituents,
    SUM(weight) as total_weight,
    ROUND(AVG(weight), 2) as avg_weight
FROM champion_market.index_constituent
WHERE index_name = 'NIFTY50'
  AND action = 'ADD'
  AND sector IS NOT NULL
GROUP BY sector
ORDER BY total_weight DESC;
```

### Track Specific Symbol Across Indices

Find all indices a specific symbol belongs to:

```sql
SELECT 
    index_name,
    weight,
    effective_date,
    sector,
    industry
FROM champion_market.index_constituent
WHERE symbol = 'RELIANCE'
  AND action = 'ADD'
ORDER BY index_name;
```

### Rebalance History

Track weight changes for a symbol over time:

```sql
SELECT 
    effective_date,
    index_name,
    action,
    weight,
    free_float_market_cap
FROM champion_market.index_constituent
WHERE symbol = 'HDFCBANK'
  AND index_name = 'NIFTY50'
ORDER BY effective_date DESC;
```

## Scheduled Execution

### Daily Prefect Flow

The index constituent ETL flow can be scheduled to run daily to capture changes:

```python
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=index_constituent_etl_flow,
    name="nse-index-constituent-daily",
    schedule=CronSchedule(cron="0 2 * * *", timezone="Asia/Kolkata"),  # 2 AM IST
    parameters={
        "indices": ["NIFTY50", "BANKNIFTY"],
        "load_to_clickhouse": True,
    },
)

deployment.apply()
```

## Data Sources

### NSE API Endpoints

The scraper uses NSE's public API endpoints:

- **NIFTY50**: `https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050`
- **BANKNIFTY**: `https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20BANK`
- **NIFTYMIDCAP50**: `https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20MIDCAP%2050`
- **NIFTYIT**: `https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20IT`

### Response Format

The API returns JSON with the following structure:

```json
{
  "name": "NIFTY 50",
  "data": [
    {
      "symbol": "RELIANCE",
      "series": "EQ",
      "open": 2850.0,
      "high": 2875.0,
      "low": 2840.0,
      "close": 2860.0,
      "last": 2860.0,
      "previousClose": 2855.0,
      "change": 5.0,
      "pChange": 0.18,
      "totalTradedVolume": 5000000,
      "totalTradedValue": 14300000000,
      "indexWeight": 10.5,
      "ffmc": 1500000,
      "meta": {
        "isin": "INE002A01018",
        "companyName": "Reliance Industries Ltd.",
        "sector": "Energy",
        "industry": "Refineries"
      }
    }
  ]
}
```

## Storage Layout

### Parquet Files

```text
data/lake/normalized/index_constituent/
├── index_name=NIFTY50/
│   ├── year=2026/
│   │   ├── month=01/
│   │   │   ├── day=11/
│   │   │   │   └── data_2026-01-11.parquet
│   │   │   └── day=12/
│   │   │       └── data_2026-01-12.parquet
│   │   └── month=02/
│   └── year=2025/
└── index_name=BANKNIFTY/
    └── year=2026/
        └── month=01/
            └── day=11/
                └── data_2026-01-11.parquet
```

## Verification

### Verification Checklist

- [x] Schema: Avro schema created in `schemas/reference-data/index_constituent.avsc`
- [x] ClickHouse: Table definition added to DDL scripts
- [x] Scraper: `IndexConstituentScraper` implemented with NSE API integration
- [x] Parser: `IndexConstituentParser` with Polars-based parsing
- [x] Tasks: Prefect tasks for scrape, parse, write Parquet, load ClickHouse
- [x] Flow: Complete ETL flow with MLflow tracking
- [x] Runner: CLI script for manual execution
- [x] Documentation: Usage examples and query patterns

### Manual Verification Steps

1. **Run ETL Pipeline**:

   ```bash
   python run_index_etl.py --indices NIFTY50 BANKNIFTY
   ```

2. **Check Parquet Files**:

   ```bash
   ls -lR data/lake/normalized/index_constituent/
   ```

3. **Query ClickHouse**:

   ```bash
   clickhouse-client --query "
       SELECT index_name, COUNT(*) as constituents
       FROM champion_market.index_constituent
       WHERE action = 'ADD'
       GROUP BY index_name
   "
   ```

4. **Verify Data Quality**:

   ```bash
   clickhouse-client --query "
       SELECT 
           index_name,
           COUNT(*) as total,
           COUNT(DISTINCT symbol) as unique_symbols,
           SUM(weight) as total_weight,
           MIN(effective_date) as min_date,
           MAX(effective_date) as max_date
       FROM champion_market.index_constituent
       GROUP BY index_name
   "
   ```

## Troubleshooting

### NSE API Blocking

If NSE blocks requests:

1. The scraper includes proper User-Agent headers
2. It visits the main page before API calls
3. Add delays between requests if needed:

   ```python
   import time
   time.sleep(2)  # 2 second delay
   ```

### Missing Weight Data

Some indices may not provide weight information in the API response. In this case:

- The `weight` field will be `NULL` in the database
- Use free float market cap as a proxy for importance
- Historical weight data may need to be obtained from NSE circulars

### Date Handling

- The scraper uses the current date as `effective_date` by default
- For historical data, manually specify the effective date
- Announcement dates may differ from effective dates

## Future Enhancements

1. **Historical Backfill**: Scrape historical constituent lists from NSE archives
2. **Circular Parsing**: Parse NSE circulars for official rebalance announcements
3. **Weight Tracking**: Track weight changes over time for REBALANCE events
4. **Index Creation**: Support for custom index creation and backtesting
5. **Constituent Alerts**: Notify when constituents change
6. **API Rate Limiting**: Implement proper rate limiting for NSE API

## References

- NSE India Website: <https://www.nseindia.com/>
- NSE Indices: <https://www.nse.gov.in/products-services/indices>
- NIFTY Methodology: <https://www.niftyindices.com/methodology>
