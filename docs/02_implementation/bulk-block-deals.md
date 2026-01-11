# Bulk and Block Deals Ingestion

## Overview

This module implements the ingestion pipeline for NSE bulk and block deals data. The pipeline scrapes, parses, stores, and validates daily bulk and block deal transactions.

## What are Bulk and Block Deals?

### Bulk Deals

Transactions in a single scrip where the **total quantity traded is more than 0.5%** of the number of equity shares of the company listed on the exchange.

**Key characteristics:**

- Regular market window (9:15 AM - 3:30 PM)
- Price discovery through normal matching
- Disclosed at end of trading day
- Transparency requirement for large positions

### Block Deals

Transactions executed through a **separate trading window** with minimum quantity requirements:

- **Minimum 5 lakh shares**, OR
- **Minimum Rs 5 crore value** (whichever is less)

**Key characteristics:**

- Separate window: 8:45 AM - 9:00 AM and 2:05 PM - 2:10 PM
- Pre-negotiated trades
- Price within ±1% of previous day's close or current market price
- Institutional/large investor transactions

## Architecture

### Data Flow

```text
┌─────────────────┐
│   NSE API       │
│ (Bulk/Block)    │
└────────┬────────┘
         │ HTTPS/JSON
         ↓
┌─────────────────┐
│    Scraper      │
│ (JSON files)    │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│     Parser      │
│  (Polars DF)    │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ Parquet Lake    │
│  (Partitioned)  │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│   ClickHouse    │
│   (Analytics)   │
└─────────────────┘
```

### Components

#### 1. Scraper (`src/scrapers/bulk_block_deals.py`)

- **Purpose**: Fetch bulk and block deals data from NSE APIs
- **Endpoints**:
  - Bulk: `https://www.nseindia.com/api/historical/bulk-deals`
  - Block: `https://www.nseindia.com/api/historical/block-deals`
- **Output**: JSON files per deal type and date
- **Retry**: 3 attempts with exponential backoff

#### 2. Parser (`src/parsers/bulk_block_deals_parser.py`)

- **Purpose**: Parse JSON to standardized events
- **Technology**: Polars (high-performance DataFrame library)
- **Transformations**:
  - Separate BUY and SELL transactions
  - Normalize client names and symbols
  - Add metadata (event_id, timestamps)
- **Output**: List of event dictionaries

#### 3. Tasks (`src/tasks/bulk_block_deals_tasks.py`)

Prefect tasks for orchestration:

- `scrape_bulk_block_deals`: Fetch data from NSE
- `parse_bulk_block_deals`: Parse JSON to events
- `write_bulk_block_deals_parquet`: Write to Parquet lake
- `load_bulk_block_deals_clickhouse`: Load to ClickHouse

#### 4. Flow (`src/orchestration/bulk_block_deals_flow.py`)

Prefect flows:

- `bulk_block_deals_etl_flow`: Single date ETL
- `bulk_block_deals_date_range_etl_flow`: Date range ETL

## Schema

### Parquet Schema

See `schemas/parquet/bulk_block_deals.json` for complete Avro schema.

**Key fields:**

- `deal_date`: Date of the deal
- `symbol`: Trading symbol (e.g., 'RELIANCE')
- `client_name`: Client/entity name
- `quantity`: Number of shares traded
- `avg_price`: Average deal price
- `deal_type`: 'BULK' or 'BLOCK'
- `transaction_type`: 'BUY' or 'SELL'
- `exchange`: 'NSE'

### ClickHouse Schema

Table: `champion_market.bulk_block_deals`

**Engine**: MergeTree  
**Partitioning**: `(deal_type, year, month)`  
**Ordering**: `(symbol, deal_date, transaction_type, event_time)`  
**TTL**: 10 years

## Usage

### Command Line

```bash
cd ingestion/nse-scraper

# Run for yesterday (default)
python run_bulk_block_deals.py

# Run for specific date
python run_bulk_block_deals.py --date 2026-01-10

# Run for date range
python run_bulk_block_deals.py --start-date 2025-12-01 --end-date 2025-12-31

# Run for specific deal type
python run_bulk_block_deals.py --deal-type bulk
python run_bulk_block_deals.py --deal-type block

# Skip ClickHouse loading
python run_bulk_block_deals.py --no-clickhouse
```

### Programmatic

```python
from datetime import date
from src.orchestration.bulk_block_deals_flow import bulk_block_deals_etl_flow

# Run ETL for specific date
result = bulk_block_deals_etl_flow(
    target_date="2026-01-10",
    deal_type="both",
    load_to_clickhouse=True,
)

print(f"Processed {result['total_events']} events")
print(f"Loaded {result['clickhouse_rows']} rows to ClickHouse")
```

## Validation

### Data Quality Checks

```sql
-- Count deals by type
SELECT 
    deal_type,
    COUNT(*) as deals,
    SUM(quantity) as total_quantity,
    AVG(avg_price) as avg_price
FROM champion_market.bulk_block_deals
WHERE deal_date >= '2026-01-01'
GROUP BY deal_type;

-- Top 10 bulk deals by quantity
SELECT 
    deal_date,
    symbol,
    client_name,
    quantity,
    avg_price,
    transaction_type
FROM champion_market.bulk_block_deals
WHERE deal_type = 'BULK'
  AND deal_date >= '2026-01-01'
ORDER BY quantity DESC
LIMIT 10;

-- Daily summary
SELECT 
    deal_date,
    deal_type,
    COUNT(*) as deals,
    SUM(quantity) as total_quantity,
    SUM(quantity * avg_price) as total_value
FROM champion_market.bulk_block_deals
WHERE deal_date >= '2026-01-01'
GROUP BY deal_date, deal_type
ORDER BY deal_date DESC, deal_type;

-- Most active clients
SELECT 
    client_name,
    COUNT(*) as deals,
    SUM(quantity) as total_quantity,
    AVG(avg_price) as avg_price
FROM champion_market.bulk_block_deals
WHERE deal_date >= '2026-01-01'
GROUP BY client_name
ORDER BY deals DESC
LIMIT 20;
```

### Verification Checklist

- [ ] Daily deals captured for specified date range
- [ ] No missing trading days (cross-check with trading calendar)
- [ ] BUY and SELL transactions properly separated
- [ ] Quantities and prices are positive and reasonable
- [ ] Symbols match symbol master
- [ ] Client names are properly parsed
- [ ] Partitioned Parquet files exist for each date
- [ ] ClickHouse data matches Parquet data

## Data Quality Notes

### Known Caveats

1. **NSE API Availability**
   - Historical data may not be available for all dates
   - API may return empty data for non-trading days
   - Weekend/holiday requests will return no data

2. **Data Completeness**
   - Some days may have zero deals (legitimate)
   - Block deals are less frequent than bulk deals
   - API response format may vary over time

3. **Client Name Variations**
   - Client names may have spelling variations
   - Same entity may appear with different names
   - Manual reconciliation may be needed for analysis

4. **Price Validation**
   - Prices should be within reasonable range
   - Cross-check with OHLC data for sanity
   - Extreme prices may indicate data issues

5. **Quantity Validation**
   - Bulk deals: quantity > 0.5% of shares outstanding
   - Block deals: quantity >= 500,000 shares or value >= Rs 5 crore
   - Very small quantities may indicate parsing errors

### Data Quality Metrics

Monitor these metrics:

- **Scrape success rate**: Should be >95%
- **Parse error rate**: Should be <1%
- **Daily deal count**: Varies, but typically 10-100 deals/day
- **Price outliers**: Flag prices >5x average for symbol
- **Quantity outliers**: Flag quantities >10% of daily volume

## Troubleshooting

### No deals for a date

```bash
# Check if it's a trading day
clickhouse-client --query "
SELECT is_trading_day, day_type, holiday_name
FROM champion_market.trading_calendar
WHERE trade_date = '2026-01-10'
"
```

### Scraping errors

- **Network timeout**: Increase timeout in config
- **API blocked**: NSE may block excessive requests
- **Invalid response**: Check NSE website for API changes

### Parsing errors

- Check JSON structure has expected fields
- Verify column names match expected format
- Review logs for specific error messages

## Performance

### Benchmarks

- **Scrape**: ~2-5 seconds per deal type per date
- **Parse**: ~100ms for typical daily data (50-100 deals)
- **Parquet write**: ~50ms
- **ClickHouse load**: ~100ms

### Scaling

- **Date range**: Can process months in parallel using Prefect
- **Large volumes**: Parser uses Polars for memory efficiency
- **ClickHouse**: Handles millions of rows efficiently

## Future Enhancements

1. **Real-time ingestion**: Scrape during trading day
2. **Client deduplication**: Entity resolution for client names
3. **Anomaly detection**: Flag unusual deals automatically
4. **Cross-reference**: Link deals to corporate actions
5. **Visualization**: Dashboard for deal analysis
6. **Alerts**: Notify on large deals or patterns

## References

- [NSE Bulk Deals](https://www.nseindia.com/report-detail/eq_security)
- [SEBI Disclosure Requirements](https://www.sebi.gov.in/)
- Schema: `schemas/parquet/bulk_block_deals.json`
- ClickHouse DDL: `warehouse/clickhouse/init/01_create_tables.sql`
