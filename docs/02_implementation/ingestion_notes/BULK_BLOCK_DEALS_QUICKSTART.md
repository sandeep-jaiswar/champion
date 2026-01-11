# Bulk and Block Deals - Quick Start Guide

## Overview

This implementation provides complete ETL pipeline for NSE bulk and block deals data.

## Quick Start

### 1. Install Dependencies

```bash
cd ingestion/nse-scraper
pip install pytest polars structlog prometheus-client
```

### 2. Run Tests

```bash
# Run all tests
python3 -m pytest tests/unit/test_bulk_block_deals_parser.py -v

# Expected output: 11 passed ✅
```

### 3. Verify with Sample Data

```bash
# Test parser and Parquet writer
python3 verify_bulk_block_deals.py
```

Expected output:

```text
✓ Parsed 6 bulk deal events
✓ Parsed 2 block deal events
✓ Written to Parquet files
✅ Verification complete!
```

### 4. Run ETL Pipeline

```bash
# For yesterday (default)
python3 run_bulk_block_deals.py

# For specific date
python3 run_bulk_block_deals.py --date 2026-01-10

# For date range (e.g., full month)
python3 run_bulk_block_deals.py --start-date 2025-12-01 --end-date 2025-12-31

# Only bulk deals
python3 run_bulk_block_deals.py --deal-type bulk

# Skip ClickHouse loading (Parquet only)
python3 run_bulk_block_deals.py --no-clickhouse
```

## Data Structure

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `deal_date` | Date | Date of the deal |
| `symbol` | String | Trading symbol (e.g., 'RELIANCE') |
| `client_name` | String | Client/entity name |
| `quantity` | Int64 | Number of shares traded |
| `avg_price` | Float64 | Average deal price |
| `deal_type` | Enum | 'BULK' or 'BLOCK' |
| `transaction_type` | Enum | 'BUY' or 'SELL' |
| `exchange` | String | 'NSE' |

### Partitioning

Parquet files are partitioned by:

```text
data/lake/bulk_block_deals/
  deal_type=BULK/
    year=2026/
      month=01/
        day=10/
          bulk_deals_20260110.parquet
```

## Verification

### Check Data in ClickHouse

```sql
-- Count deals
SELECT deal_type, COUNT(*) 
FROM champion_market.bulk_block_deals 
GROUP BY deal_type;

-- Top 10 by quantity
SELECT deal_date, symbol, quantity, avg_price, deal_type
FROM champion_market.bulk_block_deals
ORDER BY quantity DESC
LIMIT 10;
```

See `docs/implementation/bulk-block-deals-verification-queries.md` for 14+ verification queries.

### Check Parquet Files

```bash
# List Parquet files
find data/lake/bulk_block_deals -name "*.parquet"

# Read with Python
python3 -c "
import polars as pl
df = pl.read_parquet('data/lake/bulk_block_deals/**/*.parquet')
print(df.head())
print(f'Total rows: {len(df)}')
"
```

## Troubleshooting

### No data for a date

- Check if it's a trading day in `champion_market.trading_calendar`
- NSE may not have any deals for that day (legitimate)
- Weekend/holiday requests return empty data

### Scraping errors

- Network issues: NSE may be temporarily unavailable
- API changes: Check NSE website for new endpoints
- Rate limiting: Add delay between requests

### Parser errors

- JSON format mismatch: NSE may have changed response format
- Missing fields: Check `client_name`, `quantity`, `avg_price` fields
- Check logs for specific error messages

## Architecture

```text
NSE API → Scraper → Parser → Parquet → ClickHouse
                        ↓
                    Prefect Flow
                        ↓
                   Monitoring
```

## Components

| Component | Path | Purpose |
|-----------|------|---------|
| Scraper | `src/scrapers/bulk_block_deals.py` | Fetch from NSE |
| Parser | `src/parsers/bulk_block_deals_parser.py` | Parse & normalize |
| Tasks | `src/tasks/bulk_block_deals_tasks.py` | Prefect tasks |
| Flow | `src/orchestration/bulk_block_deals_flow.py` | Orchestration |
| Runner | `run_bulk_block_deals.py` | CLI tool |
| Tests | `tests/unit/test_bulk_block_deals_parser.py` | Unit tests |
| Verify | `verify_bulk_block_deals.py` | Sample data test |

## Documentation

- **Implementation Guide**: `docs/implementation/bulk-block-deals.md`
- **Verification Queries**: `docs/implementation/bulk-block-deals-verification-queries.md`
- **Schema**: `schemas/parquet/bulk_block_deals.json`
- **ClickHouse DDL**: `warehouse/clickhouse/init/01_create_tables.sql` (lines 478-537)

## Next Steps

1. **Set up scheduled runs**: Configure Prefect deployment for daily execution
2. **Monitor metrics**: Check Prometheus metrics at `http://localhost:9090/metrics`
3. **Validate data**: Run verification queries monthly
4. **Cross-check**: Manually verify random samples against NSE website

## Support

For issues or questions:

1. Check logs in Prefect UI
2. Review error messages in console output
3. Consult implementation guide for data quality notes
4. Verify ClickHouse connection settings

## Performance

- **Scrape**: ~2-5 seconds per deal type per date
- **Parse**: ~100ms for typical daily data (50-100 deals)
- **Parquet write**: ~50ms
- **ClickHouse load**: ~100ms

Can process months of historical data in minutes using date range mode.
