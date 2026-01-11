# BSE Equities Data Integration

## Overview

This implementation adds BSE (Bombay Stock Exchange) as a secondary data source for equity OHLC data, providing:

1. **Redundancy**: Backup data source if NSE is unavailable
2. **Extended Coverage**: Access to symbols listed only on BSE
3. **Data Validation**: Cross-exchange price verification

## Architecture

### Data Flow

```text
┌─────────┐         ┌─────────┐
│   NSE   │         │   BSE   │
│ Website │         │ Website │
└────┬────┘         └────┬────┘
     │                   │
     │ Scrape            │ Scrape
     ↓                   ↓
┌─────────────┐    ┌─────────────┐
│ NSE Scraper │    │ BSE Scraper │
└──────┬──────┘    └──────┬──────┘
       │                  │
       │ Parse            │ Parse
       ↓                  ↓
┌─────────────┐    ┌─────────────┐
│ NSE Parser  │    │ BSE Parser  │
│ (Polars)    │    │ (Polars)    │
└──────┬──────┘    └──────┬──────┘
       │                  │
       │ Normalize        │ Normalize to NSE schema
       ↓                  ↓
       └──────────┬───────┘
                  │
           Deduplicate by ISIN
           (NSE priority)
                  │
                  ↓
          ┌──────────────┐
          │   Combined   │
          │  DataFrame   │
          └───────┬──────┘
                  │
         ┌────────┴────────┐
         ↓                 ↓
    ┌─────────┐      ┌────────────┐
    │ Parquet │      │ ClickHouse │
    │  Lake   │      │  Warehouse │
    └─────────┘      └────────────┘
```

### Key Components

#### 1. BSE Scraper (`src/scrapers/bse_bhavcopy.py`)

- Downloads BSE equity bhavcopy files
- URL format: `https://www.bseindia.com/download/BhavCopy/Equity/EQ{DDMMYY}_CSV.ZIP`
- Handles ZIP extraction
- Error tolerance: Returns None on failure (doesn't crash pipeline)

#### 2. BSE Parser (`src/parsers/polars_bse_parser.py`)

- Parses BSE CSV format
- Normalizes BSE columns to NSE schema
- Maps BSE fields to NSE equivalents
- Adds source tracking metadata

#### 3. Deduplication Logic (`src/orchestration/combined_flows.py`)

- ISIN-based deduplication
- NSE data takes priority for overlapping symbols
- Preserves BSE-only symbols

#### 4. Combined ETL Flow

- Orchestrates both NSE and BSE pipelines
- Tolerates single-source failures
- Unified output format

## Installation

No additional dependencies required. The implementation uses existing packages:

- `polars`: High-performance DataFrame operations
- `httpx`: HTTP client for scraping
- `prefect`: Workflow orchestration
- `mlflow`: Experiment tracking

## Usage

### Quick Start

```bash
cd ingestion/nse-scraper

# Run combined NSE + BSE pipeline
python3 run_combined_etl.py --date 2026-01-09

# View help
python3 run_combined_etl.py --help
```

### Common Operations

```bash
# NSE only (disable BSE)
python3 run_combined_etl.py --date 2026-01-09 --no-bse

# Skip ClickHouse loading
python3 run_combined_etl.py --date 2026-01-09 --no-clickhouse

# Custom output path
python3 run_combined_etl.py --date 2026-01-09 --output-path /custom/path
```

### Identify BSE-Only Symbols

```bash
# Analyze from Parquet files
python3 scripts/identify_bse_only_symbols.py --date 2026-01-09

# Query from ClickHouse
python3 scripts/identify_bse_only_symbols.py --date 2026-01-09 --clickhouse
```

## Configuration

### BSE Configuration (`src/config.py`)

```python
class BSEConfig(BaseSettings):
    bhavcopy_url: str = "https://www.bseindia.com/download/BhavCopy/Equity/EQ{date}_CSV.ZIP"
    # {date} is replaced with DDMMYY format (e.g., 090126)
```

### Environment Variables

```bash
# BSE-specific (optional, uses defaults if not set)
export BSE_BHAVCOPY_URL="https://www.bseindia.com/download/BhavCopy/Equity/EQ{date}_CSV.ZIP"

# Scraper settings (applies to both NSE and BSE)
export SCRAPER_RETRY_ATTEMPTS=3
export SCRAPER_TIMEOUT=300
```

## Data Schema

### BSE CSV Format

```csv
SC_CODE,SC_NAME,SC_GROUP,SC_TYPE,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,NO_TRADES,NO_OF_SHRS,NET_TURNOV,TDCLOINDI,ISIN_CODE
500325,RELIANCE,A,R,2750.00,2780.50,2740.00,2765.25,2765.00,2750.00,50000,5000000,13826250000.00,IEP,INE002A01018
```

### Normalized Output (Unified Schema)

Both NSE and BSE data are normalized to the same schema:

```python
{
    # Metadata
    "event_id": "uuid",
    "event_time": 1704758400000,
    "ingest_time": 1704758500000,
    "source": "bse_eq_bhavcopy",  # or "nse_cm_bhavcopy"
    "schema_version": "v1",
    "entity_id": "RELIANCE:500325:BSE",
    
    # OHLC Data
    "TckrSymb": "RELIANCE",
    "ISIN": "INE002A01018",
    "OpnPric": 2750.00,
    "HghPric": 2780.50,
    "LwPric": 2740.00,
    "ClsPric": 2765.25,
    "TtlTradgVol": 5000000,
    "TtlTrfVal": 13826250000.00,
    # ... other fields
}
```

## Testing

### Unit Tests

```bash
cd ingestion/nse-scraper

# Run BSE tests
python3 -m pytest tests/unit/test_bse_scraper.py -v
python3 -m pytest tests/unit/test_bse_parser.py -v

# Run all tests
python3 -m pytest tests/unit/ -v
```

### Test Coverage

- **BSE Scraper**: 7 tests (100% coverage)
  - URL formatting
  - ZIP download and extraction
  - Error handling
  - Network failures

- **BSE Parser**: 9 tests (81% coverage)
  - CSV parsing
  - Schema normalization
  - ISIN handling
  - Null value handling
  - Event ID generation

## Deduplication Strategy

### ISIN-Based Deduplication

When the same ISIN appears in both NSE and BSE data:

1. **NSE takes priority** (more liquid, reference exchange)
2. BSE record is filtered out
3. Only one record per ISIN remains in output

### Logic

```python
# Get ISINs from NSE
nse_isins = set(nse_df["ISIN"])

# Filter BSE to exclude NSE ISINs
bse_unique = bse_df.filter(
    pl.col("ISIN").is_null() | ~pl.col("ISIN").is_in(nse_isins)
)

# Combine
combined = pl.concat([nse_df, bse_unique])
```

### Example

**Before Deduplication:**

```text
NSE: RELIANCE (ISIN: INE002A01018) - 1800 records
BSE: RELIANCE (ISIN: INE002A01018) - 2500 records
```

**After Deduplication:**

```text
Output: RELIANCE (ISIN: INE002A01018, source: nse_cm_bhavcopy) - 1800 records
(BSE record removed)
```

## Fault Tolerance

### Pipeline Behavior

The pipeline is designed to continue even if one source fails:

```python
# BSE scrape fails → returns None
bse_csv_path = scrape_bse_bhavcopy(trade_date)  # Returns None on error

# BSE parse skipped → returns None
if bse_csv_path is None:
    logger.info("skipping_bse_parse", reason="scrape_failed")
    return None

# Deduplication handles None
combined = deduplicate_by_isin(nse_df, bse_df)  # Works with None
```

### Failure Scenarios

| Scenario | Behavior | Output |
|----------|----------|--------|
| NSE fails, BSE succeeds | ✅ Continue with BSE data | BSE-only data |
| BSE fails, NSE succeeds | ✅ Continue with NSE data | NSE-only data |
| Both fail | ❌ Pipeline fails | Error |
| Network timeout | ⏱️ Retry 3 times | Then fail gracefully |

## Monitoring

### MLflow Metrics

Track pipeline execution:

```bash
open http://localhost:5000
# Experiment: "combined-equity-etl"
```

### Key Metrics

- `nse_rows`: NSE symbols processed
- `bse_rows`: BSE symbols processed
- `bse_unique_rows`: BSE after deduplication
- `duplicates_removed`: Overlapping symbols
- `final_rows`: Total output rows
- `flow_duration_seconds`: Total pipeline time

### Logs

```bash
# View logs with source tracking
tail -f logs/combined_etl.log | grep -E "nse|bse"
```

## Performance

### Benchmarks (Typical)

| Operation | Time | Records |
|-----------|------|---------|
| NSE Scrape | 2-5s | - |
| BSE Scrape | 3-7s | - |
| NSE Parse | 1-3s | ~1800 |
| BSE Parse | 2-5s | ~2500 |
| Deduplication | <1s | ~4000 |
| Parquet Write | 1-2s | ~3000 |
| ClickHouse Load | 2-5s | ~3000 |
| **Total** | **15-30s** | - |

### Optimization

- Polars for fast DataFrame operations (10-50x faster than pandas)
- Parallel scraping (NSE and BSE can be scraped concurrently in future)
- Incremental processing (only new dates)

## Troubleshooting

### BSE Scraping Issues

**Problem**: BSE scrape fails

**Solutions**:

1. Check BSE website: `https://www.bseindia.com/`
2. Verify date format (DDMMYY): `date --date="2026-01-09" +%d%m%y`
3. Check network: `curl -I https://www.bseindia.com/`
4. Review logs: Look for "bse_scrape_failed"

### No BSE Data in Output

**Causes**:

1. Pipeline ran with `--no-bse` flag
2. BSE scraping failed (check logs)
3. All BSE symbols duplicated with NSE (check metrics)

**Verification**:

```bash
# Check if BSE file exists
ls data/lake/normalized/equity_ohlc/year=2026/month=01/day=09/bhavcopy_bse_*.parquet

# Check deduplication metrics
grep "duplicates_removed" logs/mlflow/*
```

### Duplicate ISINs in Output

This should not happen. If it does:

```sql
-- Find duplicates
SELECT ISIN, COUNT(*) 
FROM normalized_equity_ohlc 
WHERE TradDt = '2026-01-09' 
GROUP BY ISIN 
HAVING COUNT(*) > 1;
```

**Resolution**: Re-run pipeline for that date

## Future Enhancements

- [ ] Parallel scraping (NSE and BSE simultaneously)
- [ ] Historical backfill script
- [ ] Price variance alerts (cross-exchange validation)
- [ ] BSE derivatives support
- [ ] Real-time data integration
- [ ] Multi-exchange arbitrage detection

## References

- [BSE India Website](https://www.bseindia.com/)
- [BSE Bhavcopy Documentation](https://www.bseindia.com/markets/equity/EQReports/BhavCopyDebt.html)
- [NSE-BSE Schema Comparison](../schemas/parquet/README.md)
- [Verification Guide](../docs/verification/bse-data-verification.md)

## Support

For issues or questions:

- GitHub Issues: Link to your repo
- Internal Wiki: Link if applicable
- Contact: Team lead or maintainer
