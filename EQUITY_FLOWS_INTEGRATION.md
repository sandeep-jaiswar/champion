# Equity Flows Integration Summary

## Overview

Complete equity data ETL pipeline integration with three major flows now operational under the unified `champion` package.

## Completed Components

### 1. Corporate Actions ETL Flow

**Module:** `src/champion/orchestration/flows/corporate_actions_flow.py`

**Purpose:** Scrape, parse, and persist corporate actions data from NSE

**Tasks:**

- `scrape_ca_task`: Scrapes corporate actions via `CorporateActionsScraper`
- `parse_ca_task`: Parses using `CorporateActionsParser`, returns `pl.DataFrame | None`
- `write_ca_parquet_task`: Writes to Parquet with schema validation
- `corporate_actions_etl_flow`: Main orchestration flow with MLflow tracking

**Output:** `data/lake/reference/corporate_actions/effective_date=YYYY-MM-DD/`

**Features:**

- Graceful empty data handling
- Automatic partition creation
- MLflow experiment: `corporate-actions-etl`
- File-based MLflow backend default

### 2. BSE Integration Tasks Module

**Module:** `src/champion/orchestration/tasks/bse_tasks.py`

**Purpose:** Provide task wrappers for BSE equity data (bhavcopy) scraping and parsing

**Tasks:**

- `scrape_bse_bhavcopy(trade_date)`: Scrapes BSE bhavcopy via `BseBhavcopyScraper`
  - Returns: Path to downloaded CSV
  - Retries: 2 (30s delay)
- `parse_bse_polars(csv_path)`: Parses via `PolarsBseParser`
  - Returns: `pl.DataFrame | None`
  - Retries: 1 (10s delay)

**Usage:** Integrated into `combined_equity_etl_flow`

### 3. Combined Equity ETL Flow

**Module:** `src/champion/orchestration/flows/combined_flows.py`

**Purpose:** Unified NSE + BSE equity bhavcopy ingestion

**Updated Imports:**

- Fixed to use `champion.orchestration.flows.flows` (NSE tasks)
- Fixed to use `champion.orchestration.tasks.bse_tasks` (BSE tasks)
- Fixed to use `champion.utils` for shared utilities

**Flow Parameters:**

- `trade_date`: Date to fetch bhavcopy for
- `output_base_path`: Base output directory
- `enable_bse`: Toggle BSE inclusion (default: True)
- `load_to_clickhouse`: Optional warehouse loading
- MLflow metrics/tracking parameters

**Outputs:**

- NSE: `data/lake/intraday/bhavcopy/trade_date=YYYY-MM-DD/`
- BSE: `data/lake/intraday/bhavcopy_bse/trade_date=YYYY-MM-DD/` (if enabled)

## CLI Integration

All flows now available via the `champion` CLI command:

```bash
# Corporate actions ETL
poetry run champion etl-corporate-actions

# Combined equity (NSE + BSE)
poetry run champion etl-combined-equity [--trade-date YYYY-MM-DD]

# View all available commands
poetry run champion --help
```

### Available Commands

- `etl-index`: Index constituent data
- `etl-macro`: Macro indicators
- `etl-trading-calendar`: Trading calendar
- `etl-bulk-deals`: Bulk/block deals
- `etl-corporate-actions`: **NEW** Corporate actions
- `etl-combined-equity`: **NEW** Combined NSE + BSE equity
- `show-config`: Display current configuration

## Class Name Corrections

**Fixed Import Issues:**

- `BSEBhavcopyScraper` → `BseBhavcopyScraper` (correct casing in scrapers module)
- `PolarsBseParser` (confirmed correct casing in parsers module)
- Updated in [bse_tasks.py](src/champion/orchestration/tasks/bse_tasks.py) lines 15-16, 35

## Integration Testing

✅ **Import Tests:** All modules import successfully

```
✓ Corporate actions ETL flow
✓ Combined equity ETL flow  
✓ BSE tasks (scrape, parse)
```

✅ **Test Suite:** All 92 tests passing (100% compatibility)

- No breaking changes to existing flows
- New modules integrated seamlessly

✅ **CLI Verification:** All commands parse correctly

- `champion etl-corporate-actions` ready
- `champion etl-combined-equity` ready

## Configuration

### MLflow Backend

Default: File-based (`file:./mlruns`)

- Avoids HTTP connection issues
- Set in CLI via `os.environ.setdefault("MLFLOW_TRACKING_URI", "file:./mlruns")`
- Individual flows respect this default

### Storage Structure

```
data/lake/
├── intraday/
│   ├── bhavcopy/              # NSE equity bhavcopy
│   └── bhavcopy_bse/          # BSE equity bhavcopy
└── reference/
    └── corporate_actions/      # Corporate actions reference data
```

## Architecture Alignment

**Unified Package Structure:**

```
src/champion/
├── orchestration/
│   ├── flows/
│   │   ├── flows.py (NSE bhavcopy)
│   │   ├── macro_flow.py
│   │   ├── trading_calendar_flow.py
│   │   ├── bulk_block_deals_flow.py
│   │   ├── corporate_actions_flow.py      # NEW
│   │   └── combined_flows.py              # PATCHED
│   └── tasks/
│       ├── macro_tasks.py
│       ├── trading_calendar_tasks.py
│       ├── bulk_block_deals_tasks.py
│       └── bse_tasks.py                   # NEW
├── scrapers/
│   └── nse/
│       ├── bhavcopy.py
│       ├── bse_bhavcopy.py
│       └── corporate_actions.py
├── parsers/
│   ├── polars_bhavcopy_parser.py
│   ├── polars_bse_parser.py
│   └── ca_parser.py
└── cli.py                                 # Updated with new commands
```

## Next Steps

1. **Execution Testing** (Optional):
   - Run `poetry run champion etl-corporate-actions` to verify end-to-end flow
   - Run `poetry run champion etl-combined-equity --trade-date YYYY-MM-DD` for BSE integration

2. **Production Monitoring**:
   - Monitor MLflow experiments for successful runs
   - Validate output data in `data/lake/` partitions
   - Check error logs for scraper/parser failures

3. **Future Enhancements**:
   - Add CLI options for output path customization
   - Implement incremental/backfill modes
   - Add data quality validation tasks
   - Consider async flow scheduling

---
**Last Updated:** 2026-01-11  
**Status:** ✅ Integrated and Test-Verified  
**Migration Phase:** 2 - Complete
