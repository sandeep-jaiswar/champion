# CLI Usage

Run common flows and utilities via the unified CLI.

## Install

```bash
poetry install
```

## Commands

- champion etl-index: Run index constituent ETL
- champion etl-macro: Run macro indicators ETL
- champion etl-trading-calendar: Run trading calendar ETL
- champion etl-bulk-deals: Run bulk/block deals ETL
- champion show-config: Print key configuration values

## Examples

```bash
# Index ETL for NIFTY50 (today)
poetry run champion etl-index --index-name NIFTY50

# Index ETL for a specific date
poetry run champion etl-index --index-name NIFTY50 --effective-date 2026-01-11

# Macro indicators
poetry run champion etl-macro

# Trading calendar
poetry run champion etl-trading-calendar

# Bulk deals (range)
poetry run champion etl-bulk-deals --start-date 2025-12-01 --end-date 2025-12-31

# Show configuration
poetry run champion show-config
```

## Notes

- Commands call Prefect `@flow` functions directly.
- Configure via environment variables and `.env` as described in docs.
- Logs, metrics, and data locations are controlled by `champion.config`.
