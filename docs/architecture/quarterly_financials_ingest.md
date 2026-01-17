Quarterly Financials Ingest - Architecture
=======================================

Scope
-----
End-to-end ETL for NSE quarterly financial results: fetch master, filter audited, download XBRL docs, normalize, write canonical Parquet, and load into ClickHouse.

Goals
-----
- Idempotent, testable, and developer-friendly flow
- Canonical lake layout and schema
- Reliable ClickHouse loading with native inserts and partitioning by `symbol`
- Observability for loads and parts activation

High level flow
----------------
1. CLI triggers `QuarterlyResultsScraper.get_master()` to fetch master CSV/JSON.
2. `QuarterlyResultsScraper.normalize_master_dataframe()` coerces columns and normalizes date/datetime fields.
3. Normalized DataFrame is written as Parquet under `data/lake/raw/quarterly_financials/date=YYYY-MM-DD/` with optional `symbol=SYM/` subfolder for single-symbol runs.
4. XBRL documents are downloaded into `data/quarterly_documents/{symbol}/`.
5. `ClickHouseLoader` performs native bulk loads from Parquet; tables partitioned by `symbol`.

Schema & Paths
----------------
- Canonical path: `data/lake/raw/quarterly_financials/date=YYYY-MM-DD/symbol=SYM/quarterly_financials.parquet`
- Master CSV artifacts: `data/master_{YYYYMMDD}.csv` and `data/master_{symbol}_{YYYYMMDD}.csv` for CI
- Documents: `data/quarterly_documents/{symbol}/{symbol}_{idx}_{orig_filename}`

Date handling
-------------
- Normalize `period_start`, `period_end_date`, `filing_date` to Date (ISO), and `exchdisstime`, `broadCastDate` to DateTime (UTC if timezone absent).

ClickHouse
---------
- Use native client for inserts (port 9000). Prefer single-table ReplacingMergeTree(partition by symbol). Avoid multi-statement HTTP posts; migrations applied via single-statement POSTs.

Observability
-------------
- Expose metrics: rows_processed, files_written, load_duration_seconds, parts_active.

Migration
---------
- Store DDL under `warehouse/clickhouse/migrations/` and apply with helper script to ensure single-statement execution and safe swaps (create new table, load, rename/drop).

Developer ergonomics
--------------------
- `make etl-quarterly-local` spins ClickHouse and runs the ETL against local data.
- Documented in README and the tests/ folder with small sample Parquet files.
