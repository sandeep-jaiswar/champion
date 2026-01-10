# Champion Data Platform: Polars + Parquet + ClickHouse + Prefect + MLflow

## Vision
A modular, schema-first market data platform optimized for batch and streaming ingestion, columnar analytics, and reproducible ML workflows. We leverage:
- Polars: fast, memory-efficient dataframes for parsing/transformations
- Parquet: columnar storage with partitioning for efficient IO
- ClickHouse: OLAP warehouse for low-latency analytical queries
- Prefect: modern orchestration for reliable, observable pipelines
- MLflow: experiment tracking and model registry for ML lifecycle

## High-Level Flow
1. Ingestion (NSE Scraper) → raw CSV files
2. Parsing/Normalization (Polars) → write Parquet to data lake (partitioned)
3. Warehouse Load (ClickHouse) → ingest from Parquet or direct insert
4. Orchestration (Prefect) → schedules, retries, lineage & logs
5. Experimentation (MLflow) → track features, training runs, metrics

## Storage Strategy (Parquet)
- Lake root: `data/lake/`
- Datasets:
  - `raw/ohlc/` schema = NSE raw CSV mapped columns
  - `normalized/ohlc/` schema = typed + renamed columns
  - `features/equity/` derived indicators (e.g., SMA, EMA, RSI)
- Partitioning:
  - Raw OHLC: `date=YYYY-MM-DD/` (optionally `symbol=SYM/` if files are large)
  - Normalized: `year=YYYY/month=MM/day=DD/`
  - Features: `year=YYYY/month=MM/day=DD/feature_group=indicators/`
- File sizing: target 128–256 MB per Parquet file (coalesce small files)
- Metadata: `_common_metadata` and `_metadata` for fast discovery

## Warehouse Strategy (ClickHouse)
- Tables:
  - `raw_equity_ohlc` (minimal typing, mirrors source; MergeTree)
  - `normalized_equity_ohlc` (typed schema; ReplacingMergeTree or MergeTree)
  - `features_equity_indicators` (feature outputs; MergeTree)
- Primary keys & sorting:
  - OHLC: `ORDER BY (symbol, trade_date)`
  - Features: `ORDER BY (symbol, ts)`
- Ingestion options:
  - `clickhouse-client` local file ingest from Parquet
  - HTTP insert (`/query`) with JSONEachRow
  - S3-like storage in future (stage → ingest)

## Orchestration (Prefect)
- Flow graph:
  - `scrape_bhavcopy` → `parse_polars_raw` → `write_parquet_raw`
  - `normalize_polars` → `write_parquet_normalized`
  - `load_clickhouse_parquet`
  - `compute_features_polars` → `write_parquet_features` → `load_clickhouse_features`
- Schedules: daily weekday for OHLC; nightly backfills
- Observability: task-level retries, metrics, logs; MLflow logging from tasks

## Experiment Tracking (MLflow)
- MLflow server (local compose) at `http://localhost:5000`
- Tracking:
  - Parameters: dataset versions, partition ranges, transformations
  - Metrics: row counts, parse duration, load throughput, data quality stats
  - Artifacts: sample Parquet files, notebooks, feature manifests

## Data Contracts & Schemas
- Canonical schemas documented under `schemas/`
- Avro → Parquet mapping table (types, nullability, defaults)
- Schema evolution policy: backward-compatible additions, strict removal rules

## Security & Governance
- Secrets via environment or Prefect blocks (later Vault)
- Access controls for ClickHouse users/roles
- Auditable lineage via Prefect + MLflow metadata

## Acceptance & KPIs
- End-to-end daily flow runs in < 15 min for one trading day
- ClickHouse query latency P95 < 200ms for typical symbol/date ranges
- Parquet write throughput > 100k rows/sec on dev machine
- Reproducible runs with MLflow experiments logged

