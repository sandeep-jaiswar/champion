# Stock Market Intelligence Platform

## Overview

This repository contains the foundations of a **production-grade, event-driven stock market analysis platform**, inspired by large-scale systems at companies like Uber.

The platform is designed to:

- Ingest raw exchange data (starting with NSE)
- Preserve immutable source truth
- Support real-time and batch analytics
- Enable financial correctness at scale
- Power research, modeling, and trading intelligence

This is **not** a monolithic application. It is a **polyglot data platform** built around **schemas, events, and contracts**.

---

## Core Principles (Read This First)

### 1. Schema-first, code-second

Schemas are **APIs**. All systems (Kafka, Hudi, ClickHouse, Spark, Flink, services) derive from schemas.

- Breaking schema changes are forbidden
- Evolution happens via versioning
- Code must conform to schemas, never the reverse

### 2. Raw data is sacred

Raw market data:

- Is immutable
- Is replayable
- Mirrors the exchange exactly
- Is never enriched or corrected

Any transformation, normalization, or adjustment happens **downstream**.

### 3. Event-driven by default

The system is built around:

- Kafka as the event backbone
- Explicit topic ownership
- Deterministic replay
- Idempotent consumers

No service talks to another service directly for market data.

### 4. Financial correctness > convenience

This platform optimizes for:

- Auditability
- Reproducibility
- Traceability
- Correctness under reprocessing

Low latency is important — but never at the cost of correctness.

---

## High-level Architecture

```text
[ Exchange / NSE ]
      |
      v
[ Ingestion Services ]
      |
      v
[ Kafka (Raw Topics) ]
      |
      +--> [ Hudi Bronze (Immutable) ]
      |
      +--> [ Normalization Pipelines ]
      |
      v
[ Hudi Silver / Gold ]
      |
      +--> ClickHouse
      +--> Analytics / Models
```

---

## Repository Structure

```text
├── README.md
├── docs/
│   ├── architecture/   # System design & contracts
│   ├── decisions/      # Technology Decision Records (TDRs)
│   └── issues/         # Canonical issue definitions
├── schemas/            # Avro schemas (source of truth)
└── .github/
    └── ISSUE_TEMPLATE/ # Enforced issue discipline
```

---

## Domain Boundaries (Important)

This repository enforces **strict domain separation**:

- **Ingestion** — fetches and emits raw exchange events
- **Market Data (Raw)** — immutable, replayable event streams
- **Normalization** — symbol mapping, corporate actions, alignment
- **Storage** — Hudi (lakehouse) and ClickHouse (serving)
- **Analytics & Intelligence** — indicators, signals, models (out of scope initially)

Cross-domain shortcuts are explicitly forbidden.

---

## What This Repo Is NOT

- ❌ A trading bot
- ❌ A UI/dashboard project
- ❌ A one-off data scraper
- ❌ A monolithic application

Those may exist later — **outside** this core platform.

---

## Development Philosophy

- Architecture before implementation
- Contracts before code
- Small, composable services
- Clear ownership and boundaries
- Tickets as executable intent

If something is unclear, it belongs in `docs/architecture/`, not in code comments.

---

## Contribution Rules (Non-Negotiable)

- No feature work without an architecture ticket
- No schema changes without versioning
- No enrichment in raw domains
- No hidden coupling between services
- No “temporary” hacks

When in doubt:

> **Preserve data. Defer decisions. Document intent.**

---

## Development Workflow

### Code Quality Checks

Before submitting a pull request, ensure all quality checks pass:

#### Markdown Linting

All markdown files must pass linting checks:

```bash
# Check markdown files
make lint-md

# Auto-fix markdown issues
make lint-md-fix
```

Markdown linting is automatically enforced in CI/CD via GitHub Actions. The configuration is in `.markdownlint.json`.

#### Pre-commit Hooks (Recommended)

Install pre-commit hooks to catch issues before committing:

```bash
pip install pre-commit
pre-commit install
```

This will automatically run markdown linting and other checks on staged files.

---

## Status

✅ **Foundation Complete - Production Ready**

Core components implemented and operational:

- ✅ NSE Data Ingestion (Bhavcopy, Corporate Actions, Symbol Master, Trading Calendar, **Index Constituents**)
- ✅ Polars-based ETL Pipeline (High-performance parsing & normalization)
- ✅ Prefect Orchestration (Flow scheduling & task management)
- ✅ Parquet Data Lake (Partitioned storage with retention policies)
- ✅ ClickHouse Data Warehouse (Fast analytical queries)
- ✅ MLflow Experiment Tracking (Pipeline metadata & metrics)
- ✅ Prometheus Metrics (Real-time observability)
- ✅ Feature Store (Technical indicators: SMA, EMA, RSI)
- ✅ **Index Membership Tracking (NIFTY50, BANKNIFTY, rebalance history)**
- ✅ Trading Calendar Validation (Holiday checking, date calculations)

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Poetry (Python dependency manager)

### 1. Start Infrastructure Services

```bash
# Start ClickHouse and MLflow
docker-compose up -d

# Verify services are running
docker-compose ps

# Check health
curl http://localhost:5000/health  # MLflow
clickhouse-client --query "SELECT 1"  # ClickHouse
```

**Services:**

- **MLflow UI**: <http://localhost:5000>
- **ClickHouse HTTP**: <http://localhost:8123>
- **ClickHouse Native**: `localhost:9000`

### 2. Install Dependencies

```bash
cd ingestion/nse-scraper
poetry install
```

### 3. Run the ETL Pipeline

```bash
cd ingestion/nse-scraper

# Run complete ETL pipeline (scrape → parse → normalize → load)
poetry run python run_etl.py

# Or run individual components
poetry run python run_scraper.py  # Just scraping
poetry run python tests/manual/test_flow_manual.py  # Test workflow
```

### 4. Access Outputs

**Data Lake:**

```bash
# View partitioned data
ls -lR data/lake/

# Raw data
data/lake/raw/equity_ohlc/year=2026/month=01/day=09/

# Normalized data
data/lake/normalized/equity_ohlc/year=2026/month=01/day=09/

# Features
data/lake/features/equity/
```

**ClickHouse Queries:**

```bash
clickhouse-client --database champion_market

# Count records
SELECT COUNT(*) FROM normalized_equity_ohlc;

# View recent data
SELECT * FROM normalized_equity_ohlc 
ORDER BY trade_date DESC 
LIMIT 10;

# Daily summary
SELECT 
    trade_date,
    COUNT(*) as symbols,
    SUM(total_traded_quantity) as volume
FROM normalized_equity_ohlc
GROUP BY trade_date
ORDER BY trade_date DESC;

# Trading calendar queries
SELECT COUNT(*) FROM trading_calendar WHERE year = 2026;

# Count trading days per month
SELECT 
    month,
    SUM(CASE WHEN is_trading_day THEN 1 ELSE 0 END) as trading_days
FROM trading_calendar
WHERE year = 2026
GROUP BY month
ORDER BY month;

# List holidays
SELECT trade_date, holiday_name 
FROM trading_calendar
WHERE day_type = 'MARKET_HOLIDAY' AND year = 2026
ORDER BY trade_date;
```

**MLflow Experiments:**

```bash
# Open browser to MLflow UI
open http://localhost:5000

# View experiments, runs, metrics, and artifacts
```

**Prometheus Metrics:**

```bash
# View metrics
curl http://localhost:9090/metrics

# Example metrics:
# - nse_scrape_success_total
# - nse_parse_rows_total
# - nse_pipeline_flow_duration_seconds
```

### 5. Run Index Constituent ETL (NEW)

**Scrape NSE index membership data:**

```bash
# Scrape NIFTY50 and BANKNIFTY constituents
python run_index_etl.py

# Scrape specific indices
python run_index_etl.py --indices NIFTY50 BANKNIFTY NIFTYIT

# Scrape for specific date
python run_index_etl.py --date 2026-01-11

# View results in ClickHouse
clickhouse-client --database champion_market --query "
SELECT 
    index_name,
    symbol,
    company_name,
    weight,
    sector
FROM index_constituent
WHERE action = 'ADD'
  AND index_name = 'NIFTY50'
ORDER BY weight DESC NULLS LAST
LIMIT 10
"
```

---

## System Architecture & Communication

### Component Communication Flow

```text
┌──────────────────────────────────────────────────────────────┐
│                    CHAMPION DATA PLATFORM                     │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌────────────┐                                              │
│  │    NSE     │  HTTPS (TLS 1.2+)                           │
│  │  Exchange  │─────────────────────┐                        │
│  └────────────┘                     │                        │
│                                     ↓                         │
│                            ┌──────────────┐                  │
│                            │   Scrapers   │                  │
│                            │ (Python/HTTP)│                  │
│                            └───────┬──────┘                  │
│                                    │ CSV Files                │
│                                    ↓                          │
│                           ┌──────────────┐                   │
│                           │    Polars    │                   │
│                           │   Parsers    │                   │
│                           └───────┬──────┘                   │
│                                   │ DataFrames               │
│                                   ↓                           │
│  ┌─────────────────────────────────────────────┐            │
│  │          PREFECT ORCHESTRATION               │            │
│  │  ┌──────────┐   ┌─────────┐  ┌───────────┐ │            │
│  │  │  Scrape  │──▶│  Parse  │─▶│ Normalize │ │            │
│  │  │   Task   │   │  Task   │  │   Task    │ │            │
│  │  └──────────┘   └─────────┘  └─────┬─────┘ │            │
│  │                                      │       │            │
│  │                            ┌─────────▼──────┐│            │
│  │                            │ Write Parquet  ││            │
│  │                            └─────────┬──────┘│            │
│  │                                      │       │            │
│  │                            ┌─────────▼──────┐│            │
│  │                            │ Load ClickHouse││            │
│  │                            └────────────────┘│            │
│  └─────────────────────────────────────────────┘            │
│                      │           │                            │
│              Metrics │           │ Run Data                   │
│                      ↓           ↓                            │
│         ┌────────────────┐  ┌──────────┐                    │
│         │  PROMETHEUS    │  │  MLFLOW  │                    │
│         │   (Port 9090)  │  │(Port 5000)│                   │
│         └────────────────┘  └──────────┘                    │
│                                                               │
│                      DATA LAYER                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                  PARQUET DATA LAKE                    │   │
│  │  ┌──────────┐   ┌──────────────┐   ┌──────────────┐ │   │
│  │  │   Raw    │──▶│  Normalized  │──▶│   Features   │ │   │
│  │  │  Bronze  │   │    Silver    │   │     Gold     │ │   │
│  │  └──────────┘   └──────────────┘   └──────────────┘ │   │
│  └──────────────────────┬───────────────────────────────┘   │
│                         │ Parquet Files                      │
│                         ↓                                     │
│              ┌────────────────────┐                          │
│              │    CLICKHOUSE      │                          │
│              │  Data Warehouse    │                          │
│              │  (Ports 8123/9000) │                          │
│              └────────────────────┘                          │
│                         │                                     │
│                         ↓                                     │
│              ┌────────────────────┐                          │
│              │  Analytics & ML    │                          │
│              │  Feature Store     │                          │
│              └────────────────────┘                          │
└──────────────────────────────────────────────────────────────┘
```

### Component Details

#### 1. **NSE Scrapers** (`ingestion/nse-scraper/src/scrapers/`)

- **Purpose**: Fetch raw data from NSE website
- **Communication**: HTTPS to NSE servers
- **Outputs**: CSV files (raw format)
- **Components**:
  - `BhavcopyScraper`: Daily equity OHLC data
  - `CorporateActionsScraper`: Dividends, splits, bonuses
  - `SymbolMasterScraper`: Symbol metadata
  - `TradingCalendarScraper`: Market holidays

#### 2. **Polars Parsers** (`ingestion/nse-scraper/src/parsers/`)

- **Purpose**: High-performance data parsing and transformation
- **Input**: CSV files from scrapers
- **Output**: Polars DataFrames (in-memory)
- **Key Features**:
  - Lazy evaluation for memory efficiency
  - Type validation
  - Schema enforcement
  - Significantly faster than traditional DataFrame libraries

#### 3. **Prefect Orchestration** (`ingestion/nse-scraper/src/orchestration/`)

- **Purpose**: Workflow management and task scheduling
- **Communication**: Direct Python function calls (in-process)
- **Features**:
  - Task dependencies (`scrape → parse → normalize → write → load`)
  - Retry logic (3 retries with exponential backoff)
  - Error handling
  - Parallel task execution (when possible)
  - Caching (1 hour TTL for scrape tasks)

#### 4. **Data Lake** (`data/lake/`)

- **Purpose**: Immutable, partitioned data storage
- **Format**: Apache Parquet (columnar)
- **Partitioning**: `year=YYYY/month=MM/day=DD/`
- **Layers**:
  - **Bronze (Raw)**: Exact copy from exchange
  - **Silver (Normalized)**: Standardized schema, cleaned data
  - **Gold (Features)**: Technical indicators, aggregations

#### 5. **ClickHouse Warehouse** (`warehouse/clickhouse/`)

- **Purpose**: Fast analytical queries on normalized data
- **Communication**: Native protocol (port 9000) or HTTP (port 8123)
- **Schema**: Matches normalized Parquet schema
- **Features**:
  - MergeTree engine (optimized for analytics)
  - Partitioned by trade_date
  - Ordered by (symbol, trade_date)
  - Compression enabled

#### 6. **MLflow Tracking** (Docker container)

- **Purpose**: Experiment tracking and metadata management
- **Communication**: HTTP REST API (port 5000)
- **Tracks**:
  - Flow parameters (trade_date, config)
  - Metrics (duration, rows processed, errors)
  - Artifacts (logs, sample data)
  - Run history and comparisons

#### 7. **Prometheus Metrics** (`src/utils/metrics.py`)

- **Purpose**: Real-time operational metrics
- **Communication**: HTTP /metrics endpoint (port 9090)
- **Exposes**:
  - Scrape success/failure counters
  - Parse row counts
  - Pipeline duration histograms
  - ClickHouse load metrics

#### 8. **Feature Store** (`src/features/`)

- **Purpose**: Pre-computed technical indicators
- **Input**: Normalized OHLC data from Parquet
- **Output**: Feature Parquet files
- **Indicators**:
  - Simple Moving Average (SMA): 5, 20, 50, 200-day
  - Exponential Moving Average (EMA): 12, 26-day
  - Relative Strength Index (RSI): 14-day

### Data Flow Example

```text
1. SCRAPE: NSE → CSV (raw/BhavCopy_NSE_CM_20260109.csv)
   └─> Metrics: nse_scrape_success_total++

2. PARSE: CSV → Polars DataFrame (in-memory)
   └─> Validation: Schema, types, nulls
   └─> Metrics: nse_parse_rows_total += N

3. NORMALIZE: DataFrame → Standardized DataFrame
   └─> Add: metadata columns (ingestion_timestamp, source_file)
   └─> Convert: types, date formats
   └─> Clean: remove test symbols, invalid data

4. WRITE PARQUET: DataFrame → data/lake/normalized/equity_ohlc/
   └─> Partition: year=2026/month=01/day=09/
   └─> Compression: Snappy
   └─> Metrics: parquet_write_success++

5. LOAD CLICKHOUSE: Parquet → ClickHouse table
   └─> Connection: localhost:9000
   └─> Table: normalized_equity_ohlc
   └─> Metrics: clickhouse_load_success++

6. TRACK: All metadata → MLflow
   └─> Experiment: nse-bhavcopy-etl
   └─> Run: bhavcopy-etl-2026-01-09
   └─> Params: {trade_date, load_to_clickhouse, ...}
   └─> Metrics: {duration_seconds, rows_processed, ...}
```

---

## Directory Structure

```text
champion/
├── README.md                          # This file
├── docker-compose.yml                 # Infrastructure services
├── Makefile                          # Common commands
├── data/
│   └── lake/                         # Parquet data lake
│       ├── raw/                      # Bronze: Immutable source
│       ├── normalized/               # Silver: Clean, standardized
│       └── features/                 # Gold: Indicators, aggregations
├── docs/
│   └── architecture/                 # System design docs
├── ingestion/
│   └── nse-scraper/                  # NSE data pipeline
│       ├── pyproject.toml           # Dependencies
│       ├── run_etl.py               # Main ETL runner
│       ├── run_scraper.py           # Standalone scraper
│       ├── src/
│       │   ├── config.py            # Configuration
│       │   ├── scrapers/            # Data fetching
│       │   ├── parsers/             # Polars parsing
│       │   ├── orchestration/       # Prefect flows
│       │   ├── models/              # Schema definitions
│       │   ├── utils/               # Logger, metrics
│       │   └── tasks/               # Individual tasks
│       └── tests/
│           ├── unit/                # Unit tests
│           ├── integration/         # Integration tests
│           └── manual/              # Manual test scripts
├── schemas/                          # Avro schemas (contracts)
│   ├── market-data/
│   └── parquet/
├── src/
│   ├── features/                     # Feature engineering
│   │   ├── indicators.py            # Technical indicators
│   │   └── demo_features.py         # Demo script
│   ├── ml/                          # ML tracking utilities
│   │   └── tracking.py
│   └── storage/                     # Parquet I/O
│       ├── parquet_io.py
│       └── retention.py
└── warehouse/
    └── clickhouse/
        ├── init/                     # DDL scripts
        └── users.xml                # User configuration
```

---

## Development Commands

### Ingestion Pipeline

```bash
cd ingestion/nse-scraper

# Run full ETL
poetry run python run_etl.py

# Run only scraper
poetry run python run_scraper.py

# Run trading calendar ETL
poetry run python run_trading_calendar.py

# Test individual components
poetry run python tests/manual/test_flow_manual.py

# Run unit tests
poetry run pytest tests/unit/

# Run integration tests
poetry run pytest tests/integration/
```

### Feature Engineering

```bash
cd src/features

# Compute features for normalized data
poetry run python demo_features.py

# Run with custom parameters
poetry run python -c "
from src.features.indicators import compute_features
import polars as pl
df = pl.read_parquet('data/lake/normalized/equity_ohlc/**/*.parquet')
features = compute_features(df, windows=[5, 20], compute_rsi=True)
features.write_parquet('data/lake/features/equity/features.parquet')
"
```

### Data Warehouse

```bash
# Load batch data
cd warehouse
poetry run python -m loader.batch_loader

# Generate sample data for testing
poetry run python -m loader.generate_sample_data
```

### Monitoring

```bash
# View Prometheus metrics
curl http://localhost:9090/metrics | grep nse_

# View MLflow experiments
open http://localhost:5000

# Check ClickHouse data
clickhouse-client --database champion_market --query "
SELECT 
    toDate(trade_date) as date,
    COUNT(*) as symbols,
    SUM(total_traded_quantity) as volume,
    AVG(close) as avg_price
FROM normalized_equity_ohlc
GROUP BY date
ORDER BY date DESC
LIMIT 7
"
```

### Code Quality

```bash
# Linting
poetry run ruff check src/ tests/

# Type checking
poetry run mypy src/

# Formatting
poetry run black src/ tests/

# Markdown linting
make lint-md
```

---

## Configuration

### Environment Variables

```bash
# MLflow
export MLFLOW_TRACKING_URI=http://localhost:5000

# ClickHouse
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=9000
export CLICKHOUSE_USER=champion_user
export CLICKHOUSE_PASSWORD=champion_pass
export CLICKHOUSE_DATABASE=champion_market

# Pipeline
export SCRAPE_DATE=2026-01-09  # Optional, defaults to yesterday
export DRY_RUN=false
```

### Configuration File

Edit `ingestion/nse-scraper/src/config.py` for:

- Data lake paths
- Retention policies
- Scraper settings
- Observability configuration

---

## Troubleshooting

### Docker Services Not Starting

```bash
# Check ports
sudo lsof -i :5000   # MLflow
sudo lsof -i :8123   # ClickHouse

# Stop conflicting services
sudo systemctl stop clickhouse-server

# Restart Docker services
docker-compose down
docker-compose up -d
```

### Pipeline Failures

```bash
# Check logs
docker-compose logs mlflow
docker-compose logs clickhouse

# View metrics for errors
curl http://localhost:9090/metrics | grep failed

# Check MLflow for run details
open http://localhost:5000
```

### Data Quality Issues

```bash
# Validate Parquet files
poetry run python -c "
import polars as pl
df = pl.read_parquet('data/lake/normalized/equity_ohlc/**/*.parquet')
print(df.describe())
print(df.null_count())
"

# Check ClickHouse data
clickhouse-client --query "
SELECT 
    COUNT(*) as total,
    COUNT(DISTINCT symbol) as symbols,
    MIN(trade_date) as first_date,
    MAX(trade_date) as last_date
FROM champion_market.normalized_equity_ohlc
"
```

---
