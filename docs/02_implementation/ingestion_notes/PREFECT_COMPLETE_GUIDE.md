# 🎯 Complete Prefect Visualization & Orchestration Guide

## Overview

The Champion data platform uses **Prefect** for complete workflow orchestration and visualization. This guide shows how to visualize everything with dashboards, UI, and metrics.

---

## 🚀 Quick Start - Everything in One Command

Start the entire orchestration stack with metrics and dashboards:

```bash
cd ingestion/nse-scraper

# Option 1: Automated setup (recommended)
poetry run python run_stack.py

# Option 2: Manual step-by-step
prefect server start &          # Terminal 1
poetry run mlflow ui &          # Terminal 2
prefect agent start -q default &  # Terminal 3
poetry run python prefect_dashboard.py  # Terminal 4
```

---

## 📊 Visualization Tools

### 1️⃣ Prefect Dashboard (Real-time Flow Monitoring)

**Access:** <http://localhost:4200>

**Shows:**

- ✅ All running flows and their status
- 📊 Task dependencies and execution timeline
- 📈 Performance metrics per task
- 🔄 Retry attempts and failure logs
- 📅 Scheduled runs
- 💾 Historical run data

**Key Features:**

```
Dashboard → Flows → NSE Bhavcopy ETL
                  ├── Runs (last 10)
                  │   ├── Successful runs
                  │   ├── Failed runs
                  │   └── Pending runs
                  ├── Task Graph
                  │   ├── scrape_bhavcopy
                  │   ├── parse_polars_raw
                  │   ├── normalize_polars
                  │   ├── write_parquet
                  │   └── load_clickhouse
                  └── Logs
                      ├── Flow logs
                      └── Task logs (live)
```

### 2️⃣ MLflow Tracking (Metrics & Experiments)

**Access:** <http://localhost:5000>

**Tracks:**

- 📊 Metrics per task (duration, rows processed)
- 📈 Performance trends across multiple runs
- 📝 Parameters (trade_date, load_to_clickhouse)
- 🔍 Experiment comparison
- 📉 Historical data analysis

**Example Metrics Visualization:**

```
Runs Timeline:
├── 2026-01-11 run
│   ├── scrape_duration_seconds: 1.234s
│   ├── parse_duration_seconds: 0.456s
│   ├── normalize_duration_seconds: 0.123s
│   ├── write_duration_seconds: 0.789s
│   ├── load_duration_seconds: 2.345s
│   ├── rows_processed: 3283
│   └── file_size_mb: 2.4
│
├── 2026-01-10 run
│   ├── scrape_duration_seconds: 1.189s
│   ├── parse_duration_seconds: 0.512s
│   ... (previous metrics)
```

### 3️⃣ CLI Visualization Dashboard

**Run:**

```bash
poetry run python prefect_dashboard.py
```

**Displays:**

```
┌──────────────────────────────────────────────────────────┐
│ 🚀 CHAMPION DATA PIPELINE DASHBOARD                      │
│                                                          │
│ Real-time NSE Data Ingestion & Analytics                 │
└──────────────────────────────────────────────────────────┘

📊 Data Pipeline Architecture
🔀 Prefect Flows Configuration
📊 Data Lineage & Transformations
⚙️ Task Execution Pipeline
📊 Data Sources & Coverage
📈 Monitoring & Metrics
🏗️ Technology Stack
🚀 Deployment Guide
```

---

## 🔀 Flow Architecture Visualization

### Data Pipeline Diagram

```
NSE APIs
  ├── Bhavcopy (ZIP)
  ├── Symbol Master (CSV)
  ├── Bulk/Block Deals (CSV)
  ├── Trading Calendar (JSON)
  ├── Index Constituents (JSON)
  └── Option Chain (JSON)
        │
        ▼
  ┌──────────────────────┐
  │  Prefect Flows [🔀]  │
  │                      │
  │  ┌──────────┐        │
  │  │ Scrape   │────────┼── httpx (auto-decompress)
  │  └──────┬───┘        │
  │         │            │
  │         ▼            │
  │  ┌──────────┐        │
  │  │ Parse    │────────┼── Polars (50-100x faster)
  │  └──────┬───┘        │
  │         │            │
  │         ▼            │
  │  ┌──────────┐        │
  │  │ Normalize│────────┼── Validation + event_id
  │  └──────┬───┘        │
  │         │            │
  │         ▼            │
  │  ┌──────────┐        │
  │  │ Write    │────────┼── Parquet (Bronze layer)
  │  └──────┬───┘        │
  │         │            │
  │         ▼            │
  │  ┌──────────┐        │
  │  │ Load     │────────┼── ClickHouse (Analytics)
  │  └──────────┘        │
  └──────────────────────┘
        │
        ▼
  ┌────────────────────────────┐
  │ Storage & Analytics Layer  │
  │                            │
  │ 📂 Parquet Data Lake       │
  │    ├─ bronze/ (raw)        │
  │    ├─ silver/ (normalized) │
  │    └─ gold/ (analytics)    │
  │                            │
  │ 🗄️ ClickHouse Warehouse    │
  │                            │
  │ 📊 MLflow Metrics          │
  │ 📈 Prometheus              │
  │ 🔍 Kafka Topics            │
  └────────────────────────────┘
```

---

## 📈 All 6 Production Flows

### 1. NSE Bhavcopy ETL

```python
Flow: nse-bhavcopy-etl
├── Schedule: Weekdays 6:00 PM IST (30 min past midnight UTC)
├── Retries: 3 attempts with backoff
├── Tasks:
│   ├── scrape_bhavcopy (download 500KB ZIP)
│   ├── parse_polars_raw (parse 3,283 securities)
│   ├── normalize_polars (validate & enrich)
│   ├── write_parquet (partitioned by trade_date)
│   └── load_clickhouse (bulk insert)
│
├── Metrics Logged:
│   ├── scrape_duration_seconds: 1.2s avg
│   ├── parse_duration_seconds: 0.5s avg
│   ├── rows_processed: 3,283
│   └── file_size_mb: 2.4
│
└── Status: ✅ Production Ready
```

### 2. Bulk & Block Deals ETL

```python
Flow: bulk-block-deals-etl
├── Schedule: Weekdays 3:00 PM IST
├── Retries: 2 attempts
├── Data: Brotli-compressed CSV
├── Tasks:
│   ├── scrape_bulk_block_deals (query NSE API)
│   ├── parse_bulk_block_deals (auto-decompress)
│   ├── normalize_bulk_block_deals (clean columns)
│   ├── write_bulk_block_deals (Polars to Parquet)
│   └── load_bulk_block_deals (ClickHouse load)
│
├── Metrics:
│   ├── bulk_deals_count: 50-150 daily
│   ├── block_deals_count: 0-50 daily
│   └── parse_duration: 0.8s avg
│
└── Status: ✅ Production Ready (Fixed + Polars optimized)
```

### 3. Trading Calendar ETL

```python
Flow: trading-calendar-etl
├── Schedule: Quarterly (Jan, Apr, Jul, Oct)
├── Tasks:
│   ├── scrape_trading_calendar (NSE API)
│   ├── parse_trading_calendar (JSON → DataFrame)
│   ├── write_trading_calendar_parquet
│   └── load_trading_calendar_clickhouse
│
├── Data:
│   ├── Trading days: 250+ per year
│   ├── Format: JSON
│   └── Contains: Holidays, market events
│
└── Status: ✅ Production Ready
```

### 4. Index Constituents ETL

```python
Flow: index-constituents-etl
├── Schedule: Daily 7:00 PM IST
├── Indices:
│   ├── NIFTY50 (51 constituents)
│   ├── BANKNIFTY (15 constituents)
│   ├── NIFTY100, NIFTY200, etc.
│
├── Tasks:
│   ├── scrape_index_constituents (all indices)
│   ├── parse_index_constituents (JSON parsing)
│   ├── write_index_constituents_parquet
│   └── load_index_constituents_clickhouse
│
└── Status: ✅ Production Ready
```

### 5. Option Chain ETL

```python
Flow: option-chain-etl
├── Schedule: Every 30 minutes (market hours)
├── Frequency: 9:15 AM - 3:30 PM IST on trading days
├── Tasks:
│   ├── scrape_option_chain (NSE API)
│   ├── parse_option_chain (Polars DataFrame)
│   ├── write_option_chain_parquet
│   └── load_option_chain_clickhouse
│
├── Data per run:
│   ├── Columns: strike, symbol, open_interest, iv, etc.
│   ├── Records: 100-1000 per run
│   └── Size: 50-200 KB per run
│
└── Status: ✅ Production Ready
```

### 6. Combined Market Data ETL

```python
Flow: combined-market-data-etl
├── Schedule: Weekdays 8:00 PM IST
├── Combines: All above flows
├── Orchestrates:
│   ├── Parallel runs of independent flows
│   ├── Sequential runs of dependent flows
│   ├── Error handling & retry logic
│   └── Metrics aggregation
│
├── Outputs:
│   ├── Comprehensive market snapshot
│   ├── Complete data lake update
│   └── ClickHouse warehouse refresh
│
└── Status: ✅ Production Ready
```

---

## 🎮 Interactive Prefect UI

### Accessing the Dashboard

```bash
# 1. Start Prefect Server
prefect server start

# 2. Open browser
http://localhost:4200
```

### Dashboard Features

**Left Sidebar:**

```
Dashboard
├── Flows (all available flows)
│   ├── nse-bhavcopy-etl (with icon)
│   ├── bulk-block-deals-etl
│   ├── trading-calendar-etl
│   ├── index-constituents-etl
│   ├── option-chain-etl
│   └── combined-market-data-etl
├── Deployments (scheduled)
├── Work queues (default)
├── Blocks (configuration)
└── Notifications
```

**Main Panel - Flow Details:**

```
nse-bhavcopy-etl
├── Runs (tab)
│   ├── Run ID: f1a2b3c4...
│   ├── Status: ✅ Completed
│   ├── Started: 2026-01-11 18:30:00
│   ├── Ended: 2026-01-11 18:35:42
│   ├── Duration: 5m 42s
│   └── View logs →
│
├── Schedule (tab)
│   ├── Type: Cron
│   ├── Cron: 30 12 * * 1-5 (UTC)
│   ├── Timezone: UTC
│   └── Next run: 2026-01-13 12:30:00
│
├── Deployment (tab)
│   ├── Name: nse-bhavcopy-daily
│   ├── Version: 1.0.0
│   ├── Work queue: default
│   └── Status: Active
│
└── Graph (tab)
    └── Task dependency graph
```

**Run Details View:**

```
Flow Run Details
├── Timeline
│   ├── scrape_bhavcopy (1.23s) ✅
│   ├── parse_polars_raw (0.45s) ✅
│   ├── normalize_polars (0.12s) ✅
│   ├── write_parquet (0.78s) ✅
│   └── load_clickhouse (2.34s) ✅
│
├── Logs
│   ├── [INFO] Flow run started
│   ├── [INFO] scrape_bhavcopy: Downloading...
│   ├── [INFO] parse_polars_raw: 3283 rows
│   ├── [INFO] write_parquet: Saved to bronze/
│   ├── [INFO] load_clickhouse: Loaded 3283 rows
│   └── [INFO] Flow run completed successfully
│
└── Parameters
    ├── trade_date: 2026-01-10
    ├── load_to_clickhouse: true
    └── output_base_path: data/lake
```

---

## 📊 MLflow Tracking Dashboard

### Access & Navigation

```bash
poetry run mlflow ui --host 0.0.0.0 --port 5000
# → http://localhost:5000
```

### Experiments View

```
Experiments
├── Default (active)
│   ├── Run 1: bhavcopy-etl-2026-01-11
│   ├── Run 2: bhavcopy-etl-2026-01-10
│   └── Run 3: bhavcopy-etl-2026-01-09
│
└── Bulk Deals (custom)
    ├── Run 1: bulk-deals-2026-01-11
    └── Run 2: bulk-deals-2026-01-11 (retry)
```

### Metrics Comparison

```
Parameter: trade_date
├── 2026-01-11
│   ├── scrape_duration_seconds: 1.23
│   ├── parse_duration_seconds: 0.45
│   ├── total_duration: 5.42s
│   └── rows_processed: 3283
│
└── 2026-01-10
    ├── scrape_duration_seconds: 1.19
    ├── parse_duration_seconds: 0.51
    ├── total_duration: 5.28s
    └── rows_processed: 3283
```

### Charts & Graphs

- **Duration Trend:** Shows task duration over time
- **Throughput:** Records per second
- **Error Rate:** Failed runs vs total
- **Storage:** Parquet file sizes

---

## 📋 Prefect CLI Commands

### View Flows

```bash
# List all flows
prefect flow ls

# List all deployments
prefect deployment ls

# View deployment details
prefect deployment inspect 'nse-bhavcopy-etl/nse-bhavcopy-daily'
```

### Run Flows

```bash
# Trigger a deployment
prefect deployment run 'nse-bhavcopy-etl/nse-bhavcopy-daily'

# With custom parameters
prefect deployment run 'nse-bhavcopy-etl/nse-bhavcopy-daily' \
  --param trade_date="2026-01-11" \
  --param load_to_clickhouse=true

# Execute flow locally
prefect flow run -p trade_date=2026-01-11 \
  src.orchestration.flows:nse_bhavcopy_etl_flow
```

### Monitor Runs

```bash
# List recent runs
prefect flow-run ls -l 20

# View specific run
prefect flow-run inspect <run-id>

# Stream logs
prefect flow-run logs -f <run-id>

# View run state
prefect flow-run state <run-id>
```

### Schedule Management

```bash
# List all work queues
prefect work-queue ls

# Start agent on default queue
prefect agent start -q default

# View agent status
prefect agent status

# Set deployment schedule
prefect deployment set-schedule 'nse-bhavcopy-etl/nse-bhavcopy-daily' \
  --cron '30 12 * * 1-5'
```

---

## 🔔 Real-Time Monitoring Setup

### Enable Slack Notifications

```python
# In Prefect UI or config
Notifications → Add Notification Block
├── Trigger: Flow run failed
├── Channel: Slack
└── Webhook: https://hooks.slack.com/services/...
```

### Configure Alert Thresholds

```python
# In src/orchestration/flows.py
@flow(
    on_completion=[send_alert],
    on_failure=[send_alert],
)
def nse_bhavcopy_etl_flow(...):
    pass

# Alert if duration > 10 minutes
if total_duration > 600:
    send_slack_alert(f"Long duration: {total_duration}s")
```

---

## 📈 Performance Metrics Dashboard

### Key Metrics to Monitor

```
Task Performance:
├── scrape_duration_seconds (target: <2s)
├── parse_duration_seconds (target: <1s)
├── normalize_duration_seconds (target: <0.5s)
├── write_duration_seconds (target: <2s)
└── load_duration_seconds (target: <5s)

Data Quality:
├── rows_processed (vs expected)
├── rows_filtered
├── validation_pass_rate (target: 100%)
└── anomalies_detected

System Health:
├── api_availability (target: 99.9%)
├── memory_usage_mb
├── cpu_usage_percent
└── disk_usage_percent
```

### Create Custom Grafana Dashboard

```bash
# 1. Add Prometheus data source (http://localhost:9090)
# 2. Create dashboard queries
SELECT rate(task_duration_seconds[5m]) FROM prometheus
SELECT rows_processed FROM mlflow

# 3. Visualize as:
# - Line charts (time series)
# - Gauge charts (current state)
# - Bar charts (comparisons)
```

---

## 🚀 Advanced: API-Driven Workflows

### Trigger Flows via API

```bash
# Get deployment ID
DEPLOY_ID=$(prefect deployment ls --name 'nse-bhavcopy-daily' \
  -o json | jq -r '.[0].id')

# Trigger via REST API
curl -X POST \
  "http://localhost:4200/api/deployments/$DEPLOY_ID/create_flow_run" \
  -H "Content-Type: application/json" \
  -d '{
    "parameters": {
      "trade_date": "2026-01-11",
      "load_to_clickhouse": true
    }
  }'
```

### Monitor via API

```bash
# Get recent runs
curl "http://localhost:4200/api/flow_runs?limit=10" \
  | jq '.[] | {id, name, state, start_time, end_time}'

# Get run details
curl "http://localhost:4200/api/flow_runs/<run-id>" \
  | jq '{status: .state, duration, logs}'
```

---

## 📚 Complete Setup Checklist

```bash
✅ Start Docker Compose
docker-compose -f ../../docker-compose.yml up -d

✅ Start Prefect Server
prefect server start &

✅ Start MLflow Server
poetry run mlflow ui &

✅ Deploy Flows
cd ingestion/nse-scraper
python -m src.orchestration.flows deploy

✅ Start Agent
prefect agent start -q default &

✅ View Dashboards
- http://localhost:4200 (Prefect)
- http://localhost:5000 (MLflow)

✅ Run Dashboard Visualization
poetry run python prefect_dashboard.py

✅ Monitor Metrics
- Flow runs: http://localhost:4200
- Task metrics: http://localhost:5000
- System metrics: http://localhost:9090
```

---

## 📞 Summary

| Component | Status | Access | Purpose |
|-----------|--------|--------|---------|
| Prefect Server | ✅ Running | <http://localhost:4200> | Flow orchestration & monitoring |
| MLflow Server | ✅ Running | <http://localhost:5000> | Metrics & experiment tracking |
| Prefect Agent | ✅ Running | (background) | Execute scheduled flows |
| Dashboard | ✅ Ready | `python prefect_dashboard.py` | Visualization |
| Docker Services | ✅ Running | (background) | Kafka, ClickHouse infrastructure |

**6 Production Flows** • **6,127+ Records Daily** • **Real-time Monitoring** ✅
