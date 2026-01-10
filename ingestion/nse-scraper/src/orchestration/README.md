# Prefect Orchestration

This module provides Prefect flows for orchestrating the NSE data pipeline with scheduling, retries, and observability through MLflow.

## Overview

The orchestration module defines a complete ETL pipeline for NSE bhavcopy data:

1. **Scrape** - Download bhavcopy CSV from NSE
2. **Parse** - Parse CSV to Polars DataFrame
3. **Normalize** - Validate and normalize data
4. **Write** - Write to Parquet format with partitioning
5. **Load** - Load into ClickHouse (optional)

All steps include retry logic, logging, and metrics tracking via MLflow.

## Components

### Tasks

- `scrape_bhavcopy` - Downloads NSE bhavcopy for a given date
- `parse_polars_raw` - Parses CSV to Polars DataFrame
- `normalize_polars` - Normalizes and validates data
- `write_parquet` - Writes to partitioned Parquet format
- `load_clickhouse` - Loads data into ClickHouse

### Flow

- `nse_bhavcopy_etl_flow` - Main orchestration flow combining all tasks

## Usage

### Local Execution

Run the flow locally for a specific date:

```python
from datetime import date
from src.orchestration.flows import nse_bhavcopy_etl_flow

# Run for a specific date
result = nse_bhavcopy_etl_flow(
    trade_date=date(2024, 1, 15),
    output_base_path="data/lake",
    load_to_clickhouse=True
)

print(f"Processed {result['rows_processed']} rows")
```

Or run from command line:

```bash
cd ingestion/nse-scraper
python -m src.orchestration.flows
```

### Creating a Deployment

Create a scheduled deployment that runs weekdays at 6pm IST:

```bash
cd ingestion/nse-scraper
python -m src.orchestration.flows deploy
```

This creates a deployment with:
- Schedule: Weekdays (Mon-Fri) at 6:00 PM IST (12:30 PM UTC)
- Retries: Configured per task (2-3 retries with backoff)
- Tags: `nse`, `bhavcopy`, `daily`, `production`

### Starting a Prefect Agent

To run scheduled flows, start a Prefect agent:

```bash
# Start agent for default work queue
prefect agent start -q default

# Or in detached mode
prefect agent start -q default &
```

### Running with Prefect Server

1. Start Prefect server:

```bash
prefect server start
```

2. Access the UI at http://localhost:4200

3. Deploy the flow:

```bash
python -m src.orchestration.flows deploy
```

4. Start an agent:

```bash
prefect agent start -q default
```

## Configuration

### Environment Variables

Configure the pipeline via environment variables:

```bash
# Data directory
export DATA_DIR=./data

# ClickHouse connection
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=champion_user
export CLICKHOUSE_PASSWORD=champion_pass
export CLICKHOUSE_DATABASE=champion_market

# MLflow tracking
export MLFLOW_TRACKING_URI=http://localhost:5000
export MLFLOW_EXPERIMENT_NAME=nse-bhavcopy-pipeline
```

### Flow Parameters

The flow accepts these parameters:

- `trade_date` (date, optional) - Date to process (defaults to previous day)
- `output_base_path` (str, optional) - Base path for Parquet output
- `load_to_clickhouse` (bool) - Whether to load to ClickHouse (default: True)
- `clickhouse_host` (str, optional) - ClickHouse host override
- `clickhouse_port` (int, optional) - ClickHouse port override
- `clickhouse_user` (str, optional) - ClickHouse user override
- `clickhouse_password` (str, optional) - ClickHouse password override
- `clickhouse_database` (str, optional) - ClickHouse database override

## Retry Configuration

Each task has specific retry configuration:

- **scrape_bhavcopy**: 3 retries, 60s delay
- **parse_polars_raw**: 2 retries, 30s delay
- **normalize_polars**: 2 retries, 30s delay
- **write_parquet**: 2 retries, 30s delay
- **load_clickhouse**: 3 retries, 60s delay

## MLflow Integration

The flow logs the following metrics to MLflow:

### Task Metrics

- `scrape_duration_seconds` - Time to download bhavcopy
- `parse_duration_seconds` - Time to parse CSV
- `raw_rows_parsed` - Number of rows parsed
- `normalize_duration_seconds` - Time to normalize
- `normalized_rows` - Number of rows after normalization
- `filtered_rows` - Number of rows filtered out
- `write_duration_seconds` - Time to write Parquet
- `parquet_size_mb` - Size of Parquet file
- `rows_written` - Number of rows written
- `load_duration_seconds` - Time to load to ClickHouse
- `rows_loaded` - Number of rows loaded

### Flow Metrics

- `flow_duration_seconds` - Total flow execution time

### Parameters Logged

- `trade_date` - Date being processed
- `load_to_clickhouse` - Whether ClickHouse load was requested
- `clickhouse_table` - Target ClickHouse table
- `status` - Flow status (success/failed)
- `error` - Error message if failed

## Testing

Run the test suite:

```bash
cd ingestion/nse-scraper

# Run all tests
pytest tests/integration/test_flows.py -v

# Run specific test
pytest tests/integration/test_flows.py::test_parse_polars_raw_task -v

# Skip integration tests
SKIP_INTEGRATION_TESTS=true pytest tests/integration/test_flows.py -v
```

## Monitoring

### Prefect UI

View flow runs, task status, and logs in the Prefect UI:
- http://localhost:4200

### MLflow UI

View metrics, parameters, and artifacts in the MLflow UI:
- http://localhost:5000

## Scheduling Details

The default schedule runs:
- **Days**: Monday through Friday (weekdays only)
- **Time**: 6:00 PM IST (18:00 IST = 12:30 PM UTC)
- **Timezone**: UTC
- **Cron**: `30 12 * * 1-5`

This timing allows the flow to run after market close (NSE closes at 3:30 PM IST) with buffer time for data availability.

## Troubleshooting

### Flow Fails to Download

- Check NSE website availability
- Verify the date is a trading day
- Check network connectivity

### ClickHouse Load Fails

- Verify ClickHouse is running: `docker-compose ps clickhouse`
- Check connection settings
- Verify table exists: `SHOW TABLES FROM champion_market`

### MLflow Metrics Not Logged

- Start MLflow server: `mlflow server --host 0.0.0.0 --port 5000`
- Set tracking URI: `export MLFLOW_TRACKING_URI=http://localhost:5000`
- Create experiment: `mlflow experiments create -n nse-bhavcopy-pipeline`

### Agent Not Picking Up Deployments

- Verify agent is running: `prefect agent ls`
- Check work queue matches: `prefect work-queue ls`
- Verify deployment exists: `prefect deployment ls`

## Example: Running for Historical Dates

Process multiple historical dates:

```python
from datetime import date, timedelta
from src.orchestration.flows import nse_bhavcopy_etl_flow

# Process last 5 trading days
start_date = date(2024, 1, 10)
for i in range(5):
    trade_date = start_date + timedelta(days=i)
    
    # Skip weekends (basic check)
    if trade_date.weekday() < 5:  # Mon=0, Fri=4
        print(f"Processing {trade_date}...")
        result = nse_bhavcopy_etl_flow(
            trade_date=trade_date,
            output_base_path="data/lake",
            load_to_clickhouse=True
        )
        print(f"✅ Processed {result['rows_processed']} rows")
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Prefect Orchestration                    │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐      ┌──────────────┐      ┌───────────┐ │
│  │   Scrape     │─────▶│    Parse     │─────▶│ Normalize │ │
│  │  Bhavcopy    │      │    Polars    │      │  Polars   │ │
│  └──────────────┘      └──────────────┘      └───────────┘ │
│         │                     │                      │       │
│         │                     │                      │       │
│         ▼                     ▼                      ▼       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              MLflow Metrics Logging                  │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌───────────┐      ┌──────────────┐                        │
│  │   Write   │─────▶│     Load     │                        │
│  │  Parquet  │      │  ClickHouse  │                        │
│  └───────────┘      └──────────────┘                        │
│         │                     │                              │
│         ▼                     ▼                              │
│  ┌──────────────┐      ┌──────────────┐                    │
│  │  Data Lake   │      │  ClickHouse  │                    │
│  │   Parquet    │      │   Database   │                    │
│  └──────────────┘      └──────────────┘                    │
└─────────────────────────────────────────────────────────────┘
```

## Dependencies

Required packages (already in pyproject.toml):
- `prefect>=2.14.0` - Workflow orchestration
- `mlflow>=2.9.0` - Experiment tracking
- `clickhouse-connect>=0.7.0` - ClickHouse client
- `polars>=0.20.0` - DataFrame processing
- `structlog>=24.1.0` - Logging

## License

This module is part of the Champion market data platform.
