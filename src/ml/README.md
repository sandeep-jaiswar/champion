# MLflow Tracking Integration

This module provides a clean abstraction layer for MLflow tracking, making it easy to log experiments, parameters, metrics, and artifacts across Prefect flows and tasks.

## Overview

The `MLflowTracker` class provides:
- Automatic MLflow tracking URI configuration
- Experiment management with automatic creation
- Context manager for run lifecycle management
- Convenience methods for logging params, metrics, and artifacts
- Error handling and structured logging
- Special utilities for data pipeline metadata

## Usage

### Basic Example

```python
from src.ml.tracking import MLflowTracker

# Initialize tracker
tracker = MLflowTracker(
    experiment_name="my-experiment",
    tracking_uri="http://localhost:5000"
)

# Start a run and log metrics
with tracker.start_run(run_name="my-run"):
    tracker.log_param("model_type", "random_forest")
    tracker.log_metric("accuracy", 0.95)
    tracker.log_artifact("model.pkl")
```

### Integration with Prefect Flows

The tracking abstraction is already integrated into the NSE bhavcopy ETL flow in `ingestion/nse-scraper/src/orchestration/flows.py`. The flow:

1. Configures MLflow tracking URI from environment variable `MLFLOW_TRACKING_URI`
2. Creates a run per flow execution with name `bhavcopy-etl-{date}`
3. Logs parameters: trade_date, load_to_clickhouse, clickhouse_table
4. Logs metrics: scrape_duration_seconds, parse_duration_seconds, normalize_duration_seconds, etc.
5. Logs row counts, durations, and partition metadata

### Data Pipeline Metadata

Use the `log_execution_metadata()` helper for common pipeline metrics:

```python
tracker.log_execution_metadata(
    trade_date=date(2024, 1, 10),
    row_count=1500,
    duration_seconds=42.5,
    partition_info={"year": 2024, "month": 1, "day": 10}
)
```

This logs:
- `trade_date` parameter
- `partition_*` parameters (year, month, day, etc.)
- `row_count` metric
- `duration_seconds` metric

## Configuration

Set the MLflow tracking server URI via environment variable:

```bash
export MLFLOW_TRACKING_URI=http://localhost:5000
```

Or pass it directly when creating the tracker:

```python
tracker = MLflowTracker(
    experiment_name="my-experiment",
    tracking_uri="http://mlflow-server:5000"
)
```

## MLflow UI

Access the MLflow UI at: http://localhost:5000

The UI shows:
- Experiments and runs
- Parameters and metrics for each run
- Artifacts stored
- Run comparisons
- Metric visualizations

## Docker Setup

The MLflow server is configured in the root `docker-compose.yml`:

```bash
# Start MLflow server
docker compose up -d mlflow

# Check MLflow is healthy
curl http://localhost:5000/health
```

The server uses:
- SQLite backend store at `/mlflow/mlflow.db`
- Local artifact store at `/mlflow/artifacts`
- Port 5000 (exposed to host)

## API Reference

### MLflowTracker

#### `__init__(experiment_name, tracking_uri=None)`
Initialize the tracker with an experiment name and optional tracking URI.

#### `start_run(run_name=None, tags=None, nested=False)`
Context manager to start an MLflow run.

#### `log_param(key, value)`
Log a single parameter.

#### `log_params(params)`
Log multiple parameters from a dictionary.

#### `log_metric(key, value, step=None)`
Log a single metric value.

#### `log_metrics(metrics, step=None)`
Log multiple metrics from a dictionary.

#### `log_artifact(local_path, artifact_path=None)`
Log a local file as an artifact.

#### `log_execution_metadata(trade_date, row_count, duration_seconds, partition_info)`
Log common pipeline execution metadata (helper method).

#### `set_tag(key, value)`
Set a tag on the current run.

#### `set_tags(tags)`
Set multiple tags from a dictionary.

## Best Practices

1. **One run per flow execution**: Create a single MLflow run for each Prefect flow execution
2. **Meaningful run names**: Use descriptive names like `bhavcopy-etl-2024-01-10`
3. **Log early**: Log parameters at the start of the run
4. **Log incrementally**: Log metrics as tasks complete
5. **Use structured logging**: The tracker integrates with structlog for consistent logging
6. **Handle errors**: The tracker logs warnings on failures but doesn't crash your pipeline

## Examples

### Logging Model Training

```python
with tracker.start_run(run_name="model-training"):
    # Log hyperparameters
    tracker.log_params({
        "learning_rate": 0.01,
        "batch_size": 32,
        "epochs": 100
    })
    
    # Train model and log metrics
    for epoch in range(100):
        loss = train_epoch()
        tracker.log_metric("train_loss", loss, step=epoch)
    
    # Log final metrics
    tracker.log_metric("final_accuracy", 0.95)
    
    # Save model artifact
    tracker.log_artifact("model.pkl")
```

### Logging ETL Pipeline

```python
with tracker.start_run(run_name=f"etl-{date.today()}"):
    # Log configuration
    tracker.log_params({
        "source": "s3://bucket/data",
        "destination": "warehouse.table"
    })
    
    # Log execution metadata
    tracker.log_execution_metadata(
        trade_date=date.today(),
        row_count=10000,
        duration_seconds=120.5
    )
    
    # Log data quality metrics
    tracker.log_metrics({
        "null_percentage": 0.02,
        "duplicate_percentage": 0.001
    })
```
