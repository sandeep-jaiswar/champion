# Prometheus Metrics for NSE Pipeline

This document describes the Prometheus metrics exposed by the NSE data pipeline for monitoring and observability.

## Metrics Server

The metrics server is automatically started during flow execution and exposes metrics on port 9090 by default.

### Configuration

- **Port**: 9090 (configurable via `metrics_port` parameter)
- **Endpoint**: `http://localhost:9090/metrics`
- **Format**: Prometheus text exposition format

## Available Metrics

### Pipeline Stage Metrics

#### Parquet Write Metrics

- **`nse_pipeline_parquet_write_success_total`**
  - Type: Counter
  - Description: Total number of successful Parquet writes
  - Labels: `table` (e.g., "normalized_equity_ohlc")
  
- **`nse_pipeline_parquet_write_failed_total`**
  - Type: Counter
  - Description: Total number of failed Parquet writes
  - Labels: `table` (e.g., "normalized_equity_ohlc")

#### ClickHouse Load Metrics

- **`nse_pipeline_clickhouse_load_success_total`**
  - Type: Counter
  - Description: Total number of successful ClickHouse loads
  - Labels: `table` (e.g., "normalized_equity_ohlc", "raw_equity_ohlc")
  
- **`nse_pipeline_clickhouse_load_failed_total`**
  - Type: Counter
  - Description: Total number of failed ClickHouse loads
  - Labels: `table` (e.g., "normalized_equity_ohlc", "raw_equity_ohlc")

#### Flow Duration Metrics

- **`nse_pipeline_flow_duration_seconds`**
  - Type: Histogram
  - Description: Time spent executing complete ETL flow
  - Labels:
    - `flow_name` (e.g., "nse-bhavcopy-etl")
    - `status` ("success" or "failed")
  - Buckets: Default Prometheus histogram buckets

### Legacy Scraper Metrics

These metrics are still available for backward compatibility:

- `nse_scraper_files_downloaded_total` - Total files downloaded
- `nse_scraper_rows_parsed_total` - Total rows parsed
- `nse_scraper_kafka_produce_success_total` - Successful Kafka produces
- `nse_scraper_kafka_produce_failed_total` - Failed Kafka produces
- `nse_scraper_scrape_duration_seconds` - Scraping duration
- `nse_scraper_last_successful_scrape_timestamp` - Last successful scrape timestamp

## Usage

### In Production Flows

The metrics server is automatically started when running the ETL flow:

```python
from src.orchestration.flows import nse_bhavcopy_etl_flow

# Metrics server starts automatically on port 9090
result = nse_bhavcopy_etl_flow(
    trade_date=date(2024, 1, 2),
    metrics_port=9090,  # Optional: customize port
    start_metrics_server_flag=True,  # Optional: disable with False
)
```

### Manual Testing

To test metrics without running a full pipeline:

```bash
cd ingestion/nse-scraper
python -m tests.manual.test_pipeline_metrics
```

Then access metrics at: `http://localhost:9090/metrics`

### Querying Metrics

#### Using curl

```bash
# Get all metrics
curl http://localhost:9090/metrics

# Filter specific metrics
curl http://localhost:9090/metrics | grep nse_pipeline
```

#### Example Prometheus Queries

```promql
# Success rate of parquet writes (per second)
rate(nse_pipeline_parquet_write_success_total[5m])

# Failure rate of ClickHouse loads
rate(nse_pipeline_clickhouse_load_failed_total[5m])

# 95th percentile flow duration (successful flows)
histogram_quantile(0.95, rate(nse_pipeline_flow_duration_seconds_bucket{status="success"}[5m]))

# Total flow executions in the last hour
increase(nse_pipeline_flow_duration_seconds_count[1h])

# Error rate percentage
sum(rate(nse_pipeline_parquet_write_failed_total[5m])) 
/ 
sum(rate(nse_pipeline_parquet_write_success_total[5m]) + rate(nse_pipeline_parquet_write_failed_total[5m])) 
* 100
```

## Grafana Dashboard

A pre-configured Grafana dashboard is available at `grafana_dashboard.json`.

### Importing the Dashboard

1. Open Grafana UI
2. Navigate to Dashboards → Import
3. Upload `grafana_dashboard.json`
4. Configure the Prometheus data source
5. Click "Import"

### Dashboard Panels

The dashboard includes:

1. **Parquet Write Operations** - Success/failure rates over time
2. **ClickHouse Load Operations** - Success/failure rates over time
3. **Flow Duration** - p50, p95, p99 percentiles
4. **Success vs Failed Flows** - Flow execution success rate
5. **Operation Counters** - Total counts for each operation
6. **Average Flow Duration** - Average execution time
7. **Error Rate Gauge** - Overall error percentage

## Integration with Monitoring Stack

### Prometheus Configuration

Add this scrape config to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'nse-pipeline'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 15s
    scrape_timeout: 10s
```

### Alerting Rules

Example alert rules for Prometheus:

```yaml
groups:
  - name: nse_pipeline_alerts
    interval: 1m
    rules:
      - alert: HighParquetWriteFailureRate
        expr: rate(nse_pipeline_parquet_write_failed_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High Parquet write failure rate"
          description: "Parquet write failures are above 0.1/sec for 5 minutes"

      - alert: HighClickHouseLoadFailureRate
        expr: rate(nse_pipeline_clickhouse_load_failed_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High ClickHouse load failure rate"
          description: "ClickHouse load failures are above 0.1/sec for 5 minutes"

      - alert: SlowFlowExecution
        expr: histogram_quantile(0.95, rate(nse_pipeline_flow_duration_seconds_bucket{status="success"}[5m])) > 300
        for: 10m
        labels:
          severity: info
        annotations:
          summary: "Slow ETL flow execution"
          description: "95th percentile flow duration is above 5 minutes"

      - alert: NoSuccessfulFlows
        expr: rate(nse_pipeline_flow_duration_seconds_count{status="success"}[1h]) == 0
        for: 1h
        labels:
          severity: critical
        annotations:
          summary: "No successful flows in the last hour"
          description: "The pipeline has not completed successfully for 1 hour"
```

## Testing

Run the integration test to verify metrics are properly tracked:

```bash
cd ingestion/nse-scraper
pytest tests/integration/test_flows.py::test_prometheus_metrics_tracking -v
```

## Troubleshooting

### Metrics server not starting

If you see "Address already in use" errors:

- Another process is using port 9090
- Change the port using `metrics_port` parameter
- Or set `start_metrics_server_flag=False` to disable

### Metrics not showing in Prometheus

1. Check if the metrics server is running: `curl http://localhost:9090/metrics`
2. Verify Prometheus scrape configuration
3. Check Prometheus targets page for scrape status
4. Ensure firewall allows connections to port 9090

### No data in Grafana

1. Verify Prometheus data source is configured correctly
2. Check that metrics are being collected in Prometheus
3. Adjust time range in Grafana (metrics are recent)
4. Verify the correct dashboard variables and filters

## Related Documentation

- [MLflow Tracking](../docs/mlflow_tracking.md) - Experiment tracking integration
- [Prefect Flows](../docs/prefect_flows.md) - Flow orchestration
- [Prometheus Documentation](https://prometheus.io/docs/introduction/overview/)
- [Grafana Documentation](https://grafana.com/docs/)
