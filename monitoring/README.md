# Production Monitoring Setup

This directory contains the complete observability stack for the Champion platform, including Prometheus metrics, Grafana dashboards, and alerting rules.

## Overview

The monitoring setup provides comprehensive observability for:

- **Pipeline Health**: ETL flow execution, success/failure rates, duration metrics
- **Data Quality**: Validation metrics, data ingestion rates, corporate actions processing
- **Circuit Breakers**: State monitoring, failure tracking, transitions
- **Performance**: CPU/memory usage, load latency, throughput metrics
- **Error Tracking**: Error rates, failure trends, operational issues

## Directory Structure

```
monitoring/
├── alert_rules.yml                    # Prometheus alert rules
├── prometheus.yml                     # Prometheus configuration
├── grafana/
│   ├── dashboards/
│   │   ├── champion-overview.json     # Legacy overview dashboard
│   │   ├── pipeline-health.json       # Pipeline health metrics
│   │   ├── data-quality.json          # Data quality and validation
│   │   ├── circuit-breaker.json       # Circuit breaker monitoring
│   │   ├── performance-metrics.json   # System performance
│   │   └── error-rate-trends.json     # Error tracking and trends
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboards.yml         # Dashboard provisioning config
│       └── datasources/
│           └── prometheus.yml         # Prometheus datasource config
```

## Quick Start

### 1. Start Monitoring Stack

The monitoring services are included in the main `docker-compose.yml`:

```bash
docker-compose up -d prometheus grafana
```

**Important**: For market hours alerts to work correctly, ensure Prometheus is configured with the correct timezone:

```yaml
# docker-compose.yml
services:
  prometheus:
    environment:
      - TZ=Asia/Kolkata  # For Indian markets (IST - no DST)
    # or
    # - TZ=America/New_York  # For US markets (EST/EDT - observes DST)
```

**Note**: Indian Standard Time (IST) does not observe Daylight Saving Time (DST). For markets in regions that observe DST (e.g., US markets), be aware that alert times will shift during DST transitions. Consider using UTC offsets in alert expressions or adjusting thresholds seasonally for DST-observing regions.

### 2. Access Dashboards

- **Prometheus**: <http://localhost:9090>
- **Grafana**: <http://localhost:3000> (default credentials: admin/admin)

### 3. Configure Application Metrics

Ensure your application starts the metrics server:

```python
from champion.utils.metrics import start_metrics_server

# Start metrics endpoint on port 9090
start_metrics_server(port=9090)
```

## Available Metrics

### Scraper Metrics

- `nse_scraper_files_downloaded_total`: Total files downloaded by scraper
- `nse_scraper_rows_parsed_total`: Rows parsed (success/failed)
- `nse_scraper_scrape_duration_seconds`: Scraping duration histogram
- `nse_scraper_last_successful_scrape_timestamp`: Last successful scrape time

### Pipeline Metrics

- `nse_pipeline_parquet_write_success_total`: Successful Parquet writes
- `nse_pipeline_parquet_write_failed_total`: Failed Parquet writes
- `nse_pipeline_clickhouse_load_success_total`: Successful ClickHouse loads
- `nse_pipeline_clickhouse_load_failed_total`: Failed ClickHouse loads
- `nse_pipeline_flow_duration_seconds`: Complete flow execution time

### Circuit Breaker Metrics

- `circuit_breaker_state`: Current state (0=CLOSED, 1=HALF_OPEN, 2=OPEN)
- `circuit_breaker_failures_total`: Total failures tracked
- `circuit_breaker_state_transitions_total`: State transition count

### Business Metrics

- `champion_stocks_ingested_total`: Total stocks ingested per run
- `champion_corporate_actions_processed_total`: CA events processed
- `champion_validation_failure_rate`: Current validation failure rate (0-1)
- `champion_validation_failures_total`: Total validation failures
- `champion_validation_total`: Total validation checks performed
- `champion_warehouse_load_latency_seconds`: Warehouse load time histogram

## Grafana Dashboards

### 1. Pipeline Health Dashboard

**Purpose**: Monitor overall pipeline execution and health

**Key Panels**:

- Successful/Failed flow rates
- Average flow duration
- Parquet write operations
- ClickHouse load operations
- Files downloaded rate
- Rows parsed rate

**Use Cases**:

- Identify pipeline bottlenecks
- Monitor flow success rates
- Track data processing throughput

### 2. Data Quality Dashboard

**Purpose**: Track data quality and validation metrics

**Key Panels**:

- Validation failure rate by table
- Stocks ingested rate
- Corporate actions processed
- Validation failures by type
- Stock ingestion by scraper
- CA events by type
- Validation failure trends

**Use Cases**:

- Monitor data quality issues
- Track validation failures
- Identify data anomalies
- Verify business metrics

### 3. Circuit Breaker Dashboard

**Purpose**: Monitor circuit breaker states and failures

**Key Panels**:

- Current circuit breaker states
- State history timeline
- Failure rate by source
- Total failures by source
- State transitions (hourly)
- State transition summary table

**Use Cases**:

- Detect service availability issues
- Track recovery attempts
- Analyze failure patterns
- Plan maintenance windows

### 4. Performance Metrics Dashboard

**Purpose**: Monitor system resource usage and performance

**Key Panels**:

- CPU usage (with 1.7 core threshold)
- Memory usage (with 3GB/3.5GB thresholds)
- Warehouse load latency (p50, p95)
- Scrape duration (p50, p95)
- Throughput metrics (rows/sec, loads/sec, writes/sec, files/sec)

**Use Cases**:

- Identify resource constraints
- Optimize performance
- Plan capacity upgrades
- Monitor SLA compliance

### 5. Error Rate Trends Dashboard

**Purpose**: Track errors and failure trends over time

**Key Panels**:

- Error rates by component
- Pipeline failure rate
- Hourly failure counts (Parquet, ClickHouse, Validation, Circuit Breaker)
- Failed row parsing rate
- Validation failure trends
- 24-hour error summary table

**Use Cases**:

- Identify error patterns
- Track failure trends
- Prioritize fixes
- Generate incident reports

## Alert Rules

### Circuit Breaker Alerts

- **CircuitBreakerOpened**: Critical alert when circuit is open for >1 minute
- **CircuitBreakerHighFailureRate**: Warning when failure rate >0.1/sec

### Validation Alerts

- **HighValidationFailureRate**: Warning when >5% failures for 5 minutes
- **CriticalValidationFailureRate**: Critical when >20% failures for 2 minutes

### Pipeline Alerts

- **PipelineDurationAnomaly**: Warning when duration is 2x the 1-hour average
- **PipelineFailureRate**: Critical when >10% failure rate for 5 minutes

### Warehouse Alerts

- **WarehouseLoadFailure**: Critical on any load failures
- **WarehouseLoadHighLatency**: Warning when p95 latency >60 seconds
- **ParquetWriteFailures**: Critical on any write failures

### Resource Alerts

- **HighMemoryUsage**: Warning at >85% of 4GB limit
- **CriticalMemoryUsage**: Critical at >95% of 4GB limit
- **HighCPUUsage**: Warning at >1.7 cores for 10 minutes
- **ServiceDown**: Critical when service is unreachable

### Data Quality Alerts

- **NoDataIngested**: Warning when no data ingested during market hours
- **LowDataVolume**: Warning when <100 stocks/hour for 30 minutes

## Testing Alerts

### 1. Using Prometheus Expression Browser

```bash
# Check alert state
http://localhost:9090/alerts

# Manually trigger conditions (example)
# Temporarily increase validation failure rate in code
```

### 2. Using amtool (with Alertmanager)

```bash
# View active alerts
amtool alert

# Silence an alert
amtool silence add alertname=CircuitBreakerOpened

# View silences
amtool silence query
```

### 3. Simulating Alert Conditions

Create a test script to trigger specific conditions:

```python
from champion.utils.metrics import (
    circuit_breaker_state,
    validation_failure_rate,
)

# Simulate open circuit breaker
circuit_breaker_state.labels(source="test_source").set(2)

# Simulate high validation failure rate
validation_failure_rate.labels(table="test_table").set(0.15)

# Wait for alert to fire (check Prometheus alerts page)
```

## Customizing Dashboards

### Adding New Panels

1. Open Grafana dashboard
2. Click "Add panel"
3. Select visualization type
4. Configure query using PromQL
5. Set display options
6. Save dashboard
7. Export JSON and commit to repository

### Modifying Alert Rules

1. Edit `alert_rules.yml`
2. Reload Prometheus configuration:

   ```bash
   curl -X POST http://localhost:9090/-/reload
   ```

   Or restart Prometheus:

   ```bash
   docker-compose restart prometheus
   ```

## Best Practices

### Metric Naming

Follow Prometheus naming conventions:

- Use `_total` suffix for counters
- Use base units (seconds, bytes, not milliseconds, megabytes)
- Use meaningful labels
- Keep cardinality low (<100 unique label combinations per metric)

### Dashboard Design

- Group related metrics
- Use appropriate visualization types
- Set meaningful thresholds
- Add descriptions and units
- Use template variables for flexibility
- Keep load times under 5 seconds

### Alert Configuration

- Set appropriate thresholds based on SLAs
- Use `for` clause to avoid flapping
- Classify severity correctly
- Write clear annotations
- Include runbook links
- Test before deploying

### Performance

- Limit time range for expensive queries
- Use recording rules for complex calculations
- Optimize label cardinality
- Set appropriate scrape intervals
- Monitor Prometheus resource usage

## Troubleshooting

### Metrics Not Appearing

1. **Check metrics endpoint**: `curl http://localhost:9090/metrics`
2. **Verify Prometheus targets**: <http://localhost:9090/targets>
3. **Check application logs** for errors
4. **Verify network connectivity** between services

### Dashboards Not Loading

1. **Check Grafana logs**: `docker-compose logs grafana`
2. **Verify datasource configuration**
3. **Test datasource connection** in Grafana UI
4. **Check dashboard JSON syntax**

### Alerts Not Firing

1. **Verify alert rules syntax**: <http://localhost:9090/rules>
2. **Check alert evaluation**: <http://localhost:9090/alerts>
3. **Verify metric values** match alert conditions
4. **Check Alertmanager connectivity** (if configured)

### High Memory Usage

1. **Reduce retention period** in `prometheus.yml`
2. **Limit time range** in dashboards
3. **Optimize queries** (avoid high cardinality)
4. **Increase Prometheus resources** in docker-compose.yml

## Integration with Other Tools

### Alertmanager

Configure alert routing, grouping, and notifications:

```yaml
# prometheus.yml
alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093
```

### Slack Notifications

Configure Alertmanager for Slack:

```yaml
# alertmanager.yml
receivers:
  - name: 'slack'
    slack_configs:
      - api_url: 'YOUR_WEBHOOK_URL'
        channel: '#alerts'
        title: 'Champion Alert'
```

### PagerDuty

Configure critical alerts to PagerDuty:

```yaml
receivers:
  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: 'YOUR_SERVICE_KEY'
        severity: 'critical'
```

## Log Aggregation

For comprehensive observability, integrate with log aggregation:

- **ELK Stack**: Elasticsearch, Logstash, Kibana
- **Grafana Loki**: Lightweight log aggregation

See [docs/LOG_AGGREGATION_SETUP.md](../docs/LOG_AGGREGATION_SETUP.md) for detailed setup instructions.

## Maintenance

### Regular Tasks

- **Daily**: Review error dashboards, check alert status
- **Weekly**: Analyze performance trends, optimize queries
- **Monthly**: Review retention policies, archive old data
- **Quarterly**: Update alert thresholds, review dashboard relevance

### Backup

Regularly backup:

- Grafana dashboards (export JSON)
- Prometheus configuration
- Alert rules
- Historical metrics (if needed)

### Updates

Keep components updated:

- Monitor security advisories
- Test updates in staging
- Update docker images in `docker-compose.yml`
- Document breaking changes

## Resources

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [PromQL Tutorial](https://prometheus.io/docs/prometheus/latest/querying/basics/)
- [Grafana Dashboard Best Practices](https://grafana.com/docs/grafana/latest/best-practices/)
- [Prometheus Alerting Rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/)

## Support

For issues or questions:

1. Check this README and linked documentation
2. Review Prometheus/Grafana logs
3. Search GitHub issues
4. Create new issue with details
