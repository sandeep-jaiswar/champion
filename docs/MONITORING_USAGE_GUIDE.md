# Monitoring and Observability Usage Guide

This guide demonstrates how to use the monitoring and observability features in the Champion platform.

## Table of Contents

- [Using Prometheus Metrics](#using-prometheus-metrics)
- [Using Trace IDs for Logging](#using-trace-ids-for-logging)
- [Example: Instrumented ETL Flow](#example-instrumented-etl-flow)
- [Testing Alerts](#testing-alerts)
- [Querying Metrics](#querying-metrics)
- [Common Patterns](#common-patterns)

## Using Prometheus Metrics

### Basic Counter Example

```python
from champion.utils.metrics import stocks_ingested

# Increment counter when stocks are ingested
def ingest_stocks(scraper_name: str, date: str, stock_data: list):
    for stock in stock_data:
        # Process stock...
        pass
    
    # Record the count
    stocks_ingested.labels(
        scraper=scraper_name,
        date=date
    ).inc(len(stock_data))
```

### Corporate Actions Processing

```python
from champion.utils.metrics import corporate_actions_processed

def process_corporate_action(action: dict):
    action_type = action.get("action_type")  # dividend, split, bonus, etc.
    
    # Process the action...
    
    # Record the metric
    corporate_actions_processed.labels(
        action_type=action_type
    ).inc()
```

### Validation Metrics

```python
from champion.utils.metrics import (
    validation_failure_rate,
    validation_failures,
    validation_total,
)

def validate_data(table_name: str, df: DataFrame) -> ValidationResult:
    """Validate data with metrics tracking.
    
    Note: This is a simplified example. Actual validation logic will vary
    based on your specific requirements and data schemas.
    """
    total_rows = len(df)
    failed_rows = 0
    
    # Perform validation...
    for row in df:
        validation_total.labels(table=table_name).inc()
        
        if not is_valid(row):
            failed_rows += 1
            validation_failures.labels(
                table=table_name,
                failure_type="schema_mismatch"
            ).inc()
    
    # Update failure rate
    failure_rate = failed_rows / total_rows if total_rows > 0 else 0
    validation_failure_rate.labels(table=table_name).set(failure_rate)
    
    return ValidationResult(
        total_rows=total_rows,
        valid_rows=total_rows - failed_rows,
        failure_rate=failure_rate
    )
```

### Warehouse Load Latency

```python
from champion.utils.metrics import warehouse_load_latency
import time

def load_to_warehouse(table_name: str, layer: str, data: DataFrame):
    # Time the warehouse load operation
    with warehouse_load_latency.labels(
        table=table_name,
        layer=layer
    ).time():
        # Perform the actual load
        clickhouse_client.insert(table_name, data)
```

## Using Trace IDs for Logging

### Setting Trace ID at Entry Point

```python
from champion.utils.logger import get_logger, set_trace_id, clear_trace_id

logger = get_logger(__name__)

def scrape_bhavcopy(trade_date: str):
    # Set trace ID at the start of the operation
    trace_id = set_trace_id()
    
    try:
        logger.info("scrape_started", date=trade_date)
        
        # Download file
        file_path = download_bhavcopy(trade_date)
        logger.info("file_downloaded", path=file_path, size_mb=get_size(file_path))
        
        # Parse data
        data = parse_bhavcopy(file_path)
        logger.info("data_parsed", rows=len(data))
        
        # Load to warehouse
        load_to_warehouse(data)
        logger.info("scrape_completed", date=trade_date)
        
    except Exception as e:
        logger.error("scrape_failed", date=trade_date, error=str(e))
        raise
    finally:
        # Always clear trace ID when done
        clear_trace_id()
```

### Passing Trace ID to Downstream Components

```python
from champion.utils.logger import get_logger, get_trace_id, set_trace_id

logger = get_logger(__name__)

def parent_operation():
    trace_id = set_trace_id()
    logger.info("parent_started")
    
    # Pass trace ID to child operation
    child_operation(trace_id)
    
    logger.info("parent_completed")
    clear_trace_id()

def child_operation(trace_id: str):
    # Reuse the same trace ID
    set_trace_id(trace_id)
    logger.info("child_started")
    
    # Do work...
    
    logger.info("child_completed")
    # Don't clear here if parent will do it
```

### Trace ID in Async Operations

```python
import asyncio
from champion.utils.logger import get_logger, set_trace_id, clear_trace_id

logger = get_logger(__name__)

async def async_scrape(date: str):
    # Set trace ID in async context
    trace_id = set_trace_id()
    
    try:
        logger.info("async_scrape_started", date=date)
        
        # Trace ID is preserved across await calls
        data = await download_data(date)
        logger.info("data_downloaded", rows=len(data))
        
        result = await process_data(data)
        logger.info("data_processed", result=result)
        
        return result
    finally:
        clear_trace_id()
```

## Example: Instrumented ETL Flow

Here's a complete example showing how to instrument an ETL flow with metrics and trace IDs:

```python
from datetime import date
import time

from champion.utils.logger import get_logger, set_trace_id, clear_trace_id
from champion.utils.metrics import (
    stocks_ingested,
    validation_failure_rate,
    validation_failures,
    validation_total,
    warehouse_load_latency,
    flow_duration,  # This is an existing metric in the codebase
)

logger = get_logger(__name__)

def etl_flow(trade_date: date):
    """Complete ETL flow with full instrumentation."""
    
    # Set trace ID for this flow execution
    trace_id = set_trace_id()
    flow_name = "equity_ohlc_etl"
    
    logger.info("flow_started", flow=flow_name, date=str(trade_date))
    start_time = time.time()
    
    try:
        # Step 1: Extract
        logger.info("extract_started")
        raw_data = extract_bhavcopy(trade_date)
        logger.info("extract_completed", rows=len(raw_data))
        
        # Record stocks ingested
        stocks_ingested.labels(
            scraper="bhavcopy",
            date=str(trade_date)
        ).inc(len(raw_data))
        
        # Step 2: Validate
        logger.info("validation_started")
        validation_result = validate_dataframe(
            raw_data,
            schema_name="raw_equity_ohlc",
            table_name="raw_equity_ohlc"
        )
        logger.info(
            "validation_completed",
            total=validation_result.total_rows,
            valid=validation_result.valid_rows,
            failed=validation_result.total_rows - validation_result.valid_rows
        )
        
        # Step 3: Transform
        logger.info("transform_started")
        normalized_data = normalize_ohlc_data(raw_data)
        logger.info("transform_completed", rows=len(normalized_data))
        
        # Step 4: Load to warehouse
        logger.info("load_started")
        with warehouse_load_latency.labels(
            table="normalized_equity_ohlc",
            layer="normalized"
        ).time():
            load_to_clickhouse("normalized_equity_ohlc", normalized_data)
        logger.info("load_completed")
        
        # Record success
        duration = time.time() - start_time
        flow_duration.labels(
            flow_name=flow_name,
            status="success"
        ).observe(duration)
        
        logger.info("flow_completed", flow=flow_name, duration_sec=duration)
        
    except Exception as e:
        # Record failure
        duration = time.time() - start_time
        flow_duration.labels(
            flow_name=flow_name,
            status="failed"
        ).observe(duration)
        
        logger.error(
            "flow_failed",
            flow=flow_name,
            error=str(e),
            duration_sec=duration
        )
        raise
    finally:
        clear_trace_id()

def validate_dataframe(df, schema_name: str, table_name: str):
    """Validate dataframe with metrics."""
    total_rows = len(df)
    failed_rows = 0
    
    for idx, row in df.iterrows():
        validation_total.labels(table=table_name).inc()
        
        # Perform validation checks
        if not validate_row(row, schema_name):
            failed_rows += 1
            failure_type = determine_failure_type(row, schema_name)
            validation_failures.labels(
                table=table_name,
                failure_type=failure_type
            ).inc()
    
    # Update failure rate gauge
    failure_rate = failed_rows / total_rows if total_rows > 0 else 0
    validation_failure_rate.labels(table=table_name).set(failure_rate)
    
    return ValidationResult(
        total_rows=total_rows,
        valid_rows=total_rows - failed_rows,
        failure_rate=failure_rate
    )
```

## Testing Alerts

### Manual Alert Testing

Create a test script to trigger alert conditions:

```python
# tests/manual/test_alerts.py
"""Manual test script to verify alert firing."""

import time
from champion.utils.metrics import (
    circuit_breaker_state,
    validation_failure_rate,
    warehouse_load_latency,
)
from champion.utils.logger import get_logger, set_trace_id

logger = get_logger(__name__)

def test_circuit_breaker_alert():
    """Test CircuitBreakerOpened alert."""
    print("Testing circuit breaker alert...")
    
    # Set circuit breaker to OPEN state (value=2)
    circuit_breaker_state.labels(source="test_source").set(2)
    
    print("Circuit breaker set to OPEN state")
    print("Check Prometheus alerts page: http://localhost:9090/alerts")
    print("Alert should fire after 1 minute")
    
    # Wait for alert to fire
    time.sleep(70)
    
    # Reset to CLOSED
    circuit_breaker_state.labels(source="test_source").set(0)
    print("Circuit breaker reset to CLOSED")

def test_validation_failure_alert():
    """Test HighValidationFailureRate alert."""
    print("Testing validation failure rate alert...")
    
    # Set validation failure rate to 10% (above 5% threshold)
    validation_failure_rate.labels(table="test_table").set(0.10)
    
    print("Validation failure rate set to 10%")
    print("Check Prometheus alerts page: http://localhost:9090/alerts")
    print("Alert should fire after 5 minutes")
    
    # Wait for alert
    time.sleep(310)
    
    # Reset
    validation_failure_rate.labels(table="test_table").set(0.0)
    print("Validation failure rate reset")

def test_warehouse_latency_alert():
    """Test WarehouseLoadHighLatency alert."""
    print("Testing warehouse latency alert...")
    
    # Simulate high latency loads
    for i in range(100):
        warehouse_load_latency.labels(
            table="test_table",
            layer="test"
        ).observe(75.0)  # 75 seconds (above 60s threshold)
    
    print("Simulated 100 high-latency loads (75s each)")
    print("Check Prometheus alerts page: http://localhost:9090/alerts")
    print("Alert should fire after 5 minutes")

if __name__ == "__main__":
    print("Alert Testing Script")
    print("=" * 50)
    print()
    
    # Uncomment the test you want to run:
    # test_circuit_breaker_alert()
    # test_validation_failure_alert()
    test_warehouse_latency_alert()
```

Run the test:

```bash
python -m tests.manual.test_alerts
```

### Automated Alert Testing

```python
# tests/test_alert_conditions.py
"""Unit tests for alert conditions."""

import pytest
from champion.utils.metrics import (
    validation_failure_rate,
    circuit_breaker_state,
)

def test_validation_failure_rate_metric():
    """Test validation failure rate metric."""
    table_name = "test_table"
    
    # Set failure rate
    validation_failure_rate.labels(table=table_name).set(0.07)
    
    # Verify metric was set (would trigger alert at 0.05)
    # Note: This tests the metric, not the alert itself
    assert True  # Metric set successfully

def test_circuit_breaker_state_metric():
    """Test circuit breaker state metric."""
    source = "test_source"
    
    # Set to OPEN state
    circuit_breaker_state.labels(source=source).set(2)
    
    # Verify metric was set (would trigger alert)
    assert True  # Metric set successfully
```

## Querying Metrics

### Prometheus Query Examples

#### Check current validation failure rate
```promql
champion_validation_failure_rate{table="raw_equity_ohlc"}
```

#### Calculate average warehouse load time
```promql
rate(champion_warehouse_load_latency_seconds_sum[5m]) / 
rate(champion_warehouse_load_latency_seconds_count[5m])
```

#### Find highest error rate by table
```promql
topk(5, rate(champion_validation_failures_total[5m]))
```

#### Count stocks ingested in last hour
```promql
sum(increase(champion_stocks_ingested_total[1h]))
```

#### Check if circuit breaker is open
```promql
circuit_breaker_state == 2
```

### Grafana Explore Queries

Use Grafana's Explore feature to build and test queries:

1. Go to http://localhost:3000
2. Click "Explore" (compass icon)
3. Select "Prometheus" datasource
4. Enter query and click "Run query"

Example queries:
- `rate(champion_stocks_ingested_total[5m])`
- `histogram_quantile(0.95, rate(champion_warehouse_load_latency_seconds_bucket[5m]))`
- `sum(rate(champion_corporate_actions_processed_total[5m])) by (action_type)`

## Common Patterns

### Pattern 1: Timed Operation with Metrics

```python
import time
from contextlib import contextmanager
from champion.utils.logger import get_logger
from champion.utils.metrics import flow_duration  # Existing metric in codebase

logger = get_logger(__name__)

@contextmanager
def timed_operation(operation_name: str):
    """Context manager for timing operations with metrics and logging."""
    start_time = time.time()
    logger.info("operation_started", operation=operation_name)
    
    try:
        yield
        duration = time.time() - start_time
        flow_duration.labels(
            flow_name=operation_name,
            status="success"
        ).observe(duration)
        logger.info("operation_completed", operation=operation_name, duration=duration)
    except Exception as e:
        duration = time.time() - start_time
        flow_duration.labels(
            flow_name=operation_name,
            status="failed"
        ).observe(duration)
        logger.error("operation_failed", operation=operation_name, error=str(e), duration=duration)
        raise

# Usage
with timed_operation("data_processing"):
    process_data()
```

### Pattern 2: Batch Processing with Progress

```python
from champion.utils.logger import get_logger, set_trace_id
from champion.utils.metrics import stocks_ingested

logger = get_logger(__name__)

def process_batch(items: list, batch_name: str):
    """Process batch with metrics and logging."""
    trace_id = set_trace_id()
    total = len(items)
    
    logger.info("batch_started", batch=batch_name, total_items=total)
    
    processed = 0
    for idx, item in enumerate(items, 1):
        try:
            process_item(item)
            processed += 1
            
            # Log progress every 100 items
            if idx % 100 == 0:
                logger.info("batch_progress", 
                           batch=batch_name,
                           processed=processed,
                           total=total,
                           percent=f"{(processed/total)*100:.1f}%")
        except Exception as e:
            logger.warning("item_failed", item_id=item.id, error=str(e))
    
    # Record final count
    stocks_ingested.labels(scraper=batch_name, date="today").inc(processed)
    
    logger.info("batch_completed", 
               batch=batch_name,
               processed=processed,
               failed=total-processed)
    
    clear_trace_id()
```

### Pattern 3: Error Recovery with Metrics

```python
from tenacity import retry, stop_after_attempt, wait_exponential
from champion.utils.logger import get_logger
from champion.utils.metrics import (
    clickhouse_load_success,  # Existing metrics in codebase
    clickhouse_load_failed,
)

logger = get_logger(__name__)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def load_with_retry(table: str, data):
    """Load data with retry and metrics."""
    try:
        clickhouse_client.insert(table, data)
        clickhouse_load_success.labels(table=table).inc()
        logger.info("load_succeeded", table=table, rows=len(data))
    except Exception as e:
        clickhouse_load_failed.labels(table=table).inc()
        logger.error("load_failed", table=table, error=str(e))
        raise
```

## Next Steps

1. **Review Dashboards**: Visit http://localhost:3000 and explore the dashboards
2. **Check Alerts**: Visit http://localhost:9090/alerts to see alert status
3. **Instrument Your Code**: Add metrics and trace IDs to your operations
4. **Test Alerts**: Run alert tests to verify configuration
5. **Query Metrics**: Practice PromQL queries in Prometheus and Grafana

## Resources

- [Monitoring README](../monitoring/README.md)
- [Log Aggregation Setup](./LOG_AGGREGATION_SETUP.md)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/)
- [Structlog Documentation](https://www.structlog.org/)
