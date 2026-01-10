"""Manual test to verify pipeline metrics are exposed correctly.

This script:
1. Starts the metrics server
2. Simulates pipeline operations
3. Shows the metrics at http://localhost:9090/metrics

Usage:
    python -m tests.manual.test_pipeline_metrics
"""

import time
from pathlib import Path

from src.utils.logger import configure_logging, get_logger
from src.utils.metrics import (
    clickhouse_load_failed,
    clickhouse_load_success,
    flow_duration,
    parquet_write_failed,
    parquet_write_success,
    start_metrics_server,
)

configure_logging()
logger = get_logger(__name__)


def simulate_pipeline_metrics():
    """Simulate pipeline operations to generate metrics."""
    logger.info("Simulating pipeline operations...")

    # Simulate 3 successful parquet writes
    for i in range(3):
        parquet_write_success.labels(table="normalized_equity_ohlc").inc()
        logger.info(f"Simulated parquet write success #{i + 1}")
        time.sleep(0.5)

    # Simulate 1 failed parquet write
    parquet_write_failed.labels(table="normalized_equity_ohlc").inc()
    logger.info("Simulated parquet write failure")
    time.sleep(0.5)

    # Simulate 2 successful ClickHouse loads
    for i in range(2):
        clickhouse_load_success.labels(table="normalized_equity_ohlc").inc()
        logger.info(f"Simulated ClickHouse load success #{i + 1}")
        time.sleep(0.5)

    # Simulate 1 failed ClickHouse load
    clickhouse_load_failed.labels(table="normalized_equity_ohlc").inc()
    logger.info("Simulated ClickHouse load failure")
    time.sleep(0.5)

    # Simulate flow durations
    flow_duration.labels(flow_name="nse-bhavcopy-etl", status="success").observe(125.5)
    logger.info("Simulated successful flow duration: 125.5s")
    time.sleep(0.5)

    flow_duration.labels(flow_name="nse-bhavcopy-etl", status="success").observe(98.3)
    logger.info("Simulated successful flow duration: 98.3s")
    time.sleep(0.5)

    flow_duration.labels(flow_name="nse-bhavcopy-etl", status="failed").observe(45.2)
    logger.info("Simulated failed flow duration: 45.2s")

    logger.info("Pipeline simulation complete!")


def main():
    """Start metrics server and simulate pipeline operations."""
    # Start metrics server
    logger.info("Starting Prometheus metrics server on port 9090")
    start_metrics_server(9090)

    logger.info("Metrics server started!")
    logger.info("View metrics at: http://localhost:9090/metrics")
    logger.info("")

    # Simulate some pipeline operations
    simulate_pipeline_metrics()

    logger.info("")
    logger.info("=" * 80)
    logger.info("Metrics Summary:")
    logger.info("=" * 80)
    logger.info("✓ parquet_write_success: 3 (for normalized_equity_ohlc)")
    logger.info("✗ parquet_write_failed: 1 (for normalized_equity_ohlc)")
    logger.info("✓ clickhouse_load_success: 2 (for normalized_equity_ohlc)")
    logger.info("✗ clickhouse_load_failed: 1 (for normalized_equity_ohlc)")
    logger.info("⏱ flow_duration: 2 successful (125.5s, 98.3s), 1 failed (45.2s)")
    logger.info("=" * 80)
    logger.info("")
    logger.info("Press Ctrl+C to stop the metrics server")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down metrics server")


if __name__ == "__main__":
    main()
