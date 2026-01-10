"""Prometheus metrics for monitoring."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Scraper metrics
files_downloaded = Counter(
    "nse_scraper_files_downloaded_total",
    "Total number of files downloaded",
    ["scraper"],
)

rows_parsed = Counter(
    "nse_scraper_rows_parsed_total",
    "Total number of rows parsed",
    ["scraper", "status"],
)

kafka_produce_success = Counter(
    "nse_scraper_kafka_produce_success_total",
    "Total successful Kafka produces",
    ["topic"],
)

kafka_produce_failed = Counter(
    "nse_scraper_kafka_produce_failed_total",
    "Total failed Kafka produces",
    ["topic"],
)

scrape_duration = Histogram(
    "nse_scraper_scrape_duration_seconds",
    "Time spent scraping",
    ["scraper"],
)

last_successful_scrape = Gauge(
    "nse_scraper_last_successful_scrape_timestamp",
    "Timestamp of last successful scrape",
    ["scraper"],
)

# Pipeline metrics
parquet_write_success = Counter(
    "nse_pipeline_parquet_write_success_total",
    "Total successful Parquet writes",
    ["table"],
)

parquet_write_failed = Counter(
    "nse_pipeline_parquet_write_failed_total",
    "Total failed Parquet writes",
    ["table"],
)

clickhouse_load_success = Counter(
    "nse_pipeline_clickhouse_load_success_total",
    "Total successful ClickHouse loads",
    ["table"],
)

clickhouse_load_failed = Counter(
    "nse_pipeline_clickhouse_load_failed_total",
    "Total failed ClickHouse loads",
    ["table"],
)

flow_duration = Histogram(
    "nse_pipeline_flow_duration_seconds",
    "Time spent executing complete ETL flow",
    ["flow_name", "status"],
)


def start_metrics_server(port: int = 9090) -> None:
    """Start Prometheus metrics HTTP server.

    Args:
        port: Port to expose metrics on
    """
    start_http_server(port)
