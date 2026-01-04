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


def start_metrics_server(port: int = 9090) -> None:
    """Start Prometheus metrics HTTP server.

    Args:
        port: Port to expose metrics on
    """
    start_http_server(port)
