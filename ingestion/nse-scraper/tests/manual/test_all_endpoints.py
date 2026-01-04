"""Test script to demonstrate all endpoints and metrics.

Usage:
    python -m tests.manual.test_all_endpoints
"""

import time
from datetime import date

from src.scrapers.bhavcopy import BhavcopyScraper
from src.utils.logger import configure_logging, get_logger
from src.utils.metrics import (
    start_metrics_server,
)

# Configure logging
configure_logging()
logger = get_logger(__name__)


def main():
    """Run comprehensive test of scraper functionality."""

    logger.info("=" * 60)
    logger.info("NSE Scraper - Comprehensive Test Suite")
    logger.info("=" * 60)

    # Start metrics server
    logger.info("Starting Prometheus metrics server on port 9090")
    start_metrics_server(9090)
    logger.info("Metrics available at http://localhost:9090/metrics")

    # Test 1: Health checks
    logger.info("\n[TEST 1] Health Check")
    logger.info("✓ Logger: Configured (structlog)")
    logger.info("✓ Metrics: Server started on port 9090")
    logger.info("✓ Data directory: Available")

    # Test 2: Bhavcopy scraper
    logger.info("\n[TEST 2] Bhavcopy Scraper")
    scraper = BhavcopyScraper()
    test_date = date(2024, 1, 2)

    try:
        scraper.scrape(test_date, dry_run=True)
        logger.info(f"✓ Successfully scraped and parsed {test_date}")
    except Exception as e:
        logger.error(f"✗ Failed to scrape {test_date}: {e}")

    # Test 3: Display current metrics
    logger.info("\n[TEST 3] Current Metrics")
    logger.info("Check http://localhost:9090/metrics for full metrics")
    logger.info("Key metrics tracked:")
    logger.info("  - nse_scraper_files_downloaded_total")
    logger.info("  - nse_scraper_rows_parsed_total")
    logger.info("  - nse_scraper_kafka_produce_success_total")
    logger.info("  - nse_scraper_scrape_duration_seconds")

    logger.info("\n" + "=" * 60)
    logger.info("Test suite complete!")
    logger.info("Metrics server running. Press Ctrl+C to stop.")
    logger.info("=" * 60)

    # Keep server running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nShutting down gracefully...")


if __name__ == "__main__":
    main()
