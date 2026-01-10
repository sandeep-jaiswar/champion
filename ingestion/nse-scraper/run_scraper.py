"""Simple CLI runner for bhavcopy scraper without Typer."""

import os
import sys
from datetime import date, datetime, timedelta

from src.config import config
from src.scrapers.bhavcopy import BhavcopyScraper
from src.utils.logger import configure_logging, get_logger
from src.utils.metrics import start_metrics_server


def main():
    """Run bhavcopy scraper."""
    configure_logging()
    logger = get_logger(__name__)

    # Start Prometheus metrics server
    try:
        start_metrics_server(port=config.observability.metrics_port)
        logger.info("Metrics server running", port=config.observability.metrics_port)
    except Exception as e:
        logger.error("Failed to start metrics server", error=str(e), exc_info=True)

    # Get date from env var or use yesterday
    date_str = os.getenv("SCRAPE_DATE")
    if date_str:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)

    # Get dry_run flag
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"

    logger.info("Starting bhavcopy scraper", date=str(target_date), dry_run=dry_run)

    try:
        scraper = BhavcopyScraper()
        scraper.scrape(target_date, dry_run=dry_run)

        logger.info("Successfully scraped bhavcopy", date=str(target_date))

    except Exception as e:
        logger.error("Scraper failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
