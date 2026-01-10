"""Simple CLI runner for bhavcopy scraper without Typer."""

import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent))

from src.config import config
from src.utils.logger import configure_logging, get_logger
from src.utils.metrics import start_metrics_server
from src.scrapers.bhavcopy import BhavcopyScraper

logger = get_logger(__name__)


def main():
    """Run bhavcopy scraper."""
    configure_logging()
    # Start Prometheus metrics server
    try:
        start_metrics_server(port=config.observability.metrics_port)
        print(f"📈 Metrics server running on port {config.observability.metrics_port}")
    except Exception as e:
        print(f"⚠️  Failed to start metrics server: {e}")
    
    # Get date from env var or use yesterday
    date_str = os.getenv("SCRAPE_DATE")
    if date_str:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)
    
    # Get dry_run flag
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
    
    logger.info("Starting bhavcopy scraper", date=str(target_date), dry_run=dry_run)
    print(f"🚀 NSE Bhavcopy Scraper")
    print(f"   Date: {target_date}")
    print(f"   Dry Run: {dry_run}")
    print()
    
    try:
        scraper = BhavcopyScraper()
        scraper.scrape(target_date, dry_run=dry_run)
        
        print(f"\n✅ Successfully scraped bhavcopy for {target_date}")
        logger.info("Scraper completed successfully")
        
    except Exception as e:
        print(f"\n❌ Scraper failed: {e}")
        logger.error("Scraper failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
