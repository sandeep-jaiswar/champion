"""Symbol Master scraper."""

from src.config import config
from src.scrapers.base import BaseScraper
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SymbolMasterScraper(BaseScraper):
    """Scraper for NSE EQUITY_L symbol master file."""

    def __init__(self):
        """Initialize symbol master scraper."""
        super().__init__("symbol_master")

    def scrape(self, dry_run: bool = False) -> None:
        """Scrape symbol master data.
        
        Args:
            dry_run: If True, parse without producing to Kafka
        """
        self.logger.info("Starting symbol master scrape", dry_run=dry_run)

        url = config.nse.equity_list_url
        local_path = config.storage.data_dir / "EQUITY_L.csv"

        if not self.download_file(url, str(local_path)):
            raise RuntimeError("Failed to download symbol master")

        self.logger.info("Symbol master scrape complete")
        
        if dry_run:
            self.logger.info("Dry run - skipped parsing and Kafka production")
