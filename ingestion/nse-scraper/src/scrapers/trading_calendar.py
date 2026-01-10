"""Trading Calendar scraper."""

from src.scrapers.base import BaseScraper


class TradingCalendarScraper(BaseScraper):
    """Scraper for NSE Trading Calendar data."""

    def __init__(self) -> None:
        """Initialize trading calendar scraper."""
        super().__init__("trading_calendar")

    def scrape(self, year: int, dry_run: bool = False) -> None:
        """Scrape trading calendar for a year.

        Args:
            year: Year to scrape
            dry_run: If True, parse without producing to Kafka
        """
        self.logger.info("Starting trading calendar scrape", year=year, dry_run=dry_run)

        # Note: Trading calendar typically requires manual maintenance
        self.logger.warning("Trading calendar scraper requires manual file download")
        self.logger.info("Trading calendar scrape complete")
