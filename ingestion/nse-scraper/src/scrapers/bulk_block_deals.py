"""NSE Bulk and Block Deals scraper.

Scrapes daily bulk and block deals data from NSE.

Bulk Deals: Transactions in a single scrip where total quantity traded is more than 0.5% 
            of the number of equity shares of the company listed on the exchange.
            
Block Deals: Transactions executed through a separate trading window with minimum quantity 
             of 5 lakh shares or Rs 5 crore, whichever is less.
"""

import json
from datetime import date
from pathlib import Path

import httpx

from src.config import config
from src.scrapers.base import BaseScraper
from src.utils.logger import get_logger
from src.utils.metrics import files_downloaded, scrape_duration

logger = get_logger(__name__)


class BulkBlockDealsScraper(BaseScraper):
    """Scraper for NSE bulk and block deals data.
    
    NSE provides bulk and block deals data through their API endpoints.
    Data is available for the current day and historical dates.
    """

    # NSE API endpoints for bulk and block deals
    # Based on NSE website structure at https://www.nseindia.com/report-detail/eq_security
    BULK_DEALS_API = "https://www.nseindia.com/api/historical/bulk-deals"
    BLOCK_DEALS_API = "https://www.nseindia.com/api/historical/block-deals"
    
    # Alternative endpoints (fallback)
    BULK_DEALS_ARCHIVE = "https://archives.nseindia.com/content/equities/bulk.csv"
    BLOCK_DEALS_ARCHIVE = "https://archives.nseindia.com/content/equities/block.csv"

    def __init__(self) -> None:
        """Initialize bulk/block deals scraper."""
        super().__init__("bulk_block_deals")
        self._session: httpx.Client | None = None

    def _get_session(self) -> httpx.Client:
        """Get or create an HTTP session with proper headers.
        
        NSE requires specific headers to prevent blocking.
        
        Returns:
            httpx.Client with appropriate headers
        """
        if self._session is None:
            headers = {
                "User-Agent": config.scraper.user_agent,
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://www.nseindia.com/",
                "X-Requested-With": "XMLHttpRequest",
            }
            self._session = httpx.Client(
                headers=headers,
                timeout=config.scraper.timeout,
                follow_redirects=True,
            )
            # Establish session by visiting main page
            try:
                self._session.get("https://www.nseindia.com/")
            except Exception as e:
                self.logger.warning("Failed to establish NSE session", error=str(e))
        
        return self._session

    def __enter__(self) -> "BulkBlockDealsScraper":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - close session."""
        if self._session is not None:
            self._session.close()
            self._session = None

    def scrape(
        self,
        target_date: date,
        deal_type: str = "both",
        output_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, Path]:
        """Scrape bulk and/or block deals for a specific date.

        Args:
            target_date: Date to scrape deals for
            deal_type: Type of deals to scrape - 'bulk', 'block', or 'both' (default)
            output_dir: Directory to save JSON files (default: config.storage.data_dir / "deals")
            dry_run: If True, perform scraping but don't save files

        Returns:
            Dictionary mapping deal type to saved file path

        Raises:
            RuntimeError: If scraping fails for any requested deal type
        """
        with scrape_duration.labels(scraper=self.name).time():
            self.logger.info(
                "Starting bulk/block deals scrape",
                date=str(target_date),
                deal_type=deal_type,
                dry_run=dry_run,
            )

            # Set output directory
            if output_dir is None:
                output_dir = config.storage.data_dir / "deals"
            output_dir.mkdir(parents=True, exist_ok=True)

            results = {}
            
            # Determine which deal types to scrape
            scrape_bulk = deal_type in ("bulk", "both")
            scrape_block = deal_type in ("block", "both")

            # Scrape bulk deals
            if scrape_bulk:
                try:
                    bulk_path = self._scrape_bulk_deals(target_date, output_dir, dry_run)
                    results["bulk"] = bulk_path
                    files_downloaded.labels(scraper=f"{self.name}_bulk").inc()
                except Exception as e:
                    self.logger.error("Failed to scrape bulk deals", error=str(e))
                    if deal_type == "bulk":
                        raise RuntimeError(f"Failed to scrape bulk deals: {e}")

            # Scrape block deals
            if scrape_block:
                try:
                    block_path = self._scrape_block_deals(target_date, output_dir, dry_run)
                    results["block"] = block_path
                    files_downloaded.labels(scraper=f"{self.name}_block").inc()
                except Exception as e:
                    self.logger.error("Failed to scrape block deals", error=str(e))
                    if deal_type == "block":
                        raise RuntimeError(f"Failed to scrape block deals: {e}")

            self.logger.info(
                "Bulk/block deals scrape complete",
                date=str(target_date),
                results=list(results.keys()),
            )

            return results

    def _scrape_bulk_deals(
        self,
        target_date: date,
        output_dir: Path,
        dry_run: bool = False,
    ) -> Path:
        """Scrape bulk deals for a specific date.

        Args:
            target_date: Date to scrape
            output_dir: Directory to save JSON file
            dry_run: If True, don't save file

        Returns:
            Path to saved JSON file

        Raises:
            RuntimeError: If scraping fails
        """
        date_str = target_date.strftime("%d-%m-%Y")
        output_file = output_dir / f"bulk_deals_{target_date.strftime('%Y%m%d')}.json"

        self.logger.info("Scraping bulk deals", date=date_str)

        try:
            session = self._get_session()
            
            # Try API endpoint first
            api_url = f"{self.BULK_DEALS_API}?from={date_str}&to={date_str}"
            
            response = session.get(api_url)
            response.raise_for_status()
            
            data = response.json()
            
            # NSE API returns data in 'data' key
            if isinstance(data, dict) and "data" in data:
                deals_data = data["data"]
            else:
                deals_data = data
            
            # Save to file if not dry run
            if not dry_run:
                with open(output_file, "w") as f:
                    json.dump(deals_data, f, indent=2)
                self.logger.info(
                    "Bulk deals saved",
                    file=str(output_file),
                    deals=len(deals_data) if isinstance(deals_data, list) else "unknown",
                )
            
            return output_file

        except Exception as e:
            self.logger.error("Failed to scrape bulk deals", error=str(e), url=api_url)
            raise RuntimeError(f"Failed to scrape bulk deals: {e}")

    def _scrape_block_deals(
        self,
        target_date: date,
        output_dir: Path,
        dry_run: bool = False,
    ) -> Path:
        """Scrape block deals for a specific date.

        Args:
            target_date: Date to scrape
            output_dir: Directory to save JSON file
            dry_run: If True, don't save file

        Returns:
            Path to saved JSON file

        Raises:
            RuntimeError: If scraping fails
        """
        date_str = target_date.strftime("%d-%m-%Y")
        output_file = output_dir / f"block_deals_{target_date.strftime('%Y%m%d')}.json"

        self.logger.info("Scraping block deals", date=date_str)

        try:
            session = self._get_session()
            
            # Try API endpoint first
            api_url = f"{self.BLOCK_DEALS_API}?from={date_str}&to={date_str}"
            
            response = session.get(api_url)
            response.raise_for_status()
            
            data = response.json()
            
            # NSE API returns data in 'data' key
            if isinstance(data, dict) and "data" in data:
                deals_data = data["data"]
            else:
                deals_data = data
            
            # Save to file if not dry run
            if not dry_run:
                with open(output_file, "w") as f:
                    json.dump(deals_data, f, indent=2)
                self.logger.info(
                    "Block deals saved",
                    file=str(output_file),
                    deals=len(deals_data) if isinstance(deals_data, list) else "unknown",
                )
            
            return output_file

        except Exception as e:
            self.logger.error("Failed to scrape block deals", error=str(e), url=api_url)
            raise RuntimeError(f"Failed to scrape block deals: {e}")
