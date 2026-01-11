"""NSE Bulk and Block Deals scraper.

Scrapes daily bulk and block deals data from NSE.

Bulk Deals: Transactions in a single scrip where total quantity traded is more than 0.5%
            of the number of equity shares of the company listed on the exchange.

Block Deals: Transactions executed through a separate trading window with minimum quantity
             of 5 lakh shares or Rs 5 crore, whichever is less.
"""

import json
from datetime import date
from io import StringIO
from pathlib import Path

import httpx
import polars as pl

from champion.config import config
from champion.scrapers.base import BaseScraper
from champion.utils.logger import get_logger
from champion.utils.metrics import files_downloaded, scrape_duration

logger = get_logger(__name__)


class BulkBlockDealsScraper(BaseScraper):
    """Scraper for NSE bulk and block deals data.

    NSE provides bulk and block deals data through their API endpoints.
    Data is available for the current day and historical dates.
    """

    # NSE API endpoints for bulk and block deals
    # Working API endpoint that returns CSV data
    BULK_DEALS_API = "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"
    BLOCK_DEALS_API = "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"
    
    # Origin URL for proper headers
    ORIGIN_URL = "https://nsewebsite-staging.nseindia.com"

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
                "Accept": "application/json, text/html, text/csv, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://www.nseindia.com/",
                "Origin": self.ORIGIN_URL,
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
                        raise RuntimeError(f"Failed to scrape bulk deals: {e}") from e

            # Scrape block deals
            if scrape_block:
                try:
                    block_path = self._scrape_block_deals(target_date, output_dir, dry_run)
                    results["block"] = block_path
                    files_downloaded.labels(scraper=f"{self.name}_block").inc()
                except Exception as e:
                    self.logger.error("Failed to scrape block deals", error=str(e))
                    if deal_type == "block":
                        raise RuntimeError(f"Failed to scrape block deals: {e}") from e

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
            output_dir: Directory to save CSV file
            dry_run: If True, don't save file

        Returns:
            Path to saved CSV file

        Raises:
            RuntimeError: If scraping fails
        """
        # Format date as DD-MM-YYYY for NSE API
        date_str = target_date.strftime("%d-%m-%Y")
        output_file = output_dir / f"bulk_deals_{target_date.strftime('%Y%m%d')}.csv"

        self.logger.info("Scraping bulk deals", date=date_str)

        try:
            session = self._get_session()

            # New API endpoint that returns CSV (brotli compressed)
            url = f"{self.BULK_DEALS_API}?optionType=bulk_deals&from={date_str}&to={date_str}&csv=true"

            response = session.get(url)
            response.raise_for_status()

            # httpx automatically decompresses brotli content
            csv_text = response.text

            # Check if response is empty
            if not csv_text or len(csv_text.strip()) == 0:
                self.logger.warning("Empty response from bulk deals API", date=date_str)
                df = pl.DataFrame()
            else:
                # Parse CSV using Polars (more efficient than Pandas)
                df = pl.read_csv(StringIO(csv_text))
                # Clean column names: remove spaces
                df = df.rename(mapping=lambda col: col.replace(' ', ''))

            # Save to file if not dry run
            if not dry_run and len(df) > 0:
                df.write_csv(output_file)
                self.logger.info(
                    "Bulk deals saved",
                    file=str(output_file),
                    deals=len(df),
                )
            elif len(df) == 0:
                self.logger.info("No bulk deals found", date=date_str)

            return output_file

        except Exception as e:
            self.logger.error("Failed to scrape bulk deals", error=str(e), url=url)
            raise RuntimeError(f"Failed to scrape bulk deals: {e}") from e

    def _scrape_block_deals(
        self,
        target_date: date,
        output_dir: Path,
        dry_run: bool = False,
    ) -> Path:
        """Scrape block deals for a specific date.

        Args:
            target_date: Date to scrape
            output_dir: Directory to save CSV file
            dry_run: If True, don't save file

        Returns:
            Path to saved CSV file

        Raises:
            RuntimeError: If scraping fails
        """
        # Format date as DD-MM-YYYY for NSE API
        date_str = target_date.strftime("%d-%m-%Y")
        output_file = output_dir / f"block_deals_{target_date.strftime('%Y%m%d')}.csv"

        self.logger.info("Scraping block deals", date=date_str)

        try:
            session = self._get_session()

            # New API endpoint that returns CSV (brotli compressed)
            url = f"{self.BLOCK_DEALS_API}?optionType=block_deals&from={date_str}&to={date_str}&csv=true"

            response = session.get(url)
            response.raise_for_status()

            # httpx automatically decompresses brotli content
            csv_text = response.text

            # Check if response is empty
            if not csv_text or len(csv_text.strip()) == 0:
                self.logger.warning("Empty response from block deals API", date=date_str)
                df = pl.DataFrame()
            else:
                # Parse CSV using Polars (more efficient than Pandas)
                df = pl.read_csv(StringIO(csv_text))
                # Clean column names: remove spaces
                df = df.rename(mapping=lambda col: col.replace(' ', ''))

            # Save to file if not dry run
            if not dry_run and len(df) > 0:
                df.write_csv(output_file)
                self.logger.info(
                    "Block deals saved",
                    file=str(output_file),
                    deals=len(df),
                )
            elif len(df) == 0:
                self.logger.info("No block deals found", date=date_str)

            return output_file

        except Exception as e:
            self.logger.error("Failed to scrape block deals", error=str(e), url=url)
            raise RuntimeError(f"Failed to scrape block deals: {e}") from e
