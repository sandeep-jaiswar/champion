"""BSE Shareholding Pattern scraper.

BSE provides shareholding pattern data through their corporate announcements section.
This scraper fetches shareholding data for companies from BSE website.

Reference URLs:
- BSE Shareholding Pattern: https://www.bseindia.com/corporates/shpSecurities.aspx
- Direct data access: https://www.bseindia.com/corporates/shpPromoterNPublic.aspx
"""

import time
from datetime import date
from pathlib import Path

import httpx

from champion.config import config
from champion.scrapers.base import BaseScraper
from champion.utils.logger import get_logger
from champion.utils.metrics import files_downloaded, scrape_duration

logger = get_logger(__name__)


class BseShareholdingScraper(BaseScraper):
    """Scraper for BSE Shareholding Pattern data."""

    def __init__(self) -> None:
        """Initialize BSE shareholding scraper."""
        super().__init__("bse_shareholding")
        self.base_url = "https://www.bseindia.com"

    def scrape(
        self,
        scrip_code: str,
        quarter_end_date: date,
        dry_run: bool = False,
    ) -> Path:
        """Scrape BSE shareholding pattern for a specific company and quarter.

        Args:
            scrip_code: BSE scrip code (e.g., "500325" for RELIANCE)
            quarter_end_date: Quarter end date for shareholding data
            dry_run: If True, parse without producing to Kafka

        Returns:
            Path to saved HTML/JSON file

        Raises:
            RuntimeError: If download fails
        """
        with scrape_duration.labels(scraper=self.name).time():
            self.logger.info(
                "Starting BSE shareholding scrape",
                scrip_code=scrip_code,
                quarter_end_date=str(quarter_end_date),
                dry_run=dry_run,
            )

            # Format dates for BSE API
            quarter_str = quarter_end_date.strftime("%d/%m/%Y")

            # BSE shareholding pattern URL
            url = f"{self.base_url}/corporates/shpPromoterNPublic.aspx"

            # Output path
            output_path = (
                config.storage.data_dir
                / "shareholding"
                / f"BSE_Shareholding_{scrip_code}_{quarter_end_date.strftime('%Y%m%d')}.html"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if not self._download_shareholding_data(url, scrip_code, quarter_str, str(output_path)):
                raise RuntimeError(
                    f"Failed to download shareholding data for {scrip_code} for {quarter_end_date}"
                )

            files_downloaded.labels(scraper=self.name).inc()
            return output_path

    def scrape_multiple(
        self,
        scrip_codes: list[str],
        quarter_end_date: date,
        dry_run: bool = False,
    ) -> list[Path]:
        """Scrape shareholding data for multiple companies.

        Args:
            scrip_codes: List of BSE scrip codes
            quarter_end_date: Quarter end date for shareholding data
            dry_run: If True, parse without producing to Kafka

        Returns:
            List of paths to saved files
        """
        results = []
        for scrip_code in scrip_codes:
            try:
                path = self.scrape(scrip_code, quarter_end_date, dry_run)
                results.append(path)
                # Add delay to avoid rate limiting
                time.sleep(2)
            except Exception as e:
                self.logger.error(
                    "Failed to scrape shareholding data",
                    scrip_code=scrip_code,
                    error=str(e),
                )
        return results

    def _download_shareholding_data(
        self, url: str, scrip_code: str, quarter_date: str, output_path: str
    ) -> bool:
        """Download shareholding data from BSE.

        Args:
            url: BSE shareholding URL
            scrip_code: BSE scrip code
            quarter_date: Quarter date in DD/MM/YYYY format
            output_path: Path to save the file

        Returns:
            True if successful, False otherwise
        """
        try:
            headers = {
                "User-Agent": config.scraper.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://www.bseindia.com/",
                "Content-Type": "application/x-www-form-urlencoded",
            }

            # Prepare POST data for BSE
            data = {
                "scripcode": scrip_code,
                "quarter": quarter_date,
                "Submit": "Submit",
            }

            self.logger.info(
                "Downloading shareholding data",
                url=url,
                scrip_code=scrip_code,
                quarter=quarter_date,
            )

            with httpx.Client(timeout=config.scraper.timeout, follow_redirects=True) as client:
                response = client.post(url, headers=headers, data=data)
                response.raise_for_status()

                # Save the HTML response
                with open(output_path, "wb") as f:
                    f.write(response.content)

                self.logger.info("Downloaded shareholding data", output_path=output_path)
                return True

        except Exception as e:
            self.logger.error(
                "Failed to download shareholding data",
                url=url,
                scrip_code=scrip_code,
                error=str(e),
            )
            return False
