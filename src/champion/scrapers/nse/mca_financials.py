"""MCA (Ministry of Corporate Affairs) financial statements scraper.

MCA provides financial statements through their website.
This scraper fetches quarterly/annual financial data for companies.

Note: MCA data access may require authentication or use of third-party APIs.
This implementation provides a framework that can be extended with actual data sources.

Potential data sources:
1. MCA21 Portal (requires login): https://www.mca.gov.in/mcafoportal/
2. Third-party APIs: BSE, NSE, or financial data providers
3. BSE Results API: https://www.bseindia.com/corporates/Comp_Resultsnew.aspx
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


class McaFinancialsScraper(BaseScraper):
    """Scraper for MCA/BSE financial statements."""

    def __init__(self) -> None:
        """Initialize MCA financials scraper."""
        super().__init__("mca_financials")
        self.bse_base_url = "https://www.bseindia.com"

    def scrape(
        self,
        scrip_code: str,
        from_date: date,
        to_date: date,
        dry_run: bool = False,
    ) -> Path:
        """Scrape financial statements for a specific company.

        Args:
            scrip_code: BSE scrip code (e.g., "500325" for RELIANCE)
            from_date: Start date for financial data
            to_date: End date for financial data
            dry_run: If True, parse without producing to Kafka

        Returns:
            Path to saved data file

        Raises:
            RuntimeError: If download fails
        """
        with scrape_duration.labels(scraper=self.name).time():
            self.logger.info(
                "Starting financial statements scrape",
                scrip_code=scrip_code,
                from_date=str(from_date),
                to_date=str(to_date),
                dry_run=dry_run,
            )

            # Use BSE results endpoint as proxy for financial data
            url = f"{self.bse_base_url}/corporates/Comp_Resultsnew.aspx"

            # Output path
            output_path = (
                config.storage.data_dir
                / "financials"
                / f"BSE_Financials_{scrip_code}_{from_date.strftime('%Y%m%d')}_{to_date.strftime('%Y%m%d')}.html"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if not self._download_financial_data(
                url, scrip_code, from_date, to_date, str(output_path)
            ):
                raise RuntimeError(
                    f"Failed to download financial data for {scrip_code} from {from_date} to {to_date}"
                )

            files_downloaded.labels(scraper=self.name).inc()
            return output_path

    def scrape_multiple(
        self,
        scrip_codes: list[str],
        from_date: date,
        to_date: date,
        dry_run: bool = False,
    ) -> list[Path]:
        """Scrape financial data for multiple companies.

        Args:
            scrip_codes: List of BSE scrip codes
            from_date: Start date for financial data
            to_date: End date for financial data
            dry_run: If True, parse without producing to Kafka

        Returns:
            List of paths to saved files
        """
        results = []
        for scrip_code in scrip_codes:
            try:
                path = self.scrape(scrip_code, from_date, to_date, dry_run)
                results.append(path)
                # Add delay to avoid rate limiting
                time.sleep(2)
            except Exception as e:
                self.logger.error(
                    "Failed to scrape financial data",
                    scrip_code=scrip_code,
                    error=str(e),
                )
        return results

    def _download_financial_data(
        self,
        url: str,
        scrip_code: str,
        from_date: date,
        to_date: date,
        output_path: str,
    ) -> bool:
        """Download financial data from BSE.

        Args:
            url: BSE financial results URL
            scrip_code: BSE scrip code
            from_date: Start date
            to_date: End date
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
                "fromdate": from_date.strftime("%d/%m/%Y"),
                "todate": to_date.strftime("%d/%m/%Y"),
                "Submit": "Submit",
            }

            self.logger.info(
                "Downloading financial data",
                url=url,
                scrip_code=scrip_code,
                from_date=str(from_date),
                to_date=str(to_date),
            )

            with httpx.Client(timeout=config.scraper.timeout, follow_redirects=True) as client:
                response = client.post(url, headers=headers, data=data)
                response.raise_for_status()

                # Save the HTML response
                with open(output_path, "wb") as f:
                    f.write(response.content)

                self.logger.info("Downloaded financial data", output_path=output_path)
                return True

        except Exception as e:
            self.logger.error(
                "Failed to download financial data",
                url=url,
                scrip_code=scrip_code,
                error=str(e),
            )
            return False
