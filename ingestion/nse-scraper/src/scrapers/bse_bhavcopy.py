"""BSE Equity Bhavcopy scraper.

BSE provides equity bhavcopy data in CSV format.
URL format: https://www.bseindia.com/download/BhavCopy/Equity/EQ{DDMMYY}_CSV.ZIP
Example: https://www.bseindia.com/download/BhavCopy/Equity/EQ090126_CSV.ZIP for 09-Jan-2026
"""

import zipfile
from datetime import date
from io import BytesIO
from pathlib import Path

from src.config import config
from src.scrapers.base import BaseScraper
from src.utils.logger import get_logger
from src.utils.metrics import files_downloaded, scrape_duration

logger = get_logger(__name__)


class BseBhavcopyScraper(BaseScraper):
    """Scraper for BSE Equity Bhavcopy files."""

    def __init__(self) -> None:
        """Initialize BSE bhavcopy scraper."""
        super().__init__("bse_bhavcopy")

    def scrape(self, target_date: date, dry_run: bool = False) -> Path:  # type: ignore[override]
        """Scrape BSE bhavcopy for a specific date.

        Args:
            target_date: Date to scrape
            dry_run: If True, parse without producing to Kafka

        Returns:
            Path to extracted CSV file

        Raises:
            RuntimeError: If download fails
        """
        with scrape_duration.labels(scraper=self.name).time():
            self.logger.info("Starting BSE bhavcopy scrape", date=str(target_date), dry_run=dry_run)

            # Format date for BSE URL (DDMMYY)
            date_str = target_date.strftime("%d%m%y")
            url = config.bse.bhavcopy_url.format(date=date_str)

            # Download ZIP file
            zip_path = (
                config.storage.data_dir
                / f"BhavCopy_BSE_EQ_{target_date.strftime('%Y%m%d')}.csv.zip"
            )
            csv_path = (
                config.storage.data_dir / f"BhavCopy_BSE_EQ_{target_date.strftime('%Y%m%d')}.csv"
            )

            if not self._download_and_extract_zip(url, str(zip_path), str(csv_path)):
                raise RuntimeError(f"Failed to download BSE bhavcopy for {target_date}")

            files_downloaded.labels(scraper=self.name).inc()
            return csv_path

    def _download_and_extract_zip(self, url: str, zip_path: str, csv_path: str) -> bool:
        """Download ZIP file from URL and extract CSV.

        Args:
            url: Source URL
            zip_path: Path to save ZIP file
            csv_path: Path to extract CSV to

        Returns:
            True if successful, False otherwise
        """
        import httpx

        try:
            headers = {
                "User-Agent": config.scraper.user_agent,
                "Accept": "application/zip, application/octet-stream, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://www.bseindia.com/",
            }
            self.logger.info("Downloading BSE ZIP file", url=url)

            response = httpx.get(
                url, headers=headers, timeout=config.scraper.timeout, follow_redirects=True
            )
            response.raise_for_status()

            # Extract CSV from ZIP
            with zipfile.ZipFile(BytesIO(response.content), "r") as zip_file:
                # Find CSV file in ZIP
                for file_name in zip_file.namelist():
                    if file_name.lower().endswith(".csv"):
                        with open(csv_path, "wb") as f:
                            f.write(zip_file.read(file_name))
                        self.logger.info("Extracted CSV from ZIP", csv_path=csv_path)
                        return True

            self.logger.error("No CSV file found in ZIP archive")
            return False

        except Exception as e:
            self.logger.error("Failed to download and extract ZIP", url=url, error=str(e))
            return False
