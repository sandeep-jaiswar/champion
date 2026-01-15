"""NSE CM Bhavcopy scraper."""

import zipfile
from datetime import date
from io import BytesIO
from pathlib import Path

from champion.config import config
from champion.scrapers.base import BaseScraper
from champion.utils.logger import get_logger
from champion.utils.metrics import files_downloaded, scrape_duration

logger = get_logger(__name__)


class BhavcopyScraper(BaseScraper):
    """Scraper for NSE CM Bhavcopy files."""

    def __init__(self) -> None:
        """Initialize bhavcopy scraper."""
        super().__init__("bhavcopy")

    def scrape(self, target_date: date, dry_run: bool = False) -> Path:  # type: ignore[override]
        """Scrape bhavcopy for a specific date.

        Args:
            target_date: Date to scrape
            dry_run: If True, parse without producing to Kafka

        Returns:
            Path to extracted CSV file
        """
        with scrape_duration.labels(scraper=self.name).time():
            self.logger.info("Starting bhavcopy scrape", date=str(target_date), dry_run=dry_run)

            # Prefer organized local data lake path if present
            date_str = target_date.strftime("%Y-%m-%d")
            lake_dir = (
                config.storage.data_dir
                / "lake"
                / "intraday"
                / "bhavcopy"
                / f"trade_date={date_str}"
            )
            if lake_dir.exists() and lake_dir.is_dir():
                # Find CSV file inside
                for p in sorted(lake_dir.iterdir()):
                    if p.is_file() and p.name.lower().endswith(".csv"):
                        self.logger.info(
                            "Using local bhavcopy CSV", path=str(p), trade_date=str(target_date)
                        )
                        return p

            # Fallback to download into storage root (preserve previous behavior)
            # Format date for NSE URL (YYYYMMDD)
            date_compact = target_date.strftime("%Y%m%d")
            url = config.nse.bhavcopy_url.format(date=date_compact)

            # Download ZIP file into lake_dir if possible
            zip_path = (
                (lake_dir / f"BhavCopy_NSE_CM_{date_compact}.csv.zip")
                if lake_dir.exists()
                else config.storage.data_dir / f"BhavCopy_NSE_CM_{date_compact}.csv.zip"
            )
            csv_path = (
                (lake_dir / f"BhavCopy_NSE_CM_{date_compact}.csv")
                if lake_dir.exists()
                else config.storage.data_dir / f"BhavCopy_NSE_CM_{date_compact}.csv"
            )

            # Ensure target dir exists when writing
            if lake_dir and not lake_dir.exists():
                try:
                    lake_dir.mkdir(parents=True, exist_ok=True)
                except Exception:
                    pass

            if not self._download_and_extract_zip(url, str(zip_path), str(csv_path)):
                raise RuntimeError(f"Failed to download bhavcopy for {target_date}")

            # Only increment metrics for real downloads (not dry runs)
            if not dry_run:
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
                "Referer": "https://www.nseindia.com/",
            }
            self.logger.info("Downloading ZIP file", url=url)

            response = httpx.get(
                url, headers=headers, timeout=config.scraper.timeout, follow_redirects=True
            )
            response.raise_for_status()

            # Extract CSV from ZIP
            with zipfile.ZipFile(BytesIO(response.content), "r") as zip_file:
                # Find CSV file in ZIP
                for file_name in zip_file.namelist():
                    if file_name.endswith(".csv"):
                        with open(csv_path, "wb") as f:
                            f.write(zip_file.read(file_name))
                        self.logger.info("Extracted CSV from ZIP", csv_path=csv_path)
                        return True

            self.logger.error("No CSV file found in ZIP archive")
            return False

        except Exception as e:
            self.logger.error("Failed to download and extract ZIP", url=url, error=str(e))
            return False
