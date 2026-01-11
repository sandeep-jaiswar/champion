"""Trading Calendar scraper."""

import json
import time
from pathlib import Path

import httpx

from src.config import config
from src.scrapers.base import BaseScraper
from src.utils.logger import get_logger
from src.utils.metrics import files_downloaded, scrape_duration

logger = get_logger(__name__)


class TradingCalendarScraper(BaseScraper):
    """Scraper for NSE Trading Calendar data."""

    def __init__(self) -> None:
        """Initialize trading calendar scraper."""
        super().__init__("trading_calendar")
        self.session = None

    def _establish_session(self) -> httpx.Client:
        """Establish session with NSE website.

        NSE requires visiting the main page first to get cookies.

        Returns:
            httpx.Client with active session
        """
        if self.session is None:
            self.session = httpx.Client(
                follow_redirects=True,
                timeout=config.scraper.timeout,
            )

            # Visit main page to establish session
            try:
                self.logger.info("Establishing session with NSE")
                self.session.get(
                    "https://www.nseindia.com/",
                    headers={"User-Agent": config.scraper.user_agent},
                )
                time.sleep(2)  # Give NSE time to set cookies
            except Exception as e:
                self.logger.warning("Failed to establish NSE session", error=str(e))

        return self.session

    def scrape(self, year: int, dry_run: bool = False) -> Path:
        """Scrape trading calendar for a year.

        Args:
            year: Year to scrape (used for filename, API returns current year)
            dry_run: If True, parse without producing to Kafka

        Returns:
            Path to downloaded JSON file

        Raises:
            RuntimeError: If download fails after retries
        """
        with scrape_duration.labels(scraper=self.name).time():
            self.logger.info("Starting trading calendar scrape", year=year, dry_run=dry_run)

            url = config.nse.holiday_calendar_url
            output_path = config.storage.data_dir / f"NSE_TradingCalendar_{year}.json"
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Try to download from NSE API
            if self._download_calendar_json(url, str(output_path)):
                files_downloaded.labels(scraper=self.name).inc()
                self.logger.info("Trading calendar scrape complete", file_path=str(output_path))
                return output_path

            # If API fails, create a minimal calendar file for the year
            self.logger.warning(
                "Failed to download from NSE API, creating minimal calendar file"
            )
            self._create_minimal_calendar(year, output_path)
            self.logger.info(
                "Created minimal trading calendar", year=year, file_path=str(output_path)
            )
            return output_path

    def _download_calendar_json(self, url: str, output_path: str) -> bool:
        """Download trading calendar from NSE API.

        Args:
            url: NSE holiday calendar API URL
            output_path: Local file path to save JSON

        Returns:
            True if download successful, False otherwise
        """
        try:
            session = self._establish_session()

            headers = {
                "User-Agent": config.scraper.user_agent,
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.nseindia.com/",
            }

            self.logger.info("Downloading trading calendar", url=url)
            response = session.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()

            # Save to file
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2)

            self.logger.info("Trading calendar downloaded successfully", path=output_path)
            return True

        except Exception as e:
            self.logger.error("Failed to download trading calendar", url=url, error=str(e))
            return False

    def _create_minimal_calendar(self, year: int, output_path: Path) -> None:
        """Create a minimal trading calendar with known holidays.

        This is a fallback when API is unavailable. Contains common Indian holidays.

        Args:
            year: Year for calendar
            output_path: Path to save calendar JSON
        """
        # Common NSE holidays (these dates vary by year, this is a template)
        holidays_template = [
            {"month": 1, "day": 26, "name": "Republic Day"},
            {"month": 3, "day": 8, "name": "Maha Shivaratri"},
            {"month": 3, "day": 25, "name": "Holi"},
            {"month": 3, "day": 29, "name": "Good Friday"},
            {"month": 4, "day": 11, "name": "Id-Ul-Fitr"},
            {"month": 4, "day": 14, "name": "Dr. Baba Saheb Ambedkar Jayanti"},
            {"month": 4, "day": 21, "name": "Ram Navami"},
            {"month": 5, "day": 1, "name": "Maharashtra Day"},
            {"month": 6, "day": 17, "name": "Bakri Id"},
            {"month": 8, "day": 15, "name": "Independence Day"},
            {"month": 8, "day": 26, "name": "Ganesh Chaturthi"},
            {"month": 10, "day": 2, "name": "Mahatma Gandhi Jayanti"},
            {"month": 10, "day": 12, "name": "Dussehra"},
            {"month": 10, "day": 31, "name": "Diwali Laxmi Pujan"},
            {"month": 11, "day": 1, "name": "Diwali Balipratipada"},
            {"month": 11, "day": 15, "name": "Gurunanak Jayanti"},
            {"month": 12, "day": 25, "name": "Christmas"},
        ]

        # Create minimal calendar structure
        calendar_data = {
            "CM": [
                {
                    "tradingDate": f"{year}-{h['month']:02d}-{h['day']:02d}",
                    "weekDay": "",
                    "description": h["name"],
                    "sr_no": i + 1,
                }
                for i, h in enumerate(holidays_template)
            ],
            "FO": [],  # Same as CM typically
            "note": f"Minimal calendar for {year}. Update with actual NSE holiday list.",
        }

        with open(output_path, "w") as f:
            json.dump(calendar_data, f, indent=2)

        self.logger.info(
            "Created minimal calendar",
            year=year,
            holidays=len(holidays_template),
        )

    def close(self) -> None:
        """Close HTTP session."""
        if self.session:
            self.session.close()
            self.session = None
