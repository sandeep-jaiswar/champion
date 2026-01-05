"""Base scraper class."""

from abc import ABC, abstractmethod
from typing import Any

from src.utils.logger import get_logger

logger = get_logger(__name__)


class BaseScraper(ABC):
    """Base class for all scrapers."""

    def __init__(self, name: str):
        """Initialize scraper.

        Args:
            name: Scraper name for logging/metrics
        """
        self.name = name
        self.logger = get_logger(f"{__name__}.{name}")

    @abstractmethod
    def scrape(self, *args: Any, **kwargs: Any) -> None:
        """Scrape data from source.

        Must be implemented by subclasses.
        """
        pass

    def download_file(self, url: str, local_path: str) -> bool:
        """Download file from URL.

        Args:
            url: Source URL
            local_path: Local file path to save

        Returns:
            True if download successful, False otherwise
        """
        import httpx

        from src.config import config
        from src.utils.retry import retry_on_network_error

        @retry_on_network_error(max_attempts=config.scraper.retry_attempts)
        def _download() -> None:
            headers = {"User-Agent": config.scraper.user_agent}
            with httpx.stream(
                "GET", url, headers=headers, timeout=config.scraper.timeout, follow_redirects=True
            ) as response:
                response.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)

        try:
            self.logger.info("Downloading file", url=url, path=local_path)
            _download()
            self.logger.info("File downloaded successfully", path=local_path)
            return True
        except Exception as e:
            self.logger.error("Download failed", url=url, error=str(e))
            return False
