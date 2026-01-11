"""Index Constituent scraper for NSE indices."""

import json
from pathlib import Path
from typing import Any

import httpx

from champion.config import config
from champion.scrapers.base import BaseScraper
from champion.utils.logger import get_logger

logger = get_logger(__name__)


class IndexConstituentScraper(BaseScraper):
    """Scraper for NSE index constituent data.

    Fetches current index membership and historical rebalance data for NSE indices
    like NIFTY50, BANKNIFTY, NIFTYMIDCAP50, etc.
    """

    # NSE Index API endpoints
    INDEX_ENDPOINTS = {
        "NIFTY50": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
        "BANKNIFTY": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20BANK",
        "NIFTYMIDCAP50": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20MIDCAP%2050",
        "NIFTYIT": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20IT",
    }

    def __init__(self) -> None:
        """Initialize index constituent scraper."""
        super().__init__("index_constituent")
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
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://www.nseindia.com/",
            }
            self._session = httpx.Client(
                headers=headers,
                timeout=config.scraper.timeout,
                follow_redirects=True,
            )
        return self._session

    def scrape(
        self,
        indices: list[str] | None = None,
        output_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, Path]:
        """Scrape index constituent data for specified indices.

        Args:
            indices: List of index names to scrape (e.g., ['NIFTY50', 'BANKNIFTY'])
                     If None, scrapes all supported indices
            output_dir: Directory to save JSON files (default: config.storage.data_dir / "indices")
            dry_run: If True, parse without producing to Kafka

        Returns:
            Dictionary mapping index name to saved file path

        Raises:
            RuntimeError: If scraping fails for any index
        """
        self.logger.info("Starting index constituent scrape", indices=indices, dry_run=dry_run)

        # Default to all supported indices
        if indices is None:
            indices = list(self.INDEX_ENDPOINTS.keys())

        # Validate requested indices
        unsupported = [idx for idx in indices if idx not in self.INDEX_ENDPOINTS]
        if unsupported:
            self.logger.warning(
                "Unsupported indices requested",
                unsupported=unsupported,
                supported=list(self.INDEX_ENDPOINTS.keys()),
            )
            indices = [idx for idx in indices if idx in self.INDEX_ENDPOINTS]

        # Set output directory
        if output_dir is None:
            output_dir = config.storage.data_dir / "indices"
        output_dir.mkdir(parents=True, exist_ok=True)

        results = {}
        session = self._get_session()

        for index_name in indices:
            try:
                data = self._scrape_index(session, index_name)
                output_path = output_dir / f"{index_name}_constituents.json"

                # Save to file
                with open(output_path, "w") as f:
                    json.dump(data, f, indent=2)

                results[index_name] = output_path

                self.logger.info(
                    "Index constituent scrape complete",
                    index=index_name,
                    output_path=str(output_path),
                    constituents=len(data.get("data", [])),
                )

            except Exception as e:
                self.logger.error("Failed to scrape index", index=index_name, error=str(e))
                # Continue with other indices
                continue

        if not results:
            raise RuntimeError("Failed to scrape any index constituents")

        self.logger.info("Index constituent scrape completed", scraped_indices=list(results.keys()))

        return results

    def _scrape_index(self, session: httpx.Client, index_name: str) -> dict[str, Any]:
        """Scrape constituent data for a single index.

        Args:
            session: HTTP client session
            index_name: Name of the index to scrape

        Returns:
            Dictionary with index metadata and constituent data

        Raises:
            RuntimeError: If request fails
        """
        url = self.INDEX_ENDPOINTS[index_name]

        self.logger.info("Fetching index data", index=index_name, url=url)

        try:
            # NSE often requires a visit to the main page first
            session.get("https://www.nseindia.com/")

            # Now fetch the actual index data
            response = session.get(url)
            response.raise_for_status()

            data = response.json()

            # Add metadata with current timestamp
            from datetime import datetime as dt

            data["index_name"] = index_name
            data["scraped_at"] = dt.now().isoformat()

            return data

        except httpx.HTTPStatusError as e:
            self.logger.error(
                "HTTP error scraping index",
                index=index_name,
                error=str(e),
                status_code=e.response.status_code,
            )
            raise RuntimeError(f"Failed to scrape {index_name}: {e}") from e
        except Exception as e:
            self.logger.error("Unexpected error scraping index", index=index_name, error=str(e))
            raise RuntimeError(f"Failed to scrape {index_name}: {e}") from e

    def close(self) -> None:
        """Close the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
