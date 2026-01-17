"""Scraper domain adapters.

Adapters decouple scrapers from specific implementations, enabling:
- Easy swapping of different scraper backends
- Consistent error handling
- Observability and monitoring
- Testing with mock implementations
"""

from __future__ import annotations

from abc import abstractmethod
from datetime import date
from typing import Any

import polars as pl
from tenacity import Retrying, stop_after_attempt, wait_exponential

from champion.core import (
    IntegrationError,
    Scraper,
    ValidationError,
    get_logger,
)

logger = get_logger(__name__)


class EquityScraper(Scraper):
    """Base adapter for equity data scrapers.

    Defines standard interface for scraping equity OHLC data from any source.
    Implementations: NSEEquityScraper, BSEEquityScraper
    """

    @abstractmethod
    def scrape_date(self, trade_date: date) -> pl.DataFrame:
        """Scrape equity data for a specific date.

        Args:
            trade_date: The date to scrape

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, volume
        """

    @abstractmethod
    def scrape_date_range(
        self,
        start_date: date,
        end_date: date,
        symbols: list[str] | None = None,
    ) -> pl.DataFrame:
        """Scrape equity data for a date range.

        Args:
            start_date: Start date
            end_date: End date
            symbols: Optional list of symbols to filter

        Returns:
            DataFrame with all data for the range
        """

    def scrape(self, **kwargs) -> pl.DataFrame:
        """Implements Scraper interface."""
        if "trade_date" in kwargs:
            return self.scrape_date(kwargs["trade_date"])
        elif "start_date" in kwargs and "end_date" in kwargs:
            return self.scrape_date_range(
                kwargs["start_date"],
                kwargs["end_date"],
                kwargs.get("symbols"),
            )
        else:
            raise ValueError("Must provide trade_date or start_date/end_date")

    def validate_scrape(self, data: pl.DataFrame) -> bool:
        """Validate scraped data has required columns and sanity checks."""
        required_cols = {"symbol", "date", "open", "high", "low", "close", "volume"}
        if not required_cols.issubset(set(data.columns)):
            raise ValidationError(
                f"Missing required columns. Have {data.columns}, need {required_cols}",
                recovery_hint="Check scraper implementation",
            )

        # Basic sanity checks
        if data.is_empty():
            raise ValidationError("Scraped data is empty")

        # Check OHLC logic
        invalid = data.filter(
            (pl.col("open") <= 0)
            | (pl.col("high") <= 0)
            | (pl.col("low") <= 0)
            | (pl.col("close") <= 0)
            | (pl.col("high") < pl.col("low"))
            | (pl.col("high") < pl.col("open"))
            | (pl.col("high") < pl.col("close"))
        )

        if len(invalid) > 0:
            raise ValidationError(
                f"Found {len(invalid)} rows with invalid OHLC values",
                recovery_hint="Check source data quality",
            )

        return True


class ReferenceDataScraper(Scraper):
    """Base adapter for reference data (master data, calendars).

    Implementations: SymbolMasterScraper, CorporateActionsScraper
    """

    @abstractmethod
    def scrape_latest(self) -> pl.DataFrame:
        """Scrape latest version of reference data."""

    @abstractmethod
    def scrape_as_of_date(self, as_of_date: date) -> pl.DataFrame:
        """Scrape reference data as it was on a specific date."""

    def scrape(self, **kwargs) -> pl.DataFrame:
        """Implements Scraper interface."""
        if "as_of_date" in kwargs:
            return self.scrape_as_of_date(kwargs["as_of_date"])
        return self.scrape_latest()

    def validate_scrape(self, data: pl.DataFrame) -> bool:
        """Validate reference data."""
        if data.is_empty():
            raise ValidationError("Reference data is empty")
        return True


class ScraperWithRetry:
    """Decorator for scrapers to add automatic retry logic.

    Usage:
        scraper = ScraperWithRetry(NSEEquityScraper(), max_attempts=3)
        data = scraper.scrape_date(date(2024, 1, 1))
    """

    def __init__(self, scraper: Scraper, max_attempts: int = 3, backoff_factor: float = 2.0):
        self.scraper = scraper
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor

    def scrape(self, **kwargs: Any) -> pl.DataFrame:
        """Call scraper with automatic retry."""
        for attempt in Retrying(
            stop=stop_after_attempt(self.max_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            reraise=True,
        ):
            with attempt:
                logger.info(
                    "Scraping data",
                    attempt=attempt.retry_state.attempt_number,
                    kwargs=kwargs,
                )
                return self.scraper.scrape(**kwargs)

        # This should never be reached due to reraise=True, but satisfy mypy
        raise IntegrationError(
            service="Scraper",
            message=f"Failed after {self.max_attempts} attempts",
            retryable=False,
        )
