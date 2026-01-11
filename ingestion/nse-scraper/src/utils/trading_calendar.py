"""Trading calendar validation utilities.

Provides functions to check if dates are trading days and get next/previous trading days.
"""

from datetime import date, timedelta
from pathlib import Path
from typing import List, Set

import polars as pl
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Constants for weekday handling (Python convention: Monday=0, Sunday=6)
SATURDAY = 5
SUNDAY = 6
WEEKEND_DAYS = {SATURDAY, SUNDAY}


class TradingCalendarValidator:
    """Validator for trading calendar operations."""

    def __init__(self, calendar_path: str | None = None):
        """Initialize validator.

        Args:
            calendar_path: Path to trading calendar Parquet file.
                          If None, loads from default location.
        """
        self.calendar_df = None
        self.trading_days_set: Set[date] = set()

        if calendar_path:
            self.load_calendar(calendar_path)
        else:
            self._try_load_default_calendar()

    def _try_load_default_calendar(self) -> None:
        """Try to load calendar from default location."""
        try:
            # Try to find the most recent calendar file
            calendar_dir = Path("data/lake/reference/trading_calendar")

            if not calendar_dir.exists():
                logger.warning("Trading calendar directory not found", path=str(calendar_dir))
                return

            # Find all parquet files
            parquet_files = list(calendar_dir.rglob("*.parquet"))

            if not parquet_files:
                logger.warning("No trading calendar files found")
                return

            # Load the most recent one
            latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
            self.load_calendar(str(latest_file))

        except Exception as e:
            logger.warning("Failed to load default calendar", error=str(e))

    def load_calendar(self, calendar_path: str) -> None:
        """Load trading calendar from Parquet file.

        Args:
            calendar_path: Path to Parquet file
        """
        logger.info("Loading trading calendar", path=calendar_path)

        try:
            self.calendar_df = pl.read_parquet(calendar_path)

            # Build set of trading days for fast lookup
            trading_days_df = self.calendar_df.filter(pl.col("is_trading_day"))
            self.trading_days_set = set(trading_days_df["trade_date"].to_list())

            logger.info(
                "Trading calendar loaded",
                total_days=len(self.calendar_df),
                trading_days=len(self.trading_days_set),
            )

        except Exception as e:
            logger.error("Failed to load trading calendar", path=calendar_path, error=str(e))
            raise

    def is_trading_day(self, check_date: date) -> bool:
        """Check if a date is a trading day.

        Args:
            check_date: Date to check

        Returns:
            True if trading day, False otherwise
        """
        if not self.trading_days_set:
            logger.warning("Trading calendar not loaded, cannot validate")
            # Fallback: assume weekdays are trading days
            return check_date.weekday() not in WEEKEND_DAYS

        return check_date in self.trading_days_set

    def get_next_trading_day(self, from_date: date, skip_count: int = 1) -> date:
        """Get the next trading day after a given date.

        Args:
            from_date: Starting date
            skip_count: Number of trading days to skip (default: 1)

        Returns:
            Next trading day

        Raises:
            ValueError: If no trading day found within 60 days
        """
        if not self.trading_days_set:
            # Fallback: just add days and skip weekends
            next_date = from_date + timedelta(days=1)
            while next_date.weekday() in WEEKEND_DAYS:
                next_date += timedelta(days=1)
            return next_date

        found_count = 0
        current_date = from_date + timedelta(days=1)
        max_days = 60  # Search up to 60 days ahead

        for _ in range(max_days):
            if self.is_trading_day(current_date):
                found_count += 1
                if found_count == skip_count:
                    return current_date

            current_date += timedelta(days=1)

        raise ValueError(f"No trading day found within {max_days} days after {from_date}")

    def get_previous_trading_day(self, from_date: date, skip_count: int = 1) -> date:
        """Get the previous trading day before a given date.

        Args:
            from_date: Starting date
            skip_count: Number of trading days to skip back (default: 1)

        Returns:
            Previous trading day

        Raises:
            ValueError: If no trading day found within 60 days
        """
        if not self.trading_days_set:
            # Fallback: just subtract days and skip weekends
            prev_date = from_date - timedelta(days=1)
            while prev_date.weekday() in WEEKEND_DAYS:
                prev_date -= timedelta(days=1)
            return prev_date

        found_count = 0
        current_date = from_date - timedelta(days=1)
        max_days = 60  # Search up to 60 days back

        for _ in range(max_days):
            if self.is_trading_day(current_date):
                found_count += 1
                if found_count == skip_count:
                    return current_date

            current_date -= timedelta(days=1)

        raise ValueError(f"No trading day found within {max_days} days before {from_date}")

    def get_trading_days_in_range(self, start_date: date, end_date: date) -> List[date]:
        """Get all trading days in a date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of trading days in range
        """
        if self.calendar_df is None:
            logger.warning("Trading calendar not loaded")
            return []

        df = self.calendar_df.filter(
            (pl.col("trade_date") >= start_date)
            & (pl.col("trade_date") <= end_date)
            & pl.col("is_trading_day")
        )

        return df["trade_date"].to_list()

    def get_holidays_in_range(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Get all holidays in a date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            DataFrame with holiday information
        """
        if self.calendar_df is None:
            logger.warning("Trading calendar not loaded")
            return pl.DataFrame()

        df = self.calendar_df.filter(
            (pl.col("trade_date") >= start_date)
            & (pl.col("trade_date") <= end_date)
            & (pl.col("day_type") == "MARKET_HOLIDAY")
        )

        return df.select(["trade_date", "holiday_name", "day_type"])

    def count_trading_days_in_month(self, year: int, month: int) -> int:
        """Count trading days in a specific month.

        Args:
            year: Year
            month: Month (1-12)

        Returns:
            Number of trading days
        """
        if self.calendar_df is None:
            logger.warning("Trading calendar not loaded")
            return 0

        df = self.calendar_df.filter(
            (pl.col("year") == year) & (pl.col("month") == month) & pl.col("is_trading_day")
        )

        return len(df)


# Convenience functions using a global instance
_global_validator: TradingCalendarValidator | None = None


def get_validator() -> TradingCalendarValidator:
    """Get global trading calendar validator instance."""
    global _global_validator

    if _global_validator is None:
        _global_validator = TradingCalendarValidator()

    return _global_validator


def is_trading_day(check_date: date) -> bool:
    """Check if a date is a trading day.

    Args:
        check_date: Date to check

    Returns:
        True if trading day, False otherwise
    """
    return get_validator().is_trading_day(check_date)


def get_next_trading_day(from_date: date) -> date:
    """Get the next trading day after a given date.

    Args:
        from_date: Starting date

    Returns:
        Next trading day
    """
    return get_validator().get_next_trading_day(from_date)


def get_previous_trading_day(from_date: date) -> date:
    """Get the previous trading day before a given date.

    Args:
        from_date: Starting date

    Returns:
        Previous trading day
    """
    return get_validator().get_previous_trading_day(from_date)
