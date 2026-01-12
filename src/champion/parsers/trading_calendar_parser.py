"""Parser for NSE Trading Calendar JSON files.

NSE provides trading calendar/holiday data in JSON format with structure like:
{
  "CM": [
    {"tradingDate": "26-Jan-2026", "weekDay": "Monday", "description": "Republic Day", "sr_no": 1}
  ],
  "FO": [...],
  "CD": [...]
}

This parser generates a complete calendar for the year with trading/non-trading days.
"""

import json
from calendar import Calendar
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from champion.utils.logger import get_logger

logger = get_logger(__name__)

# Constants for weekend detection using Python's weekday() method
# Python weekday: Monday=0, Tuesday=1, ..., Saturday=5, Sunday=6
SATURDAY = 5
SUNDAY = 6
WEEKEND_DAYS = {SATURDAY, SUNDAY}


class TradingCalendarParser:
    """Parser for NSE Trading Calendar JSON files.

    Attributes:
        SCHEMA_VERSION: Parser schema version for tracking compatibility.
    """

    SCHEMA_VERSION = "v1.0"

    def __init__(self):
        """Initialize parser."""
        self.calendar_gen = Calendar(firstweekday=0)  # Monday = 0
        # Store holiday details for lookup
        self.holiday_details: dict[date, str] = {}

    def parse(self, json_file_path: Path, year: int) -> pl.DataFrame:
        """Parse NSE trading calendar JSON and generate complete year calendar.

        Args:
            json_file_path: Path to NSE holiday JSON file
            year: Year for calendar generation

        Returns:
            Polars DataFrame with complete trading calendar
        """
        logger.info("Parsing trading calendar", file=str(json_file_path), year=year)

        # Load holiday data
        with open(json_file_path) as f:
            data = json.load(f)

        # Extract holidays by segment
        holidays_by_segment = self._extract_holidays(data, year)

        # Generate complete calendar
        calendar_df = self._generate_calendar(year, holidays_by_segment)

        logger.info(
            "Trading calendar parsed",
            total_days=len(calendar_df),
            trading_days=calendar_df.filter(pl.col("is_trading_day"))["is_trading_day"].sum(),
            holidays=calendar_df.filter(~pl.col("is_trading_day"))["is_trading_day"]
            .count()
            .__sub__(
                calendar_df.filter(pl.col("day_type").is_in(["WEEKEND", "NORMAL_TRADING"]))[
                    "day_type"
                ].count()
            ),
        )

        return calendar_df

    def _extract_holidays(self, data: dict[str, Any], year: int) -> dict[str, set[date]]:
        """Extract holiday dates from NSE JSON response.

        Args:
            data: NSE holiday JSON data
            year: Year for parsing

        Returns:
            Dictionary mapping segment (CM, FO, CD) to set of holiday dates
        """
        holidays = {}

        for segment in ["CM", "FO", "CD"]:
            segment_holidays = set()

            if segment in data and isinstance(data[segment], list):
                for holiday in data[segment]:
                    try:
                        # Parse various date formats NSE might use
                        trading_date = holiday.get("tradingDate", "")

                        # Try multiple date formats
                        holiday_date = self._parse_date(trading_date, year)

                        if holiday_date:
                            segment_holidays.add(holiday_date)

                            # Store holiday name for later lookup
                            holiday_name = holiday.get("description", "Holiday")
                            self.holiday_details[holiday_date] = holiday_name

                            logger.debug(
                                "Parsed holiday",
                                segment=segment,
                                date=str(holiday_date),
                                description=holiday_name,
                            )

                    except Exception as e:
                        logger.warning(
                            "Failed to parse holiday",
                            segment=segment,
                            holiday=holiday,
                            error=str(e),
                        )

            holidays[segment] = segment_holidays
            logger.info(f"Extracted {len(segment_holidays)} holidays for {segment}")

        return holidays

    def _parse_date(self, date_str: str, year: int) -> date | None:
        """Parse date string in various formats.

        Args:
            date_str: Date string from NSE
            year: Year for context

        Returns:
            Parsed date or None if parsing fails
        """
        if not date_str:
            return None

        # Try different formats
        formats = [
            "%d-%b-%Y",  # 26-Jan-2026
            "%d-%B-%Y",  # 26-January-2026
            "%Y-%m-%d",  # 2026-01-26
            "%d/%m/%Y",  # 26/01/2026
            "%d-%m-%Y",  # 26-01-2026
        ]

        for fmt in formats:
            try:
                parsed = datetime.strptime(date_str, fmt).date()
                return parsed
            except ValueError:
                continue

        # If no format worked, try adding year for formats without year
        for fmt in ["%d-%b", "%d-%B", "%d/%m", "%d-%m"]:
            try:
                parsed = datetime.strptime(f"{date_str}-{year}", f"{fmt}-%Y").date()
                return parsed
            except ValueError:
                continue

        logger.warning("Could not parse date", date_str=date_str)
        return None

    def _generate_calendar(
        self, year: int, holidays_by_segment: dict[str, set[date]]
    ) -> pl.DataFrame:
        """Generate complete trading calendar for the year.

        Args:
            year: Year for calendar
            holidays_by_segment: Holiday dates by market segment

        Returns:
            DataFrame with trading calendar entries
        """
        calendar_entries = []

        # Use CM (Capital Market) segment as the primary segment
        cm_holidays = holidays_by_segment.get("CM", set())

        # Generate entry for each day of the year
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)

        current_date = start_date
        while current_date <= end_date:
            # Check if weekend using Python's weekday() method (0=Monday, 6=Sunday)
            python_weekday = current_date.weekday()
            is_weekend = python_weekday in WEEKEND_DAYS
            is_holiday = current_date in cm_holidays

            is_trading_day = not (is_weekend or is_holiday)

            # Determine day type
            if is_holiday:
                day_type = "MARKET_HOLIDAY"
            elif is_weekend:
                day_type = "WEEKEND"
            else:
                day_type = "NORMAL_TRADING"

            # Get holiday name if applicable
            holiday_name = None
            if is_holiday:
                holiday_name = self._get_holiday_name(current_date)

            # Determine segments status
            segments = [
                {
                    "segment": "CM",
                    "is_open": is_trading_day,
                },
                {
                    "segment": "FO",
                    "is_open": is_trading_day,
                },
                {
                    "segment": "CD",
                    "is_open": is_trading_day,
                },
            ]

            # Convert to ISO 8601 weekday for storage (1=Monday, 7=Sunday)
            # This matches ClickHouse's toDayOfWeek() function
            iso_weekday = current_date.isoweekday()

            entry = {
                "trade_date": current_date,
                "is_trading_day": is_trading_day,
                "day_type": day_type,
                "holiday_name": holiday_name,
                "exchange": "NSE",
                "year": current_date.year,
                "month": current_date.month,
                "day": current_date.day,
                "weekday": iso_weekday,  # Stored in ISO 8601 format for consistency
                "segments": segments,
            }

            calendar_entries.append(entry)
            current_date = date.fromordinal(current_date.toordinal() + 1)

        # Convert to DataFrame
        df = pl.DataFrame(
            {
                "trade_date": [e["trade_date"] for e in calendar_entries],
                "is_trading_day": [e["is_trading_day"] for e in calendar_entries],
                "day_type": [e["day_type"] for e in calendar_entries],
                "holiday_name": [e["holiday_name"] for e in calendar_entries],
                "exchange": [e["exchange"] for e in calendar_entries],
                "year": [e["year"] for e in calendar_entries],
                "month": [e["month"] for e in calendar_entries],
                "day": [e["day"] for e in calendar_entries],
                "weekday": [e["weekday"] for e in calendar_entries],
            }
        )

        return df

    def _get_holiday_name(self, target_date: date) -> str | None:
        """Get holiday name for a date from stored holiday details.

        Args:
            target_date: Date to check

        Returns:
            Holiday name or None
        """
        return self.holiday_details.get(target_date)
