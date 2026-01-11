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
from typing import Any, Dict, List, Set

import polars as pl
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TradingCalendarParser:
    """Parser for NSE Trading Calendar JSON files."""

    def __init__(self):
        """Initialize parser."""
        self.calendar_gen = Calendar(firstweekday=0)  # Monday = 0

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
        with open(json_file_path, "r") as f:
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
                calendar_df.filter(
                    pl.col("day_type").is_in(["WEEKEND", "NORMAL_TRADING"])
                )["day_type"].count()
            ),
        )

        return calendar_df

    def _extract_holidays(self, data: Dict[str, Any], year: int) -> Dict[str, Set[date]]:
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
                            logger.debug(
                                "Parsed holiday",
                                segment=segment,
                                date=str(holiday_date),
                                description=holiday.get("description", ""),
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
        self, year: int, holidays_by_segment: Dict[str, Set[date]]
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
            # Determine if trading day
            is_weekend = current_date.weekday() in [5, 6]  # Saturday=5, Sunday=6
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
                # Find holiday name from data
                holiday_name = self._get_holiday_name(current_date, holidays_by_segment)

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

            entry = {
                "trade_date": current_date,
                "is_trading_day": is_trading_day,
                "day_type": day_type,
                "holiday_name": holiday_name,
                "exchange": "NSE",
                "year": current_date.year,
                "month": current_date.month,
                "day": current_date.day,
                "weekday": current_date.weekday(),
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

    def _get_holiday_name(
        self, target_date: date, holidays_by_segment: Dict[str, Set[date]]
    ) -> str | None:
        """Get holiday name for a date.

        This is a simple implementation. In a production system,
        you'd want to store the holiday name along with the date.

        Args:
            target_date: Date to check
            holidays_by_segment: Holiday data

        Returns:
            Holiday name or None
        """
        # Common holiday names (this should come from parsed data in production)
        # For now, return a generic name
        return "Holiday"
