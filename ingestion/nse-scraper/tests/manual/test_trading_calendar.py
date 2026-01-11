#!/usr/bin/env python3
"""Test script for trading calendar implementation."""

import sys
from datetime import date
from pathlib import Path

# Add parent directory to path
parent_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(parent_dir))


def test_scraper():
    """Test trading calendar scraper."""
    print("\n=== Testing Trading Calendar Scraper ===")

    from src.scrapers.trading_calendar import TradingCalendarScraper

    scraper = TradingCalendarScraper()

    try:
        # Test scraping for current year
        year = 2026
        json_path = scraper.scrape(year=year)
        print(f"✓ Scraper completed: {json_path}")

        # Check if file exists
        if not json_path.exists():
            print(f"✗ File not found: {json_path}")
            return False

        print(f"✓ File exists: {json_path}")

        # Check file size
        file_size = json_path.stat().st_size
        print(f"✓ File size: {file_size} bytes")

        scraper.close()
        return True

    except Exception as e:
        print(f"✗ Scraper failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_parser():
    """Test trading calendar parser."""
    print("\n=== Testing Trading Calendar Parser ===")

    from src.parsers.trading_calendar_parser import TradingCalendarParser

    # Check if test data exists
    test_file = Path("data/NSE_TradingCalendar_2026.json")

    if not test_file.exists():
        print(f"✗ Test data not found: {test_file}")
        print("  Run scraper first: python test_trading_calendar.py scraper")
        return False

    parser = TradingCalendarParser()

    try:
        year = 2026
        df = parser.parse(test_file, year)

        print("✓ Parser completed")
        print(f"✓ Total days: {len(df)}")
        print(f"✓ Trading days: {df.filter(df['is_trading_day'])['is_trading_day'].sum()}")
        print(f"✓ Holidays: {df.filter(df['day_type'] == 'MARKET_HOLIDAY')['day_type'].count()}")
        print(f"✓ Weekends: {df.filter(df['day_type'] == 'WEEKEND')['day_type'].count()}")

        # Show schema
        print("\n✓ DataFrame schema:")
        print(df.schema)

        # Show sample data
        print("\n✓ Sample trading days:")
        print(df.filter(df["is_trading_day"]).head(5))

        print("\n✓ Sample holidays:")
        print(df.filter(df["day_type"] == "MARKET_HOLIDAY").head(5))

        return True

    except Exception as e:
        print(f"✗ Parser failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_validator():
    """Test trading calendar validator."""
    print("\n=== Testing Trading Calendar Validator ===")

    from src.utils.trading_calendar import TradingCalendarValidator

    # Check if calendar exists
    calendar_file = Path(
        "data/lake/reference/trading_calendar/year=2026/trading_calendar_2026.parquet"
    )

    if not calendar_file.exists():
        print(f"✗ Calendar file not found: {calendar_file}")
        print("  Run full ETL first")
        return False

    validator = TradingCalendarValidator(str(calendar_file))

    try:
        # Test is_trading_day
        test_date = date(2026, 1, 26)  # Republic Day (holiday)
        is_trading = validator.is_trading_day(test_date)
        print(f"✓ Is {test_date} a trading day? {is_trading} (expected: False)")

        # Test weekday
        weekday_date = date(2026, 1, 2)  # Friday
        is_trading = validator.is_trading_day(weekday_date)
        print(f"✓ Is {weekday_date} a trading day? {is_trading} (expected: True)")

        # Test next trading day
        next_day = validator.get_next_trading_day(test_date)
        print(f"✓ Next trading day after {test_date}: {next_day}")

        # Test previous trading day
        prev_day = validator.get_previous_trading_day(test_date)
        print(f"✓ Previous trading day before {test_date}: {prev_day}")

        # Test trading days in month
        trading_days_jan = validator.count_trading_days_in_month(2026, 1)
        print(f"✓ Trading days in Jan 2026: {trading_days_jan}")

        # Test holidays in range
        holidays = validator.get_holidays_in_range(date(2026, 1, 1), date(2026, 12, 31))
        print(f"✓ Holidays in 2026: {len(holidays)}")
        print(holidays)

        return True

    except Exception as e:
        print(f"✗ Validator failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_full_etl():
    """Test full ETL pipeline."""
    print("\n=== Testing Full ETL Pipeline ===")

    try:
        from src.orchestration.trading_calendar_flow import trading_calendar_etl_flow

        year = 2026
        result = trading_calendar_etl_flow(year=year, load_to_clickhouse=False)

        print("✓ ETL completed successfully")
        print(f"  Year: {result['year']}")
        print(f"  JSON Path: {result['json_path']}")
        print(f"  Parquet Path: {result['parquet_path']}")

        return True

    except Exception as e:
        print(f"✗ ETL failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all tests or specific test."""
    if len(sys.argv) > 1:
        test_name = sys.argv[1]

        if test_name == "scraper":
            success = test_scraper()
        elif test_name == "parser":
            success = test_parser()
        elif test_name == "validator":
            success = test_validator()
        elif test_name == "etl":
            success = test_full_etl()
        else:
            print(f"Unknown test: {test_name}")
            print("Available tests: scraper, parser, validator, etl")
            return 1

        return 0 if success else 1

    # Run all tests
    print("=" * 60)
    print("Running Trading Calendar Tests")
    print("=" * 60)

    results = {
        "Scraper": test_scraper(),
        "Parser": test_parser(),
        "Validator": test_validator(),
        "Full ETL": test_full_etl(),
    }

    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)

    all_passed = True
    for test, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test}: {status}")
        if not passed:
            all_passed = False

    print("=" * 60)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
