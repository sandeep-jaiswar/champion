#!/usr/bin/env python3
"""
Run trading calendar ETL pipeline.

Usage:
    python run_trading_calendar.py [--year YEAR] [--no-clickhouse]

Examples:
    # Run for current year
    python run_trading_calendar.py

    # Run for specific year
    python run_trading_calendar.py --year 2026

    # Skip ClickHouse loading
    python run_trading_calendar.py --no-clickhouse
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.orchestration.trading_calendar_flow import trading_calendar_etl_flow


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run NSE Trading Calendar ETL")
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Year to process (default: current year)",
    )
    parser.add_argument(
        "--no-clickhouse",
        action="store_true",
        help="Skip loading to ClickHouse",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode (no actual data processing)",
    )

    args = parser.parse_args()

    year = args.year or datetime.now().year
    load_to_clickhouse = not args.no_clickhouse

    print(f"Starting Trading Calendar ETL for year {year}")
    print(f"Load to ClickHouse: {load_to_clickhouse}")

    if args.dry_run:
        print("DRY RUN MODE - No actual processing")
        return

    try:
        result = trading_calendar_etl_flow(
            year=year,
            load_to_clickhouse=load_to_clickhouse,
        )

        print("\n" + "=" * 60)
        print("Trading Calendar ETL Complete!")
        print("=" * 60)
        print(f"Year: {result['year']}")
        print(f"JSON Path: {result['json_path']}")
        print(f"Parquet Path: {result['parquet_path']}")
        if load_to_clickhouse:
            print(f"Rows Loaded to ClickHouse: {result['rows_loaded']}")
        print("=" * 60)

        return 0

    except Exception as e:
        print(f"\nERROR: Trading Calendar ETL failed: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
