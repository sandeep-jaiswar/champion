#!/usr/bin/env python3
"""Runner script for combined NSE + BSE equity ETL pipeline.

This script runs the combined ETL flow that:
- Scrapes data from both NSE and BSE
- Deduplicates by ISIN (NSE takes priority)
- Writes to Parquet data lake
- Loads to ClickHouse

Usage:
    python run_combined_etl.py --date 2026-01-09
    python run_combined_etl.py --date 2026-01-09 --no-clickhouse
    python run_combined_etl.py --date 2026-01-09 --no-bse  # NSE only
"""

import argparse
import sys
from datetime import date, datetime

from src.orchestration.combined_flows import combined_equity_etl_flow
from src.utils.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run combined NSE + BSE equity ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--date",
        type=str,
        help="Trading date in YYYY-MM-DD format (default: yesterday)",
        default=None,
    )

    parser.add_argument(
        "--output-path",
        type=str,
        help="Base path for data lake output (default: data/lake)",
        default=None,
    )

    parser.add_argument(
        "--no-clickhouse",
        action="store_true",
        help="Skip loading to ClickHouse",
    )

    parser.add_argument(
        "--no-bse",
        action="store_true",
        help="Disable BSE scraping (NSE only)",
    )

    parser.add_argument(
        "--clickhouse-host",
        type=str,
        help="ClickHouse host (default: localhost)",
        default=None,
    )

    parser.add_argument(
        "--clickhouse-port",
        type=int,
        help="ClickHouse port (default: 8123)",
        default=None,
    )

    parser.add_argument(
        "--metrics-port",
        type=int,
        help="Prometheus metrics port (default: 9090)",
        default=9090,
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()

    # Parse trade date
    trade_date: date | None = None
    if args.date:
        try:
            trade_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logger.error("invalid_date_format", date=args.date, expected="YYYY-MM-DD")
            return 1

    logger.info(
        "starting_combined_etl",
        trade_date=str(trade_date) if trade_date else "yesterday",
        enable_bse=not args.no_bse,
        load_to_clickhouse=not args.no_clickhouse,
    )

    try:
        # Run the combined ETL flow
        result = combined_equity_etl_flow(
            trade_date=trade_date,
            output_base_path=args.output_path,
            load_to_clickhouse=not args.no_clickhouse,
            clickhouse_host=args.clickhouse_host,
            clickhouse_port=args.clickhouse_port,
            metrics_port=args.metrics_port,
            enable_bse=not args.no_bse,
        )

        logger.info("etl_complete", result=result)
        print(f"\n✅ ETL pipeline completed successfully!")
        print(f"   Trade Date: {result['trade_date']}")
        print(f"   Total Rows: {result['total_rows']}")
        print(f"   Parquet File: {result['parquet_file']}")
        print(f"   Duration: {result['flow_duration_seconds']:.2f}s")

        if result.get("load_stats"):
            print(f"   ClickHouse: {result['load_stats']['rows_loaded']} rows loaded")

        return 0

    except Exception as e:
        logger.error("etl_failed", error=str(e))
        print(f"\n❌ ETL pipeline failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
