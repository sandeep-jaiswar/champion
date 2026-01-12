#!/usr/bin/env python3
"""Runner script for NSE index constituent ETL pipeline.

This script provides a simple CLI interface to run the index constituent
ingestion pipeline for NSE indices like NIFTY50 and BANKNIFTY.

Usage:
    python run_index_etl.py                          # Run for NIFTY50 and BANKNIFTY (today)
    python run_index_etl.py --indices NIFTY50        # Run for NIFTY50 only
    python run_index_etl.py --no-clickhouse          # Skip ClickHouse loading
    python run_index_etl.py --date 2026-01-11        # Run for specific date
"""

import argparse
import sys
from datetime import date
from pathlib import Path

from champion.orchestration.flows import index_constituent_etl_flow


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run NSE index constituent ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Default: NIFTY50 and BANKNIFTY for today
  %(prog)s --indices NIFTY50 BANKNIFTY NIFTYIT
  %(prog)s --date 2026-01-11
  %(prog)s --no-clickhouse                    # Skip ClickHouse loading
        """,
    )

    parser.add_argument(
        "--indices",
        nargs="+",
        default=None,
        help="Index names to scrape (default: NIFTY50 BANKNIFTY)",
    )

    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Effective date in YYYY-MM-DD format (default: today)",
    )

    parser.add_argument(
        "--no-clickhouse",
        action="store_true",
        help="Skip loading data into ClickHouse",
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Parse date
    effective_date = None
    if args.date:
        try:
            effective_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"Error: Invalid date format: {args.date}")
            print("Expected format: YYYY-MM-DD")
            sys.exit(1)

    # Display configuration
    print("=" * 70)
    print("NSE Index Constituent ETL Pipeline")
    print("=" * 70)
    print(f"Indices: {args.indices or ['NIFTY50', 'BANKNIFTY']}")
    print(f"Effective Date: {effective_date or date.today()}")
    print(f"Load to ClickHouse: {not args.no_clickhouse}")
    print("=" * 70)
    print()

    try:
        # Run the ETL flow
        result = index_constituent_etl_flow(
            indices=args.indices,
            effective_date=effective_date,
            load_to_clickhouse=not args.no_clickhouse,
        )

        # Display results
        print()
        print("=" * 70)
        print("✅ ETL Pipeline Completed Successfully")
        print("=" * 70)
        print(f"Duration: {result['duration_seconds']:.2f} seconds")
        print(f"Status: {result['status']}")
        print()
        print("Results by Index:")
        for index_name, index_result in result["results"].items():
            print(f"\n  {index_name}:")
            print(f"    Constituents: {index_result['constituents']}")
            print(f"    JSON File: {index_result['json_file']}")
            print(f"    Parquet File: {index_result['parquet_file']}")
            if "rows_loaded" in index_result:
                print(f"    Rows Loaded to ClickHouse: {index_result['rows_loaded']}")
        print()

        # Query examples
        print("=" * 70)
        print("Query Examples (ClickHouse)")
        print("=" * 70)
        print(
            """
# Get current NIFTY50 constituents
clickhouse-client --query "
    SELECT symbol, company_name, weight
    FROM champion_market.index_constituent
    WHERE index_name = 'NIFTY50'
      AND action = 'ADD'
    ORDER BY symbol
"

# Count constituents by index
clickhouse-client --query "
    SELECT index_name, COUNT(*) as constituents
    FROM champion_market.index_constituent
    WHERE action = 'ADD'
    GROUP BY index_name
    ORDER BY index_name
"

# View recent changes
clickhouse-client --query "
    SELECT index_name, symbol, action, effective_date
    FROM champion_market.index_constituent
    ORDER BY effective_date DESC
    LIMIT 20
"
        """
        )

        return 0

    except Exception as e:
        print()
        print("=" * 70)
        print("❌ ETL Pipeline Failed")
        print("=" * 70)
        print(f"Error: {e}")
        print()
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
