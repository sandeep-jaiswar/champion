#!/usr/bin/env python3
"""Runner script for NSE bulk and block deals ETL pipeline.

This script orchestrates the complete ETL pipeline:
1. Scrapes bulk and block deals from NSE
2. Parses and normalizes the data
3. Writes to partitioned Parquet files
4. Loads into ClickHouse for analytics

Usage:
    # Run for yesterday (default)
    python run_bulk_block_deals.py

    # Run for specific date
    python run_bulk_block_deals.py --date 2026-01-10

    # Run for date range
    python run_bulk_block_deals.py --start-date 2025-12-01 --end-date 2025-12-31

    # Run for specific deal type
    python run_bulk_block_deals.py --deal-type bulk
    python run_bulk_block_deals.py --deal-type block

    # Skip ClickHouse loading
    python run_bulk_block_deals.py --no-clickhouse
"""

import sys
from datetime import date, timedelta
from pathlib import Path

import structlog
import typer

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.orchestration.bulk_block_deals_flow import (
    bulk_block_deals_date_range_etl_flow,
    bulk_block_deals_etl_flow,
)

logger = structlog.get_logger()

app = typer.Typer(help="NSE Bulk and Block Deals ETL Pipeline")


@app.command()
def run(
    target_date: str = typer.Option(
        None,
        "--date",
        "-d",
        help="Target date in YYYY-MM-DD format (default: yesterday)",
    ),
    start_date: str = typer.Option(
        None,
        "--start-date",
        help="Start date for date range in YYYY-MM-DD format",
    ),
    end_date: str = typer.Option(
        None,
        "--end-date",
        help="End date for date range in YYYY-MM-DD format",
    ),
    deal_type: str = typer.Option(
        "both",
        "--deal-type",
        "-t",
        help="Type of deals: 'bulk', 'block', or 'both'",
    ),
    no_clickhouse: bool = typer.Option(
        False,
        "--no-clickhouse",
        help="Skip loading data to ClickHouse",
    ),
    output_base_path: str = typer.Option(
        "data/lake",
        "--output",
        "-o",
        help="Base path for Parquet output",
    ),
) -> None:
    """Run NSE bulk and block deals ETL pipeline."""
    
    # Validate deal type
    if deal_type not in ["bulk", "block", "both"]:
        typer.echo(f"Error: Invalid deal type '{deal_type}'. Must be 'bulk', 'block', or 'both'.")
        raise typer.Exit(code=1)

    load_to_clickhouse = not no_clickhouse

    try:
        # Date range mode
        if start_date and end_date:
            typer.echo(f"Running bulk/block deals ETL for date range: {start_date} to {end_date}")
            typer.echo(f"Deal type: {deal_type}")
            typer.echo(f"Load to ClickHouse: {load_to_clickhouse}")

            result = bulk_block_deals_date_range_etl_flow(
                start_date=start_date,
                end_date=end_date,
                deal_type=deal_type,
                load_to_clickhouse=load_to_clickhouse,
                output_base_path=output_base_path,
            )

            typer.echo("\n✅ Date range ETL complete!")
            typer.echo(f"Dates processed: {len(result['dates_processed'])}")
            typer.echo(f"Total events: {result['total_events']}")
            typer.echo(f"Total ClickHouse rows: {result['total_clickhouse_rows']}")
            
            if result["failed_dates"]:
                typer.echo(f"\n⚠️  Failed dates: {len(result['failed_dates'])}")
                for failed in result["failed_dates"]:
                    typer.echo(f"  - {failed['date']}: {failed['error']}")

        # Single date mode
        else:
            if target_date is None:
                target_date = (date.today() - timedelta(days=1)).isoformat()

            typer.echo(f"Running bulk/block deals ETL for: {target_date}")
            typer.echo(f"Deal type: {deal_type}")
            typer.echo(f"Load to ClickHouse: {load_to_clickhouse}")

            result = bulk_block_deals_etl_flow(
                target_date=target_date,
                deal_type=deal_type,
                load_to_clickhouse=load_to_clickhouse,
                output_base_path=output_base_path,
            )

            typer.echo("\n✅ ETL complete!")
            typer.echo(f"Deal types processed: {', '.join(result['deal_types_processed'])}")
            typer.echo(f"Total events: {result['total_events']}")
            typer.echo(f"Parquet files: {len(result['parquet_files'])}")
            typer.echo(f"ClickHouse rows loaded: {result['clickhouse_rows']}")

            if result['parquet_files']:
                typer.echo("\nOutput files:")
                for file in result['parquet_files']:
                    typer.echo(f"  - {file}")

    except Exception as e:
        logger.error("bulk_block_deals_etl_failed", error=str(e))
        typer.echo(f"\n❌ Error: {e}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
