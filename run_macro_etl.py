#!/usr/bin/env python3
"""Run macro indicators ETL pipeline.

Usage:
    python run_macro_etl.py
    python run_macro_etl.py --start-date 2024-01-01 --end-date 2025-12-31
    python run_macro_etl.py --no-clickhouse
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / "ingestion" / "nse-scraper"))

import structlog
import typer
from rich.console import Console

from src.orchestration.macro_flow import macro_indicators_flow

# Initialize logging
logger = structlog.get_logger()
console = Console()

app = typer.Typer(help="Macro Indicators ETL Pipeline")


@app.command()
def main(
    start_date: str = typer.Option(
        None,
        "--start-date",
        help="Start date (YYYY-MM-DD). Default: 2 years ago",
    ),
    end_date: str = typer.Option(
        None,
        "--end-date",
        help="End date (YYYY-MM-DD). Default: today",
    ),
    clickhouse: bool = typer.Option(
        True,
        "--clickhouse/--no-clickhouse",
        help="Load data to ClickHouse",
    ),
    rbi_indicators: str = typer.Option(
        None,
        "--rbi-indicators",
        help="Comma-separated list of RBI indicator codes",
    ),
    mospi_indicators: str = typer.Option(
        None,
        "--mospi-indicators",
        help="Comma-separated list of MOSPI indicator codes",
    ),
) -> None:
    """Run macro indicators ETL pipeline for RBI and MOSPI data.

    Examples:
        # Default: Last 2 years
        python run_macro_etl.py

        # Custom date range
        python run_macro_etl.py --start-date 2023-01-01 --end-date 2024-12-31

        # Skip ClickHouse load
        python run_macro_etl.py --no-clickhouse

        # Specific indicators
        python run_macro_etl.py --rbi-indicators REPO_RATE,FX_RESERVES_TOTAL
    """
    console.print("\n[bold cyan]Macro Indicators ETL Pipeline[/bold cyan]\n")

    # Parse dates
    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        # Default: 2 years ago
        start_dt = datetime.now() - timedelta(days=730)

    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        # Default: today
        end_dt = datetime.now()

    # Parse indicator lists
    rbi_list = rbi_indicators.split(",") if rbi_indicators else None
    mospi_list = mospi_indicators.split(",") if mospi_indicators else None

    console.print(f"[green]Start Date:[/green] {start_dt.strftime('%Y-%m-%d')}")
    console.print(f"[green]End Date:[/green] {end_dt.strftime('%Y-%m-%d')}")
    console.print(f"[green]Load to ClickHouse:[/green] {clickhouse}")
    console.print(f"[green]RBI Indicators:[/green] {rbi_list or 'default'}")
    console.print(f"[green]MOSPI Indicators:[/green] {mospi_list or 'default'}\n")

    try:
        # Set MLflow tracking URI
        mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        os.environ["MLFLOW_TRACKING_URI"] = mlflow_uri

        console.print("[yellow]Starting ETL flow...[/yellow]\n")

        # Run flow
        parquet_path = macro_indicators_flow(
            start_date=start_dt,
            end_date=end_dt,
            rbi_indicators=rbi_list,
            mospi_indicators=mospi_list,
            load_to_clickhouse=clickhouse,
        )

        console.print(f"\n[bold green]✓ ETL Complete![/bold green]")
        console.print(f"[green]Parquet file:[/green] {parquet_path}")

        if clickhouse:
            console.print(
                "\n[cyan]Query data in ClickHouse:[/cyan]\n"
                "  clickhouse-client --database champion_market --query \\\n"
                '    "SELECT indicator_code, indicator_date, value, unit \\\n'
                "     FROM macro_indicators \\\n"
                "     ORDER BY indicator_code, indicator_date \\\n"
                '     LIMIT 10"'
            )

    except Exception as e:
        console.print(f"\n[bold red]✗ ETL Failed![/bold red]")
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
