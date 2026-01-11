#!/usr/bin/env python3
"""Example: Correlation analysis between macro indicators and stock prices.

This script demonstrates how to analyze relationships between:
- CPI (inflation) and stock prices
- FX reserves and market turnover
- Repo rate changes and market volatility
"""

import glob
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import polars as pl
from rich.console import Console
from rich.table import Table

console = Console()


def analyze_cpi_stock_correlation(parquet_macro: str, parquet_ohlc: str, symbol: str = "TCS"):
    """Analyze CPI vs stock price correlation.

    Args:
        parquet_macro: Path to macro indicators parquet
        parquet_ohlc: Path to OHLC parquet
        symbol: Stock symbol to analyze
    """
    console.print(f"\n[bold cyan]CPI vs {symbol} Stock Price Analysis[/bold cyan]\n")

    # Load macro data
    df_macro = pl.read_parquet(parquet_macro)
    cpi_data = df_macro.filter(pl.col("indicator_code") == "CPI_COMBINED")

    # Aggregate to monthly
    cpi_monthly = (
        cpi_data.group_by(pl.col("indicator_date").dt.truncate("1mo").alias("month"))
        .agg(pl.col("value").mean().alias("cpi_value"))
        .sort("month")
    )

    # Load OHLC data
    df_ohlc = pl.read_parquet(parquet_ohlc)
    stock_data = df_ohlc.filter(pl.col("TckrSymb") == symbol)

    # Aggregate to monthly
    stock_monthly = (
        stock_data.group_by(pl.col("TradDt").dt.truncate("1mo").alias("month"))
        .agg(pl.col("ClsPric").mean().alias("avg_close"))
        .sort("month")
    )

    # Join and calculate changes
    joined = cpi_monthly.join(stock_monthly, on="month", how="inner")

    # Calculate month-over-month changes
    result = joined.with_columns(
        [
            (
                (pl.col("avg_close") - pl.col("avg_close").shift(1))
                / pl.col("avg_close").shift(1)
                * 100
            ).alias("price_change_pct"),
            (pl.col("cpi_value") - pl.col("cpi_value").shift(1)).alias("cpi_change"),
        ]
    )

    # Display results
    table = Table(title=f"CPI vs {symbol} Monthly Correlation")
    table.add_column("Month", style="cyan")
    table.add_column("CPI Value", style="green")
    table.add_column("CPI Change", style="yellow")
    table.add_column(f"{symbol} Price", style="green")
    table.add_column("Price Change %", style="yellow")

    for row in result.tail(12).iter_rows(named=True):
        table.add_row(
            row["month"].strftime("%Y-%m"),
            f"{row['cpi_value']:.2f}",
            f"{row['cpi_change']:.2f}" if row["cpi_change"] else "-",
            f"₹{row['avg_close']:.2f}",
            f"{row['price_change_pct']:.2f}%" if row["price_change_pct"] else "-",
        )

    console.print(table)

    # Calculate correlation
    valid_data = result.drop_nulls(subset=["price_change_pct", "cpi_change"])
    if len(valid_data) > 0:
        correlation = valid_data.select(
            pl.corr("price_change_pct", "cpi_change").alias("correlation")
        )
        console.print(
            f"\n[bold]Correlation coefficient:[/bold] {correlation['correlation'][0]:.3f}\n"
        )


def analyze_fx_reserves_trend(parquet_macro: str):
    """Analyze FX reserves trend over time.

    Args:
        parquet_macro: Path to macro indicators parquet
    """
    console.print("\n[bold cyan]Foreign Exchange Reserves Trend[/bold cyan]\n")

    # Load macro data
    df_macro = pl.read_parquet(parquet_macro)
    fx_data = df_macro.filter(pl.col("indicator_code") == "FX_RESERVES_TOTAL").sort(
        "indicator_date"
    )

    # Calculate weekly change
    fx_with_change = fx_data.with_columns(
        [
            (pl.col("value") - pl.col("value").shift(1)).alias("weekly_change"),
            ((pl.col("value") - pl.col("value").shift(1)) / pl.col("value").shift(1) * 100).alias(
                "weekly_change_pct"
            ),
        ]
    )

    # Display results
    table = Table(title="FX Reserves (USD Million) - Last 12 Weeks")
    table.add_column("Date", style="cyan")
    table.add_column("FX Reserves", style="green")
    table.add_column("Weekly Change", style="yellow")
    table.add_column("Change %", style="yellow")

    for row in fx_with_change.tail(12).iter_rows(named=True):
        change_color = "green" if row["weekly_change"] and row["weekly_change"] > 0 else "red"
        table.add_row(
            row["indicator_date"].strftime("%Y-%m-%d"),
            f"${row['value']:,.0f}M",
            (
                f"[{change_color}]{row['weekly_change']:+,.0f}M[/{change_color}]"
                if row["weekly_change"]
                else "-"
            ),
            (
                f"[{change_color}]{row['weekly_change_pct']:+.2f}%[/{change_color}]"
                if row["weekly_change_pct"]
                else "-"
            ),
        )

    console.print(table)


def analyze_policy_rates(parquet_macro: str):
    """Analyze policy rate trends.

    Args:
        parquet_macro: Path to macro indicators parquet
    """
    console.print("\n[bold cyan]RBI Policy Rates - Latest Values[/bold cyan]\n")

    # Load macro data
    df_macro = pl.read_parquet(parquet_macro)

    # Get latest values for each policy rate
    policy_rates = df_macro.filter(pl.col("indicator_category") == "POLICY_RATE")

    latest_rates = (
        policy_rates.sort("indicator_date")
        .group_by("indicator_code")
        .agg(
            [
                pl.col("indicator_name").last().alias("indicator_name"),
                pl.col("value").last().alias("value"),
                pl.col("indicator_date").last().alias("latest_date"),
                pl.col("unit").last().alias("unit"),
            ]
        )
        .sort("indicator_code")
    )

    # Display results
    table = Table(title="RBI Policy Rates")
    table.add_column("Rate", style="cyan")
    table.add_column("Value", style="green")
    table.add_column("Unit", style="yellow")
    table.add_column("As of", style="yellow")

    for row in latest_rates.iter_rows(named=True):
        table.add_row(
            row["indicator_name"],
            f"{row['value']:.2f}",
            row["unit"],
            row["latest_date"].strftime("%Y-%m-%d"),
        )

    console.print(table)


def main():
    """Run correlation analysis examples."""
    console.print("\n[bold]Macro Data Correlation Analysis Examples[/bold]\n")

    # Check if data files exist
    macro_path = "data/lake/macro/indicators"
    ohlc_path = "data/lake/normalized/equity_ohlc"

    # Find latest parquet files
    macro_files = glob.glob(f"{macro_path}/*.parquet")
    ohlc_files = glob.glob(f"{ohlc_path}/**/*.parquet", recursive=True)

    if not macro_files:
        console.print(
            "[red]No macro data found. Run:[/red] python run_macro_etl.py\n", style="bold"
        )
        return

    if not ohlc_files:
        console.print("[yellow]No OHLC data found. Some analyses will be limited.[/yellow]\n")

    # Use latest macro file
    latest_macro = sorted(macro_files)[-1]
    console.print(f"[green]Using macro data:[/green] {latest_macro}\n")

    # Run analyses
    analyze_policy_rates(latest_macro)
    analyze_fx_reserves_trend(latest_macro)

    if ohlc_files:
        # Use latest OHLC file
        latest_ohlc = sorted(ohlc_files)[-1]
        console.print(f"\n[green]Using OHLC data:[/green] {latest_ohlc}\n")
        analyze_cpi_stock_correlation(latest_macro, latest_ohlc, symbol="TCS")


if __name__ == "__main__":
    main()
