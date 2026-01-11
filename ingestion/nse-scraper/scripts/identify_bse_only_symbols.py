#!/usr/bin/env python3
"""Script to identify symbols unique to BSE (not listed on NSE).

This script compares ISINs from BSE and NSE data to find:
- Symbols listed only on BSE
- Overlapping symbols (listed on both exchanges)
- Statistics on data coverage

Usage:
    python scripts/identify_bse_only_symbols.py --date 2026-01-09
    python scripts/identify_bse_only_symbols.py --clickhouse  # Query from ClickHouse
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.utils.logger import get_logger

logger = get_logger(__name__)


def identify_from_parquet(trade_date: date, base_path: Path = Path("data/lake")) -> dict:
    """Identify BSE-only symbols from Parquet files.

    Args:
        trade_date: Trading date to analyze
        base_path: Base path for data lake

    Returns:
        Dictionary with analysis results
    """
    logger.info("identifying_bse_only_symbols_from_parquet", trade_date=str(trade_date))

    # Construct paths for NSE and BSE parquet files
    year = trade_date.year
    month = trade_date.month
    day = trade_date.day

    partition_path = (
        base_path / "normalized" / "equity_ohlc" / f"year={year}" / f"month={month:02d}" / f"day={day:02d}"
    )

    if not partition_path.exists():
        logger.error("partition_not_found", path=str(partition_path))
        return {"error": "No data found for the specified date"}

    # Read all parquet files for this date
    try:
        df = pl.read_parquet(partition_path / "*.parquet")
    except Exception as e:
        logger.error("failed_to_read_parquet", error=str(e))
        return {"error": f"Failed to read data: {e}"}

    # Separate NSE and BSE data by source
    nse_df = df.filter(pl.col("source") == "nse_cm_bhavcopy")
    bse_df = df.filter(pl.col("source") == "bse_eq_bhavcopy")

    # Get ISINs from each exchange
    nse_isins = set(nse_df.filter(pl.col("ISIN").is_not_null())["ISIN"].unique().to_list())
    bse_isins = set(bse_df.filter(pl.col("ISIN").is_not_null())["ISIN"].unique().to_list())

    # Calculate overlaps
    overlapping_isins = nse_isins.intersection(bse_isins)
    bse_only_isins = bse_isins - nse_isins
    nse_only_isins = nse_isins - bse_isins

    # Get BSE-only symbols
    bse_only_symbols = (
        bse_df.filter(pl.col("ISIN").is_in(list(bse_only_isins)))
        .select(["TckrSymb", "ISIN", "FinInstrmNm", "ClsPric", "TtlTradgVol"])
        .unique(subset=["ISIN"])
        .sort("TtlTradgVol", descending=True)
    )

    results = {
        "trade_date": str(trade_date),
        "total_nse_symbols": len(nse_df),
        "total_bse_symbols": len(bse_df),
        "nse_unique_isins": len(nse_isins),
        "bse_unique_isins": len(bse_isins),
        "overlapping_isins": len(overlapping_isins),
        "bse_only_isins": len(bse_only_isins),
        "nse_only_isins": len(nse_only_isins),
        "bse_only_symbols": bse_only_symbols.to_dicts(),
    }

    logger.info("identification_complete", **{k: v for k, v in results.items() if k != "bse_only_symbols"})

    return results


def identify_from_clickhouse(
    trade_date: date | None = None,
    host: str = "localhost",
    port: int = 8123,
    user: str = "champion_user",
    password: str = "champion_pass",
    database: str = "champion_market",
) -> dict:
    """Identify BSE-only symbols from ClickHouse.

    Args:
        trade_date: Trading date to analyze (None for all data)
        host: ClickHouse host
        port: ClickHouse port
        user: ClickHouse user
        password: ClickHouse password
        database: ClickHouse database

    Returns:
        Dictionary with analysis results
    """
    logger.info("identifying_bse_only_symbols_from_clickhouse", trade_date=str(trade_date) if trade_date else "all")

    try:
        import clickhouse_connect
    except ImportError:
        logger.error("clickhouse_not_installed")
        return {"error": "clickhouse-connect not installed"}

    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
        )

        # Query to get symbol counts by source
        date_filter = f"AND TradDt = '{trade_date}'" if trade_date else ""

        query = f"""
        SELECT
            source,
            COUNT(*) as total_records,
            COUNT(DISTINCT ISIN) as unique_isins,
            COUNT(DISTINCT TckrSymb) as unique_symbols
        FROM normalized_equity_ohlc
        WHERE 1=1 {date_filter}
        GROUP BY source
        ORDER BY source
        """

        result = client.query(query)
        source_stats = result.result_rows

        # Query to identify BSE-only symbols
        bse_only_query = f"""
        SELECT
            TckrSymb as symbol,
            ISIN as isin,
            FinInstrmNm as name,
            ClsPric as close_price,
            TtlTradgVol as volume
        FROM normalized_equity_ohlc
        WHERE source = 'bse_eq_bhavcopy'
          AND ISIN IS NOT NULL
          {date_filter}
          AND ISIN NOT IN (
              SELECT DISTINCT ISIN
              FROM normalized_equity_ohlc
              WHERE source = 'nse_cm_bhavcopy'
                AND ISIN IS NOT NULL
                {date_filter}
          )
        GROUP BY symbol, isin, name, close_price, volume
        ORDER BY volume DESC
        LIMIT 100
        """

        bse_only_result = client.query(bse_only_query)
        bse_only_symbols = bse_only_result.result_rows

        client.close()

        results = {
            "trade_date": str(trade_date) if trade_date else "all_dates",
            "source_statistics": [
                {
                    "source": row[0],
                    "total_records": row[1],
                    "unique_isins": row[2],
                    "unique_symbols": row[3],
                }
                for row in source_stats
            ],
            "bse_only_symbols": [
                {
                    "symbol": row[0],
                    "isin": row[1],
                    "name": row[2],
                    "close_price": row[3],
                    "volume": row[4],
                }
                for row in bse_only_symbols
            ],
        }

        logger.info("identification_complete_clickhouse", num_bse_only=len(bse_only_symbols))

        return results

    except Exception as e:
        logger.error("clickhouse_query_failed", error=str(e))
        return {"error": f"ClickHouse query failed: {e}"}


def print_results(results: dict) -> None:
    """Print results in a readable format.

    Args:
        results: Analysis results dictionary
    """
    if "error" in results:
        print(f"\n❌ Error: {results['error']}\n")
        return

    print(f"\n{'='*80}")
    print(f"BSE-Only Symbol Analysis - {results['trade_date']}")
    print(f"{'='*80}\n")

    # Print from Parquet analysis
    if "total_nse_symbols" in results:
        print("Data Coverage Statistics:")
        print(f"  NSE Total Symbols:      {results['total_nse_symbols']:,}")
        print(f"  BSE Total Symbols:      {results['total_bse_symbols']:,}")
        print(f"  NSE Unique ISINs:       {results['nse_unique_isins']:,}")
        print(f"  BSE Unique ISINs:       {results['bse_unique_isins']:,}")
        print(f"  Overlapping ISINs:      {results['overlapping_isins']:,}")
        print(f"  BSE-Only ISINs:         {results['bse_only_isins']:,}")
        print(f"  NSE-Only ISINs:         {results['nse_only_isins']:,}")
        print()

    # Print from ClickHouse analysis
    if "source_statistics" in results:
        print("Source Statistics (ClickHouse):")
        for stat in results["source_statistics"]:
            print(f"  {stat['source']}:")
            print(f"    Total Records:    {stat['total_records']:,}")
            print(f"    Unique ISINs:     {stat['unique_isins']:,}")
            print(f"    Unique Symbols:   {stat['unique_symbols']:,}")
        print()

    # Print BSE-only symbols
    bse_only = results.get("bse_only_symbols", [])
    if bse_only:
        print(f"BSE-Only Symbols (Top {min(len(bse_only), 20)}):")
        print(f"{'Symbol':<15} {'ISIN':<15} {'Name':<40} {'Close':>10} {'Volume':>15}")
        print(f"{'-'*100}")

        for i, symbol in enumerate(bse_only[:20], 1):
            name = symbol.get("name") or symbol.get("FinInstrmNm", "")
            close = symbol.get("close_price") or symbol.get("ClsPric", 0)
            volume = symbol.get("volume") or symbol.get("TtlTradgVol", 0)

            print(
                f"{symbol.get('symbol') or symbol.get('TckrSymb', ''):<15} "
                f"{symbol.get('isin') or symbol.get('ISIN', ''):<15} "
                f"{name[:38]:<40} "
                f"{close:>10.2f} "
                f"{volume:>15,}"
            )
        print()
    else:
        print("No BSE-only symbols found.\n")

    print(f"{'='*80}\n")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Identify symbols unique to BSE",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--date",
        type=str,
        help="Trading date in YYYY-MM-DD format (default: yesterday)",
        default=None,
    )

    parser.add_argument(
        "--clickhouse",
        action="store_true",
        help="Query from ClickHouse instead of Parquet files",
    )

    parser.add_argument(
        "--base-path",
        type=str,
        help="Base path for data lake (default: data/lake)",
        default="data/lake",
    )

    parser.add_argument(
        "--clickhouse-host",
        type=str,
        help="ClickHouse host (default: localhost)",
        default="localhost",
    )

    args = parser.parse_args()

    # Parse trade date
    trade_date: date | None = None
    if args.date:
        try:
            trade_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logger.error("invalid_date_format", date=args.date, expected="YYYY-MM-DD")
            return 1
    else:
        from datetime import timedelta

        trade_date = date.today() - timedelta(days=1)

    # Run analysis
    if args.clickhouse:
        results = identify_from_clickhouse(
            trade_date=trade_date,
            host=args.clickhouse_host,
        )
    else:
        results = identify_from_parquet(
            trade_date=trade_date,
            base_path=Path(args.base_path),
        )

    # Print results
    print_results(results)

    return 0 if "error" not in results else 1


if __name__ == "__main__":
    sys.exit(main())
