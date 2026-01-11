#!/usr/bin/env python3
"""Runner script for fundamentals data ingestion (quarterly financials and shareholding patterns).

This script demonstrates:
1. Generating sample data (or scraping real data)
2. Parsing and computing KPIs
3. Storing in Parquet data lake
4. Loading into ClickHouse
5. Computing PE ratios by joining with OHLC data
"""

import sys
from datetime import date, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

import polars as pl

from champion.config import config
from champion.parsers.quarterly_financials_parser import QuarterlyFinancialsParser, compute_pe_ratio
from champion.parsers.shareholding_parser import ShareholdingPatternParser
from champion.utils.generate_fundamentals_sample import (
    generate_quarterly_financials_sample,
    generate_shareholding_pattern_sample,
)
from champion.utils.logger import get_logger

logger = get_logger(__name__)


# Top 50+ NSE companies for sample data generation
NIFTY50_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "HINDUNILVR",
    "ICICIBANK", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK",
    "LT", "AXISBANK", "ASIANPAINT", "MARUTI", "TITAN",
    "BAJFINANCE", "HCLTECH", "WIPRO", "ULTRACEMCO", "SUNPHARMA",
    "TECHM", "NESTLEIND", "TATAMOTORS", "M&M", "POWERGRID",
    "NTPC", "ONGC", "TATASTEEL", "ADANIPORTS", "BAJAJFINSV",
    "JSWSTEEL", "COALINDIA", "GRASIM", "BRITANNIA", "DIVISLAB",
    "EICHERMOT", "BPCL", "DRREDDY", "CIPLA", "SHREECEM",
    "INDUSINDBK", "UPL", "TATACONSUM", "APOLLOHOSP", "HINDALCO",
    "ADANIENT", "HEROMOTOCO", "SBILIFE", "HDFCLIFE", "BAJAJ-AUTO",
]


def run_fundamentals_etl(
    symbols: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    load_to_clickhouse: bool = True,
    compute_pe: bool = True,
) -> None:
    """Run the fundamentals data ETL pipeline.

    Args:
        symbols: List of symbols to process (default: NIFTY50)
        start_date: Start date for data (default: 2 years ago)
        end_date: End date for data (default: today)
        load_to_clickhouse: Whether to load data into ClickHouse
        compute_pe: Whether to compute PE ratios
    """
    # Set defaults
    if symbols is None:
        symbols = NIFTY50_SYMBOLS
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=730)  # 2 years

    logger.info(
        "Starting fundamentals ETL",
        symbols=len(symbols),
        start_date=str(start_date),
        end_date=str(end_date),
    )

    # Create output directories
    data_dir = config.storage.data_dir
    financials_dir = data_dir / "lake" / "normalized" / "quarterly_financials"
    shareholding_dir = data_dir / "lake" / "normalized" / "shareholding_pattern"
    financials_dir.mkdir(parents=True, exist_ok=True)
    shareholding_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Generate sample quarterly financials data
    logger.info("Generating quarterly financials sample data")
    financials_path = generate_quarterly_financials_sample(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        output_dir=financials_dir,
    )

    # Step 2: Generate sample shareholding pattern data
    logger.info("Generating shareholding pattern sample data")
    shareholding_path = generate_shareholding_pattern_sample(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        output_dir=shareholding_dir,
    )

    # Step 3: Load and display sample data
    logger.info("Loading generated data")
    financials_df = pl.read_parquet(financials_path)
    shareholding_df = pl.read_parquet(shareholding_path)

    logger.info(
        "Data generation complete",
        financials_rows=len(financials_df),
        shareholding_rows=len(shareholding_df),
    )

    # Display sample records
    print("\n" + "="*80)
    print("SAMPLE QUARTERLY FINANCIALS DATA")
    print("="*80)
    print(financials_df.head(5))
    print(f"\nTotal records: {len(financials_df)}")
    print(f"Symbols: {financials_df['symbol'].n_unique()}")
    print(f"Date range: {financials_df['period_end_date'].min()} to {financials_df['period_end_date'].max()}")

    print("\n" + "="*80)
    print("SAMPLE SHAREHOLDING PATTERN DATA")
    print("="*80)
    print(shareholding_df.head(5))
    print(f"\nTotal records: {len(shareholding_df)}")
    print(f"Symbols: {shareholding_df['symbol'].n_unique()}")
    print(f"Date range: {shareholding_df['quarter_end_date'].min()} to {shareholding_df['quarter_end_date'].max()}")

    # Step 4: Display computed KPIs
    print("\n" + "="*80)
    print("KEY FINANCIAL METRICS SUMMARY")
    print("="*80)
    
    metrics_summary = financials_df.select([
        "symbol",
        "period_end_date",
        "revenue",
        "net_profit",
        "eps",
        "roe",
        "debt_to_equity",
        "current_ratio",
        "net_margin",
    ]).sort("period_end_date", descending=True)
    
    print(metrics_summary.head(10))

    # Step 5: Compute PE ratios (if OHLC data available)
    if compute_pe:
        logger.info("Computing PE ratios")
        try:
            # Try to load OHLC data
            ohlc_path = data_dir / "lake" / "normalized" / "equity_ohlc"
            if ohlc_path.exists():
                ohlc_files = list(ohlc_path.glob("**/*.parquet"))
                if ohlc_files:
                    logger.info("Loading OHLC data", files=len(ohlc_files))
                    ohlc_df = pl.read_parquet(ohlc_files[0])  # Load first file as sample
                    
                    pe_df = compute_pe_ratio(financials_df, ohlc_df)
                    
                    print("\n" + "="*80)
                    print("P/E RATIO ANALYSIS")
                    print("="*80)
                    print(pe_df.head(10))
                    
                    # Save PE ratios
                    pe_path = data_dir / "lake" / "features" / "fundamentals" / "pe_ratios.parquet"
                    pe_path.parent.mkdir(parents=True, exist_ok=True)
                    pe_df.write_parquet(pe_path)
                    logger.info("Saved PE ratios", path=str(pe_path))
                else:
                    logger.warning("No OHLC parquet files found")
            else:
                logger.warning("OHLC directory not found", path=str(ohlc_path))
        except Exception as e:
            logger.warning("Could not compute PE ratios", error=str(e))

    # Step 6: Load into ClickHouse (if requested)
    if load_to_clickhouse:
        logger.info("Loading data into ClickHouse")
        try:
            from clickhouse_driver import Client

            client = Client(
                host="localhost",
                port=9000,
                database="champion_market",
                user="champion_user",
                password="champion_pass",
            )

            # Insert quarterly financials
            logger.info("Inserting quarterly financials into ClickHouse")
            financials_records = financials_df.to_dicts()
            
            # Prepare data for ClickHouse (convert datetime to timestamp)
            for record in financials_records:
                record["event_time"] = record["event_time"].timestamp() if record.get("event_time") else None
                record["ingest_time"] = record["ingest_time"].timestamp() if record.get("ingest_time") else None
            
            client.execute(
                "INSERT INTO quarterly_financials VALUES",
                financials_records,
            )
            logger.info("Inserted quarterly financials", rows=len(financials_records))

            # Insert shareholding patterns
            logger.info("Inserting shareholding patterns into ClickHouse")
            shareholding_records = shareholding_df.to_dicts()
            
            # Prepare data for ClickHouse
            for record in shareholding_records:
                record["event_time"] = record["event_time"].timestamp() if record.get("event_time") else None
                record["ingest_time"] = record["ingest_time"].timestamp() if record.get("ingest_time") else None
            
            client.execute(
                "INSERT INTO shareholding_pattern VALUES",
                shareholding_records,
            )
            logger.info("Inserted shareholding patterns", rows=len(shareholding_records))

            # Verify data
            qf_count = client.execute("SELECT COUNT(*) FROM quarterly_financials")[0][0]
            sp_count = client.execute("SELECT COUNT(*) FROM shareholding_pattern")[0][0]
            
            print("\n" + "="*80)
            print("CLICKHOUSE DATA VERIFICATION")
            print("="*80)
            print(f"Quarterly Financials: {qf_count} rows")
            print(f"Shareholding Patterns: {sp_count} rows")

        except Exception as e:
            logger.error("Failed to load into ClickHouse", error=str(e))
            logger.info("Continuing without ClickHouse load")

    logger.info("Fundamentals ETL complete")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run fundamentals data ETL pipeline")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="List of symbols to process (default: NIFTY50)",
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: date.fromisoformat(s),
        help="Start date in YYYY-MM-DD format (default: 2 years ago)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: date.fromisoformat(s),
        help="End date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--no-clickhouse",
        action="store_true",
        help="Skip loading data into ClickHouse",
    )
    parser.add_argument(
        "--no-pe",
        action="store_true",
        help="Skip computing PE ratios",
    )

    args = parser.parse_args()

    run_fundamentals_etl(
        symbols=args.symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        load_to_clickhouse=not args.no_clickhouse,
        compute_pe=not args.no_pe,
    )
