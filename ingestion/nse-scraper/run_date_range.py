#!/usr/bin/env python3
"""Run NSE data ingestion for a date range.

Usage:
    poetry run python run_date_range.py --start-date 2024-01-02 --end-date 2024-01-05
    poetry run python run_date_range.py --start-date 2024-01-02 --end-date 2024-01-05 --skip-weekends
"""

import sys
import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import List

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.scrapers.bhavcopy import BhavcopyScraper
from src.parsers.polars_bhavcopy_parser import PolarsBhavcopyParser
from src.utils.logger import configure_logging, get_logger
from src.utils import metrics

configure_logging()
logger = get_logger(__name__)


def daterange(start_date: date, end_date: date):
    """Generate dates between start and end (inclusive)."""
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def is_weekend(d: date) -> bool:
    """Check if date is a weekend (Saturday=5, Sunday=6)."""
    return d.weekday() in (5, 6)


def run_etl_for_date(
    target_date: date,
    output_base: Path,
    load_clickhouse: bool = False,
    clickhouse_config: dict = None
) -> dict:
    """Run complete ETL pipeline for a single date.
    
    Args:
        target_date: Date to process
        output_base: Base path for data lake
        load_clickhouse: Whether to load data into ClickHouse
        clickhouse_config: ClickHouse connection config (host, port, user, password, database)
    
    Returns:
        dict with keys: status, rows, error (if failed)
    """
    logger.info("processing_date", date=str(target_date))
    
    try:
        # Step 1: Scrape
        scraper = BhavcopyScraper()
        csv_file = scraper.scrape(target_date, dry_run=False)
        logger.info("scrape_success", date=str(target_date), file=csv_file)
        metrics.files_downloaded.labels(scraper="nse_bhavcopy").inc()
        
        # Step 2: Parse
        parser = PolarsBhavcopyParser()
        normalized_df = parser.parse_to_dataframe(csv_file, target_date)
        logger.info("parse_success", date=str(target_date), rows=len(normalized_df))
        metrics.rows_parsed.labels(scraper="nse_bhavcopy", status="success").inc(len(normalized_df))
        
        # Step 3: Write Parquet
        output_file = parser.write_parquet(
            df=normalized_df,
            trade_date=target_date,
            base_path=output_base
        )
        file_size_mb = output_file.stat().st_size / (1024*1024)
        logger.info(
            "write_success",
            date=str(target_date),
            file=str(output_file),
            size_mb=file_size_mb
        )
        metrics.parquet_write_success.labels(table="normalized_equity_ohlc").inc()
        
        result = {
            "status": "success",
            "rows": len(normalized_df),
            "symbols": normalized_df['TckrSymb'].n_unique(),
            "file": str(output_file),
            "size_mb": file_size_mb
        }
        
        # Step 5: Load to ClickHouse (optional)
        if load_clickhouse and clickhouse_config:
            try:
                import clickhouse_connect
                
                client = clickhouse_connect.get_client(
                    host=clickhouse_config.get('host', 'localhost'),
                    port=clickhouse_config.get('port', 8123),
                    username=clickhouse_config.get('user', 'default'),
                    password=clickhouse_config.get('password', ''),
                    database=clickhouse_config.get('database', 'default')
                )
                
                # Read parquet and insert
                import polars as pl
                df = pl.read_parquet(output_file)
                
                # Convert to format ClickHouse expects
                data = df.to_dicts()
                
                client.insert(
                    clickhouse_config.get('table', 'normalized_equity_ohlc'),
                    data,
                    column_names=df.columns
                )
                
                logger.info(
                    "clickhouse_load_success",
                    date=str(target_date),
                    rows=len(data)
                )
                metrics.clickhouse_load_success.labels(table=clickhouse_config.get('table', 'normalized_equity_ohlc')).inc()
                result["clickhouse_loaded"] = True
                
            except Exception as ch_error:
                logger.error(
                    "clickhouse_load_failed",
                    date=str(target_date),
                    error=str(ch_error)
                )
                metrics.clickhouse_load_failed.labels(table=clickhouse_config.get('table', 'normalized_equity_ohlc')).inc()
                result["clickhouse_error"] = str(ch_error)
                result["clickhouse_loaded"] = False
        
        return result
        
    except Exception as e:
        logger.error("etl_failed", date=str(target_date), error=str(e))
        return {
            "status": "failed",
            "error": str(e)
        }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run NSE data ingestion for a date range",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run for a week in January 2024
  poetry run python run_date_range.py --start-date 2024-01-02 --end-date 2024-01-08
  
  # Skip weekends
  poetry run python run_date_range.py --start-date 2024-01-02 --end-date 2024-01-31 --skip-weekends
  
  # Specify custom output path
  poetry run python run_date_range.py --start-date 2024-01-02 --end-date 2024-01-05 --output /custom/path
        """
    )
    
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format (e.g., 2024-01-02)"
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format (e.g., 2024-01-31)"
    )
    parser.add_argument(
        "--skip-weekends",
        action="store_true",
        help="Skip Saturday and Sunday"
    )
    parser.add_argument(
        "--output",
        default="../../data/lake",
        help="Base output path for data lake (default: ../../data/lake)"
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing even if some dates fail"
    )
    parser.add_argument(
        "--load-clickhouse",
        action="store_true",
        help="Load data into ClickHouse after writing Parquet"
    )
    parser.add_argument(
        "--clickhouse-host",
        default="localhost",
        help="ClickHouse host (default: localhost)"
    )
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        default=8123,
        help="ClickHouse HTTP port (default: 8123)"
    )
    parser.add_argument(
        "--clickhouse-user",
        default="default",
        help="ClickHouse user (default: default)"
    )
    parser.add_argument(
        "--clickhouse-password",
        default="",
        help="ClickHouse password"
    )
    parser.add_argument(
        "--clickhouse-database",
        default="default",
        help="ClickHouse database (default: default)"
    )
    parser.add_argument(
        "--clickhouse-table",
        default="normalized_equity_ohlc",
        help="ClickHouse table (default: normalized_equity_ohlc)"
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=9090,
        help="Prometheus metrics port (default: 9090)"
    )
    parser.add_argument(
        "--no-metrics",
        action="store_true",
        help="Disable Prometheus metrics server"
    )
    
    args = parser.parse_args()
    
    # Parse dates
    try:
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
    except ValueError as e:
        print(f"❌ Invalid date format: {e}")
        print("   Use YYYY-MM-DD format (e.g., 2024-01-02)")
        sys.exit(1)
    
    if start_date > end_date:
        print("❌ Start date must be before or equal to end date")
        sys.exit(1)
    
    output_base = Path(args.output)
    
    # Start metrics server
    if not args.no_metrics:
        try:
            metrics.start_metrics_server(port=args.metrics_port)
            logger.info("metrics_server_started", port=args.metrics_port)
        except Exception as e:
            logger.warning("metrics_server_failed", error=str(e))
    
    # Prepare ClickHouse config
    clickhouse_config = None
    if args.load_clickhouse:
        clickhouse_config = {
            'host': args.clickhouse_host,
            'port': args.clickhouse_port,
            'user': args.clickhouse_user,
            'password': args.clickhouse_password,
            'database': args.clickhouse_database,
            'table': args.clickhouse_table
        }
    
    # Print configuration
    print("🚀 Champion NSE Date Range Ingestion")
    print("=" * 70)
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"📊 Total Days: {(end_date - start_date).days + 1}")
    if args.skip_weekends:
        print("⏭️  Skipping: Weekends (Saturday & Sunday)")
    print(f"📁 Output: {output_base}")
    print(f"🔄 Continue on Error: {args.continue_on_error}")
    if args.load_clickhouse:
        print(f"🗄️  ClickHouse: {args.clickhouse_host}:{args.clickhouse_port}/{args.clickhouse_database}")
    if not args.no_metrics:
        print(f"📊 Metrics: http://localhost:{args.metrics_port}/metrics")
    print("=" * 70)
    print()
    
    # Process each date
    results = []
    dates_to_process: List[date] = []
    
    for d in daterange(start_date, end_date):
        if args.skip_weekends and is_weekend(d):
            logger.info("skipping_weekend", date=str(d), day=d.strftime("%A"))
            continue
        dates_to_process.append(d)
    
    print(f"📋 Processing {len(dates_to_process)} dates...\n")
    
    for idx, target_date in enumerate(dates_to_process, 1):
        day_name = target_date.strftime("%A")
        print(f"[{idx}/{len(dates_to_process)}] {target_date} ({day_name})...")
        
        result = run_etl_for_date(target_date, output_base, args.load_clickhouse, clickhouse_config)
        result["date"] = target_date
        results.append(result)
        
        if result["status"] == "success":
            ch_status = ""
            if args.load_clickhouse:
                if result.get("clickhouse_loaded"):
                    ch_status = " | ClickHouse ✅"
                else:
                    ch_status = f" | ClickHouse ❌ ({result.get('clickhouse_error', 'unknown error')})"
            print(f"    ✅ Success: {result['rows']:,} rows, {result['symbols']} symbols{ch_status}")
        else:
            print(f"    ❌ Failed: {result['error']}")
            if not args.continue_on_error:
                print("\n⚠️  Stopping due to error (use --continue-on-error to skip failures)")
                break
        print()
    
    # Print summary
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    
    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]
    
    print(f"✅ Successful: {len(successful)}")
    print(f"❌ Failed: {len(failed)}")
    print(f"📈 Total Rows Processed: {sum(r.get('rows', 0) for r in successful):,}")
    
    if successful:
        print("\n✅ Successfully Processed Dates:")
        for r in successful:
            print(f"   • {r['date']}: {r['rows']:,} rows")
    
    if failed:
        print("\n❌ Failed Dates:")
        for r in failed:
            print(f"   • {r['date']}: {r['error']}")
    
    print("\n✨ Next Steps:")
    print(f"  1. View data: ls -lR {output_base}/normalized/equity_ohlc/")
    print(f"  2. Query with Polars:")
    print(f"     poetry run python -c \"import polars as pl; df = pl.read_parquet('{output_base}/normalized/**/*.parquet'); print(df)\"")
    print(f"  3. Load to ClickHouse:")
    print(f"     cd ../../warehouse && poetry run python -m loader.batch_loader")
    
    # Exit with appropriate code
    if failed and not args.continue_on_error:
        sys.exit(1)
    elif failed:
        sys.exit(2)  # Partial success
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
