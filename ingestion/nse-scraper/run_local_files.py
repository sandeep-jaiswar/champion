#!/usr/bin/env python3
"""Process existing local bhavcopy CSV files.

Usage:
    poetry run python run_local_files.py
"""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.parsers.polars_bhavcopy_parser import PolarsBhavcopyParser
from src.utils.logger import configure_logging, get_logger
from src.utils import metrics

configure_logging()
logger = get_logger(__name__)


def main():
    """Process all CSV files in the data directory."""
    data_dir = Path("./data")
    output_base = Path("../../data/lake")
    
    # Start metrics server
    try:
        metrics.start_metrics_server(port=9090)
        logger.info("metrics_server_started", port=9090)
    except Exception as e:
        logger.warning("metrics_server_failed", error=str(e))
    
    # Find all CSV files
    csv_files = list(data_dir.glob("BhavCopy_NSE_CM_*.csv"))
    
    if not csv_files:
        logger.error("no_csv_files_found", directory=str(data_dir))
        print(f"❌ No CSV files found in {data_dir}")
        return
    
    print(f"🚀 Processing {len(csv_files)} CSV files from {data_dir}")
    print("=" * 70)
    
    parser = PolarsBhavcopyParser()
    success_count = 0
    failed_count = 0
    total_rows = 0
    
    for csv_file in sorted(csv_files):
        # Extract date from filename: BhavCopy_NSE_CM_20240101.csv
        try:
            date_str = csv_file.stem.split('_')[-1]  # Get '20240101'
            target_date = date(
                int(date_str[0:4]),  # year
                int(date_str[4:6]),  # month
                int(date_str[6:8])   # day
            )
            
            print(f"\n📄 Processing {csv_file.name} ({target_date})...")
            logger.info("processing_file", file=str(csv_file), date=str(target_date))
            
            # Parse to DataFrame
            normalized_df = parser.parse_to_dataframe(csv_file, target_date)
            logger.info("parse_success", date=str(target_date), rows=len(normalized_df))
            metrics.rows_parsed.labels(scraper="local_file", status="success").inc(len(normalized_df))
            
            # Write Parquet
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
            
            print(f"   ✅ Success: {len(normalized_df)} rows, {file_size_mb:.2f} MB")
            print(f"   📦 Output: {output_file}")
            
            success_count += 1
            total_rows += len(normalized_df)
            
        except Exception as e:
            logger.error("processing_failed", file=str(csv_file), error=str(e))
            print(f"   ❌ Failed: {e}")
            failed_count += 1
    
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"✅ Successful: {success_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📈 Total Rows Processed: {total_rows:,}")
    print(f"\n📁 Data Location: {output_base}/normalized/equity_ohlc/")
    print(f"📊 Metrics: http://localhost:9090/metrics")


if __name__ == "__main__":
    main()
