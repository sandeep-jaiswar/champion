#!/usr/bin/env python3
"""Simple ETL runner that doesn't require MLflow."""

import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

print("🚀 Starting Champion NSE Data Pipeline")
print("=" * 60)

# Step 1: Scrape
print("\n📥 Step 1: Scraping NSE Bhavcopy Data...")
from src.scrapers.bhavcopy import BhavcopyScraper
from src.utils.logger import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

target_date = date.today() - timedelta(days=1)
scraper = BhavcopyScraper()

try:
    csv_file = scraper.scrape(target_date, dry_run=False)
    print(f"✅ Downloaded: {csv_file}")
except Exception as e:
    print(f"❌ Scrape failed: {e}")
    sys.exit(1)

# Step 2: Parse
print("\n📊 Step 2: Parsing with Polars...")
from src.parsers.polars_bhavcopy_parser import PolarsBhavcopyParser

parser = PolarsBhavcopyParser()
try:
    raw_df = parser.parse_raw_csv(csv_file)
    print(f"✅ Parsed {len(raw_df)} rows")
except Exception as e:
    print(f"❌ Parse failed: {e}")
    sys.exit(1)

# Step 3: Normalize
print("\n🔧 Step 3: Normalizing data...")
try:
    normalized_df = parser.normalize(raw_df)
    print(f"✅ Normalized {len(normalized_df)} rows")
except Exception as e:
    print(f"❌ Normalize failed: {e}")
    sys.exit(1)

# Step 4: Write Parquet
print("\n💾 Step 4: Writing to Parquet...")
try:
    output_file = parser.write_parquet(
        df=normalized_df,
        trade_date=target_date,
        base_path=Path("../../data/lake")
    )
    print(f"✅ Written to: {output_file}")
except Exception as e:
    print(f"❌ Write failed: {e}")
    sys.exit(1)

# Summary
print("\n" + "=" * 60)
print("🎉 ETL Pipeline Completed Successfully!")
print("=" * 60)
print(f"\n📅 Trade Date: {target_date}")
print(f"📈 Rows Processed: {len(normalized_df)}")
print(f"📁 Output: {output_file}")
print(f"💾 File Size: {output_file.stat().st_size / (1024*1024):.2f} MB")

# Show sample data
print("\n📊 Sample Data:")
print(normalized_df.head(5))
print("\n✨ Next steps:")
print("  1. View data: ls -lh data/lake/normalized/equity_ohlc/**/*.parquet")
print("  2. Load to ClickHouse: poetry run python warehouse/loader/batch_loader.py")
print("  3. Compute features: poetry run python src/features/demo_features.py")
