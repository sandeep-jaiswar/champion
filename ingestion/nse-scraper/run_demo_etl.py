#!/usr/bin/env python3
"""Simple ETL runner for a specific historical date."""

import sys
from datetime import date
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

print("🚀 Starting Champion NSE Data Pipeline")
print("=" * 60)

# Use a date we know has data (January 2, 2024)
target_date = date(2024, 1, 2)

# Step 1: Scrape
print(f"\n📥 Step 1: Scraping NSE Bhavcopy Data for {target_date}...")
from src.scrapers.bhavcopy import BhavcopyScraper
from src.utils.logger import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

scraper = BhavcopyScraper()

try:
    csv_file = scraper.scrape(target_date, dry_run=False)
    print(f"✅ Downloaded: {csv_file}")
except Exception as e:
    print(f"❌ Scrape failed: {e}")
    # Try to use existing file if available
    csv_file = Path(f"data/BhavCopy_NSE_CM_{target_date.strftime('%Y%m%d')}.csv")
    if not csv_file.exists():
        print(f"❌ No existing file found at {csv_file}")
        sys.exit(1)
    print(f"📁 Using existing file: {csv_file}")

# Step 2: Parse
print("\n📊 Step 2: Parsing with Polars...")
from src.parsers.polars_bhavcopy_parser import PolarsBhavcopyParser

parser = PolarsBhavcopyParser()
try:
    raw_df = parser.parse_raw_csv(str(csv_file))
    print(f"✅ Parsed {len(raw_df)} rows")
    print(f"   Columns: {raw_df.columns[:5]}...")
except Exception as e:
    print(f"❌ Parse failed: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)

# Step 3: Normalize
print("\n🔧 Step 3: Normalizing data...")
try:
    normalized_df = parser.normalize(raw_df)
    print(f"✅ Normalized {len(normalized_df)} rows")
    print(f"   Symbols: {normalized_df['symbol'].n_unique()}")
except Exception as e:
    print(f"❌ Normalize failed: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)

# Step 4: Write Parquet
print("\n💾 Step 4: Writing to Parquet...")
try:
    output_file = parser.write_parquet(
        df=normalized_df, trade_date=target_date, base_path=Path("../../data/lake")
    )
    print(f"✅ Written to: {output_file}")
    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    print(f"   Size: {file_size_mb:.2f} MB")
except Exception as e:
    print(f"❌ Write failed: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)

# Summary
print("\n" + "=" * 60)
print("🎉 ETL Pipeline Completed Successfully!")
print("=" * 60)
print(f"\n📅 Trade Date: {target_date}")
print(f"📈 Rows Processed: {len(normalized_df)}")
print(f"🏢 Unique Symbols: {normalized_df['symbol'].n_unique()}")
print(f"📁 Output: {output_file}")
print(f"💾 File Size: {file_size_mb:.2f} MB")

# Show sample data
print("\n📊 Sample Data (first 3 rows):")
print(normalized_df.head(3))

# Show summary statistics
print("\n📈 Volume Statistics:")
volume_stats = normalized_df.select(["total_traded_quantity", "total_traded_value"]).describe()
print(volume_stats)

print("\n✨ Next steps:")
print(
    f"  1. View data: ls -lh ../../data/lake/normalized/equity_ohlc/year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/"
)
print("  2. Query with Polars:")
print(
    "     poetry run python -c \"import polars as pl; df = pl.read_parquet('../../data/lake/normalized/**/*.parquet'); print(df.head())\""
)
print("  3. Load to ClickHouse (if running):")
print(
    '     clickhouse-client --query "SELECT COUNT(*) FROM champion_market.normalized_equity_ohlc"'
)
print("  4. Compute features:")
print("     cd ../../src/features && poetry run python demo_features.py")
