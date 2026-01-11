#!/usr/bin/env python3
"""Test bulk and block deals scraper with new API."""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.scrapers.bulk_block_deals import BulkBlockDealsScraper
from src.utils.logger import configure_logging

configure_logging()

print("🧪 Testing Bulk & Block Deals Scraper (Updated API)")
print("=" * 60)

# Use a recent date
target_date = date.today() - timedelta(days=5)

with BulkBlockDealsScraper() as scraper:
    print(f"\n📅 Scraping for date: {target_date}")
    
    try:
        results = scraper.scrape(
            target_date=target_date,
            deal_type="both",
            dry_run=False
        )
        
        print(f"\n✅ Successfully scraped:")
        for deal_type, file_path in results.items():
            print(f"   - {deal_type.upper()}: {file_path}")
            
            # Try to read and show first few rows
            if file_path.exists():
                import pandas as pd
                df = pd.read_csv(file_path)
                print(f"\n   {deal_type.upper()} Deals Preview (first 3 rows):")
                print(df.head(3).to_string())
                print(f"   Total {deal_type} deals: {len(df)}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 60)
