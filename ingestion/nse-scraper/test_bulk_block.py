#!/usr/bin/env python3
"""Test Bulk & Block Deals scraper."""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.scrapers.bulk_block_deals import BulkBlockDealsScraper
from src.utils.logger import configure_logging

configure_logging()

# Use Dec 31, 2024 (known good date)
test_date = date(2024, 12, 31)

print("🧪 Testing Bulk & Block Deals Scraper")
print(f"📅 Test Date: {test_date}")
print("=" * 60)

scraper = BulkBlockDealsScraper()
try:
    result = scraper.scrape(test_date, dry_run=False)
    print(f"\n✅ SUCCESS: {result}")
except Exception as e:
    print(f"\n❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
