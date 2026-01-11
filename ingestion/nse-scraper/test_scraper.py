#!/usr/bin/env python3
"""Test scraper with a known good date."""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.scrapers.bhavcopy import BhavcopyScraper
from src.utils.logger import configure_logging

configure_logging()

# Use a date from 2024 that should have data
test_date = date(2024, 12, 31)  # Last trading day of 2024

print(f"🧪 Testing NSE Bhavcopy Scraper")
print(f"📅 Test Date: {test_date}")
print("=" * 60)

scraper = BhavcopyScraper()
try:
    csv_file = scraper.scrape(test_date, dry_run=False)
    print(f"\n✅ SUCCESS: Downloaded {csv_file}")
    print(f"📊 File size: {csv_file.stat().st_size / 1024:.2f} KB")
except Exception as e:
    print(f"\n❌ FAILED: {e}")
    sys.exit(1)
