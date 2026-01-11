#!/usr/bin/env python3
"""Test Symbol Master scraper."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.scrapers.symbol_master import SymbolMasterScraper
from src.utils.logger import configure_logging

configure_logging()

print("🧪 Testing Symbol Master Scraper")
print("=" * 60)

scraper = SymbolMasterScraper()
try:
    scraper.scrape(dry_run=False)
    csv_file = Path("data/EQUITY_L.csv")
    if csv_file.exists():
        lines = len(csv_file.read_text().strip().split('\n'))
        print(f"\n✅ SUCCESS: Downloaded {csv_file}")
        print(f"📊 Total rows: {lines}")
    else:
        print(f"❌ File not found: {csv_file}")
except Exception as e:
    print(f"\n❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
