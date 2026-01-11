#!/usr/bin/env python3
"""Test all remaining scrapers."""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logger import configure_logging

configure_logging()

print("🧪 Testing All NSE Scrapers")
print("=" * 80)

# Test 1: Trading Calendar
print("\n1️⃣  TRADING CALENDAR SCRAPER")
print("-" * 80)
try:
    from src.scrapers.trading_calendar import TradingCalendarScraper
    
    scraper = TradingCalendarScraper()
    csv_file = scraper.scrape(year=2026, dry_run=False)
    
    if csv_file.exists():
        print(f"✅ SUCCESS: Downloaded trading calendar")
        print(f"📁 File: {csv_file}")
        
        # Try to read with Polars
        try:
            import polars as pl
            df = pl.read_json(str(csv_file))
            print(f"📊 Calendar data shape: {df.shape}")
            print(f"🏷️  Columns: {df.columns[:5]}...")  # Show first 5 columns
        except Exception as e:
            print(f"ℹ️  Reading as JSON (calendar format): {len(open(csv_file).read())} bytes")
    else:
        print(f"⚠️  File not found: {csv_file}")
        
    scraper.close()
except Exception as e:
    print(f"❌ FAILED: {e}")
    import traceback
    traceback.print_exc()

# Test 2: Option Chain
print("\n2️⃣  OPTION CHAIN SCRAPER")
print("-" * 80)
try:
    from src.scrapers.option_chain import OptionChainScraper
    
    scraper = OptionChainScraper()
    symbols = ["NIFTY", "BANKNIFTY"]
    
    for symbol in symbols:
        try:
            df = scraper.scrape(symbol=symbol, output_dir="data/option_chain")
            print(f"✅ {symbol}: {len(df)} options found")
            if len(df) > 0:
                print(f"   Columns: {df.columns[:4]}")
        except Exception as e:
            print(f"⚠️  {symbol}: {str(e)[:80]}")
    
    scraper.close()
except Exception as e:
    print(f"❌ FAILED: {e}")

# Test 3: BSE Bhavcopy
print("\n3️⃣  BSE BHAVCOPY SCRAPER")
print("-" * 80)
try:
    from src.scrapers.bse_bhavcopy import BseBhavcopyScraper
    
    scraper = BseBhavcopyScraper()
    bse_date = date(2024, 12, 31)
    
    csv_file = scraper.scrape(target_date=bse_date, dry_run=False)
    print(f"✅ SUCCESS: Downloaded BSE Bhavcopy")
    print(f"📁 File: {csv_file}")
    
    if csv_file.exists():
        import polars as pl
        df = pl.read_csv(str(csv_file))
        print(f"📊 BSE data shape: {df.shape}")
        print(f"🏷️  Sample columns: {df.columns[:5]}")
        
except Exception as e:
    print(f"❌ FAILED: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Index Constituents
print("\n4️⃣  INDEX CONSTITUENT SCRAPER")
print("-" * 80)
try:
    from src.scrapers.index_constituent import IndexConstituentScraper
    
    scraper = IndexConstituentScraper()
    indices = ["NIFTY50", "BANKNIFTY"]
    
    for index in indices:
        try:
            result = scraper.scrape(indices=[index])
            if result:
                print(f"✅ {index}: {len(result)} constituents")
        except Exception as e:
            print(f"⚠️  {index}: {str(e)[:80]}")
    
    scraper.close()
except Exception as e:
    print(f"❌ FAILED: {e}")
    import traceback
    traceback.print_exc()

# Test 5: BSE Shareholding
print("\n5️⃣  BSE SHAREHOLDING SCRAPER")
print("-" * 80)
try:
    from src.scrapers.bse_shareholding import BseShareholdingScraper
    
    scraper = BseShareholdingScraper()
    # Try a known company
    scraper_date = date(2024, 12, 31)
    
    print(f"⚠️  BSE Shareholding requires manual download or authentication")
    print(f"📋 Scraper available at: src/scrapers/bse_shareholding.py")
    
except Exception as e:
    print(f"❌ FAILED: {e}")

# Test 6: MCA Financials
print("\n6️⃣  MCA FINANCIALS SCRAPER")
print("-" * 80)
try:
    from src.scrapers.mca_financials import McaFinancialsScraper
    
    scraper = McaFinancialsScraper()
    print(f"⚠️  MCA Financials requires specific company codes")
    print(f"📋 Scraper available at: src/scrapers/mca_financials.py")
    
except Exception as e:
    print(f"❌ FAILED: {e}")

# Test 7: RBI Macro
print("\n7️⃣  RBI MACRO SCRAPER")
print("-" * 80)
try:
    from src.scrapers.rbi_macro import RBIMacroScraper
    
    scraper = RBIMacroScraper()
    print(f"⚠️  RBI Macro data requires DBIE portal access")
    print(f"📋 Scraper available at: src/scrapers/rbi_macro.py")
    
except Exception as e:
    print(f"❌ FAILED: {e}")

# Test 8: MOSPI Macro
print("\n8️⃣  MOSPI MACRO SCRAPER")
print("-" * 80)
try:
    from src.scrapers.mospi_macro import MospiMacroScraper
    
    scraper = MospiMacroScraper()
    print(f"⚠️  MOSPI data requires specific source access")
    print(f"📋 Scraper available at: src/scrapers/mospi_macro.py")
    
except Exception as e:
    print(f"❌ FAILED: {e}")

print("\n" + "=" * 80)
print("✅ Testing Complete!")
