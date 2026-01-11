#!/usr/bin/env python3
"""Verification script for bulk and block deals ingestion.

This script demonstrates:
1. Creating sample bulk/block deals data
2. Running the parser
3. Writing to Parquet
4. Verifying data quality

For actual NSE data, use run_bulk_block_deals.py
"""

import json
from datetime import date
from pathlib import Path

# Sample bulk deals data matching NSE API format
SAMPLE_BULK_DEALS = [
    {
        "symbol": "RELIANCE",
        "clientName": "ABC SECURITIES LTD",
        "buyQty": 1500000,
        "sellQty": 0,
        "buyAvgPrice": 2850.50,
        "sellAvgPrice": 0,
        "dealDate": "10-JAN-2026",
    },
    {
        "symbol": "TCS",
        "clientName": "XYZ INVESTMENTS PVT LTD",
        "buyQty": 0,
        "sellQty": 750000,
        "buyAvgPrice": 0,
        "sellAvgPrice": 3920.75,
        "dealDate": "10-JAN-2026",
    },
    {
        "symbol": "INFY",
        "clientName": "DEF CAPITAL MARKETS",
        "buyQty": 1000000,
        "sellQty": 0,
        "buyAvgPrice": 1625.25,
        "sellAvgPrice": 0,
        "dealDate": "10-JAN-2026",
    },
    {
        "symbol": "HDFCBANK",
        "clientName": "GHI TRADING COMPANY",
        "buyQty": 0,
        "sellQty": 800000,
        "buyAvgPrice": 0,
        "sellAvgPrice": 1685.90,
        "dealDate": "10-JAN-2026",
    },
    {
        "symbol": "ICICIBANK",
        "clientName": "JKL SECURITIES",
        "buyQty": 600000,
        "sellQty": 600000,
        "buyAvgPrice": 1125.50,
        "sellAvgPrice": 1125.75,
        "dealDate": "10-JAN-2026",
    },
]

SAMPLE_BLOCK_DEALS = [
    {
        "symbol": "TATASTEEL",
        "clientName": "MNO INVESTMENT FUND",
        "buyQty": 2000000,
        "sellQty": 0,
        "buyAvgPrice": 145.80,
        "sellAvgPrice": 0,
        "dealDate": "10-JAN-2026",
    },
    {
        "symbol": "SBIN",
        "clientName": "PQR WEALTH MANAGEMENT",
        "buyQty": 0,
        "sellQty": 1500000,
        "buyAvgPrice": 0,
        "sellAvgPrice": 785.25,
        "dealDate": "10-JAN-2026",
    },
]


def create_sample_data():
    """Create sample JSON files for testing."""
    data_dir = Path("data/deals/sample")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Write bulk deals
    bulk_file = data_dir / "bulk_deals_20260110.json"
    with open(bulk_file, "w") as f:
        json.dump(SAMPLE_BULK_DEALS, f, indent=2)
    print(f"✓ Created sample bulk deals: {bulk_file}")
    
    # Write block deals
    block_file = data_dir / "block_deals_20260110.json"
    with open(block_file, "w") as f:
        json.dump(SAMPLE_BLOCK_DEALS, f, indent=2)
    print(f"✓ Created sample block deals: {block_file}")
    
    return bulk_file, block_file


def verify_parsing(bulk_file, block_file):
    """Verify parsing of sample data."""
    from src.parsers.bulk_block_deals_parser import BulkBlockDealsParser
    
    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)
    
    # Parse bulk deals
    print("\n📊 Parsing bulk deals...")
    bulk_events = parser.parse(bulk_file, deal_date, "BULK")
    print(f"✓ Parsed {len(bulk_events)} bulk deal events")
    
    # Parse block deals
    print("\n📊 Parsing block deals...")
    block_events = parser.parse(block_file, deal_date, "BLOCK")
    print(f"✓ Parsed {len(block_events)} block deal events")
    
    return bulk_events, block_events


def verify_parquet_write(events, deal_date, deal_type):
    """Verify Parquet write functionality."""
    from src.parsers.bulk_block_deals_parser import BulkBlockDealsParser
    
    parser = BulkBlockDealsParser()
    output_path = Path("data/lake/sample")
    
    print(f"\n📝 Writing {deal_type} deals to Parquet...")
    parquet_file = parser.write_parquet(
        events=events,
        output_base_path=output_path,
        deal_date=deal_date,
        deal_type=deal_type,
    )
    print(f"✓ Written to: {parquet_file}")
    
    # Verify file exists and read it
    import polars as pl
    df = pl.read_parquet(parquet_file)
    print(f"✓ Verified Parquet: {len(df)} rows")
    
    return parquet_file


def print_summary(bulk_events, block_events):
    """Print summary statistics."""
    print("\n" + "="*60)
    print("VERIFICATION SUMMARY")
    print("="*60)
    
    # Bulk deals summary
    print("\n📈 BULK DEALS:")
    print(f"  Total events: {len(bulk_events)}")
    buy_events = [e for e in bulk_events if e["transaction_type"] == "BUY"]
    sell_events = [e for e in bulk_events if e["transaction_type"] == "SELL"]
    print(f"  Buy transactions: {len(buy_events)}")
    print(f"  Sell transactions: {len(sell_events)}")
    
    if buy_events:
        total_buy_qty = sum(e["quantity"] for e in buy_events)
        total_buy_value = sum(e["quantity"] * e["avg_price"] for e in buy_events)
        print(f"  Total buy quantity: {total_buy_qty:,}")
        print(f"  Total buy value: ₹{total_buy_value:,.2f}")
    
    # Block deals summary
    print("\n📈 BLOCK DEALS:")
    print(f"  Total events: {len(block_events)}")
    buy_events = [e for e in block_events if e["transaction_type"] == "BUY"]
    sell_events = [e for e in block_events if e["transaction_type"] == "SELL"]
    print(f"  Buy transactions: {len(buy_events)}")
    print(f"  Sell transactions: {len(sell_events)}")
    
    if buy_events:
        total_buy_qty = sum(e["quantity"] for e in buy_events)
        total_buy_value = sum(e["quantity"] * e["avg_price"] for e in buy_events)
        print(f"  Total buy quantity: {total_buy_qty:,}")
        print(f"  Total buy value: ₹{total_buy_value:,.2f}")
    
    # Top deals by quantity
    print("\n🏆 TOP 5 DEALS BY QUANTITY:")
    all_events = bulk_events + block_events
    top_deals = sorted(all_events, key=lambda x: x["quantity"], reverse=True)[:5]
    for i, deal in enumerate(top_deals, 1):
        print(f"  {i}. {deal['symbol']}: {deal['quantity']:,} @ ₹{deal['avg_price']:.2f} ({deal['deal_type']}, {deal['transaction_type']})")
    
    print("\n✅ Verification complete!")
    print("\nNext steps:")
    print("  1. Run: python run_bulk_block_deals.py --date 2026-01-10")
    print("  2. Query ClickHouse to verify data")
    print("  3. Check Parquet files in data/lake/bulk_block_deals/")


def main():
    """Run verification."""
    print("="*60)
    print("BULK AND BLOCK DEALS VERIFICATION")
    print("="*60)
    
    # Create sample data
    print("\n1️⃣  Creating sample data...")
    bulk_file, block_file = create_sample_data()
    
    # Verify parsing
    print("\n2️⃣  Verifying parsing...")
    bulk_events, block_events = verify_parsing(bulk_file, block_file)
    
    # Verify Parquet write
    print("\n3️⃣  Verifying Parquet write...")
    deal_date = date(2026, 1, 10)
    verify_parquet_write(bulk_events, deal_date, "BULK")
    verify_parquet_write(block_events, deal_date, "BLOCK")
    
    # Print summary
    print_summary(bulk_events, block_events)


if __name__ == "__main__":
    main()
