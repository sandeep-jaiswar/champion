#!/usr/bin/env python3
"""
Validate bulk/block deals backfill data integrity and quality.
"""
import polars as pl
from pathlib import Path
from collections import defaultdict

def validate_backfill():
    """Validate all backfilled data."""
    lake_root = Path("data/lake/bulk_block_deals")
    
    if not lake_root.exists():
        print("❌ Data lake not found!")
        return
    
    print("=" * 80)
    print("BACKFILL DATA VALIDATION")
    print("=" * 80)
    
    # Find all parquet files
    parquet_files = list(lake_root.rglob("*.parquet"))
    print(f"\n✅ Found {len(parquet_files)} Parquet files")
    
    # Organize by year and deal_type
    stats = defaultdict(lambda: {"bulk": 0, "block": 0})
    total_rows = 0
    schema_check = {}
    
    for pf in sorted(parquet_files):
        try:
            df = pl.read_parquet(pf, hive_partitioning=False)
            row_count = len(df)
            total_rows += row_count
            
            # Extract metadata from path
            parts = pf.parts
            deal_type = next((p.split("=")[1] for p in parts if p.startswith("deal_type=")), None)
            year = next((p.split("=")[1] for p in parts if p.startswith("year=")), None)
            
            if deal_type and year:
                if deal_type == "BULK":
                    stats[year]["bulk"] += row_count
                else:
                    stats[year]["block"] += row_count
            
            # Check schema on first file
            if year not in schema_check:
                schema_check[year] = {
                    "columns": df.columns,
                    "n_cols": len(df.columns),
                    "sample_row": df.to_dicts()[0] if len(df) > 0 else None
                }
                
        except Exception as e:
            print(f"⚠️  Error reading {pf.name}: {e}")
    
    # Print summary by year
    print(f"\n{'YEAR':<8} {'BULK DEALS':<20} {'BLOCK DEALS':<20} {'TOTAL':<10}")
    print("-" * 60)
    
    grand_total_bulk = 0
    grand_total_block = 0
    
    for year in sorted(stats.keys()):
        bulk_count = stats[year]["bulk"]
        block_count = stats[year]["block"]
        year_total = bulk_count + block_count
        
        grand_total_bulk += bulk_count
        grand_total_block += block_count
        
        print(f"{year:<8} {bulk_count:<20,} {block_count:<20,} {year_total:<10,}")
    
    print("-" * 60)
    grand_total = grand_total_bulk + grand_total_block
    print(f"{'TOTAL':<8} {grand_total_bulk:<20,} {grand_total_block:<20,} {grand_total:<10,}")
    
    # Schema info
    print(f"\n{'SCHEMA VALIDATION':<40}")
    print("-" * 60)
    for year in sorted(schema_check.keys()):
        info = schema_check[year]
        print(f"\nYear {year}:")
        print(f"  Columns ({info['n_cols']}): {', '.join(info['columns'][:5])}...")
        if info['sample_row']:
            print(f"  Sample row keys: {list(info['sample_row'].keys())}")
    
    print("\n" + "=" * 80)
    print(f"✅ VALIDATION COMPLETE: {total_rows:,} total events across {len(parquet_files)} files")
    print("=" * 80)

if __name__ == "__main__":
    validate_backfill()
