#!/usr/bin/env python3
"""
Analytics queries on backfilled bulk/block deals data.
"""
import polars as pl
from pathlib import Path
from datetime import datetime

def run_analytics():
    """Run analytics on bulk/block deals data."""
    
    lake_root = Path("data/lake/bulk_block_deals")
    
    print("=" * 80)
    print("BULK/BLOCK DEALS ANALYTICS")
    print("=" * 80)
    
    # Aggregate all Parquet files
    all_files = list(lake_root.rglob("*.parquet"))
    print(f"\n📊 Loading {len(all_files)} Parquet files...")
    
    dfs = []
    for pf in all_files:
        try:
            df = pl.read_parquet(pf, hive_partitioning=False)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️  Skipped {pf.name}: {e}")
            continue
    
    if not dfs:
        print("❌ No data found!")
        return
    
    # Combine all data
    data = pl.concat(dfs, how="vertical")
    print(f"✅ Loaded {len(data):,} total events")
    
    print("\n" + "=" * 80)
    print("QUERY 1: Top 10 Most Active Symbols (by event count)")
    print("=" * 80)
    top_symbols = data.group_by("symbol").agg(
        pl.col("event_id").count().alias("event_count"),
        pl.col("quantity").sum().alias("total_quantity"),
        pl.col("avg_price").mean().alias("avg_price_mean")
    ).sort("event_count", descending=True).head(10)
    print(top_symbols)
    
    print("\n" + "=" * 80)
    print("QUERY 2: Deal Type Distribution")
    print("=" * 80)
    deal_dist = data.group_by("deal_type").agg(
        pl.col("event_id").count().alias("count"),
        (pl.col("event_id").count() * 100 / len(data)).alias("percentage")
    )
    print(deal_dist)
    
    print("\n" + "=" * 80)
    print("QUERY 3: Transaction Type Distribution")
    print("=" * 80)
    txn_dist = data.group_by("transaction_type").agg(
        pl.col("event_id").count().alias("count"),
        (pl.col("event_id").count() * 100 / len(data)).alias("percentage")
    )
    print(txn_dist)
    
    print("\n" + "=" * 80)
    print("QUERY 4: Events by Year-Month")
    print("=" * 80)
    events_by_month = data.group_by("year", "month").agg(
        pl.col("event_id").count().alias("events"),
        pl.col("quantity").sum().alias("total_quantity")
    ).sort(["year", "month"])
    print(events_by_month)
    
    print("\n" + "=" * 80)
    print("QUERY 5: Top 10 Clients (by deal count)")
    print("=" * 80)
    top_clients = data.group_by("client_name").agg(
        pl.col("event_id").count().alias("deal_count"),
        pl.col("quantity").sum().alias("total_quantity"),
        pl.col("avg_price").mean().alias("avg_price")
    ).sort("deal_count", descending=True).head(10)
    print(top_clients)
    
    print("\n" + "=" * 80)
    print("QUERY 6: Price Statistics")
    print("=" * 80)
    price_stats = data.select(
        pl.col("avg_price").min().alias("min_price"),
        pl.col("avg_price").max().alias("max_price"),
        pl.col("avg_price").mean().alias("mean_price"),
        pl.col("avg_price").median().alias("median_price"),
        pl.col("avg_price").std().alias("std_price"),
    )
    print(price_stats)
    
    print("\n" + "=" * 80)
    print("QUERY 7: Quantity Statistics")
    print("=" * 80)
    qty_stats = data.select(
        pl.col("quantity").min().alias("min_qty"),
        pl.col("quantity").max().alias("max_qty"),
        pl.col("quantity").mean().alias("mean_qty"),
        pl.col("quantity").median().alias("median_qty"),
        pl.col("quantity").sum().alias("total_qty"),
    )
    print(qty_stats)
    
    print("\n" + "=" * 80)
    print("QUERY 8: Data Quality Check")
    print("=" * 80)
    quality = data.select(
        (pl.col("symbol").null_count() == 0).alias("symbol_complete"),
        (pl.col("client_name").null_count() == 0).alias("client_complete"),
        (pl.col("quantity").null_count() == 0).alias("quantity_complete"),
        (pl.col("avg_price").null_count() == 0).alias("price_complete"),
    )
    print(quality.describe())
    
    print("\n" + "=" * 80)
    print("✅ ANALYTICS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    run_analytics()
