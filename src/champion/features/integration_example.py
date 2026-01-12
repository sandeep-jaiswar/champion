#!/usr/bin/env python3
"""
End-to-end integration example for features pipeline.

This script demonstrates the complete workflow:
1. Generate sample normalized OHLC data (or use existing data)
2. Compute technical indicators
3. Write to Parquet
4. Validate ClickHouse schema compatibility

Usage:
    python integration_example.py
"""

import sys
from pathlib import Path

import polars as pl

# Ensure src/ is on path for editable runs
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from champion.features.indicators import compute_features

# Import warehouse loader for validation
try:
    from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader
    LOADER_AVAILABLE = True
except ImportError:
    LOADER_AVAILABLE = False
    print("Warning: ClickHouse loader not available")


def generate_sample_data():
    """Generate sample normalized OHLC data."""
    from datetime import date, timedelta
    
    symbols = ["AAPL", "GOOGL", "MSFT"]
    base_date = date(2024, 1, 1)
    
    records = []
    for symbol in symbols:
        base_price = {"AAPL": 150.0, "GOOGL": 2800.0, "MSFT": 350.0}[symbol]
        
        for day in range(100):
            trade_date = base_date + timedelta(days=day)
            price_var = (day % 20) - 10  # Oscillation
            close_price = base_price + price_var + (day * 0.3)  # Trend
            
            records.append({
                "symbol": symbol,
                "trade_date": trade_date,
                "open": close_price - 1.0,
                "high": close_price + 2.0,
                "low": close_price - 2.0,
                "close": close_price,
                "volume": 1000000 + day * 5000,
            })
    
    return pl.DataFrame(records)


def main():
    """Run the integration example."""
    print("=" * 70)
    print("Features Pipeline - Integration Example")
    print("=" * 70)
    print()
    
    # Step 1: Generate or load normalized data
    print("Step 1: Loading normalized OHLC data...")
    df_normalized = generate_sample_data()
    print(f"  Loaded {len(df_normalized)} rows for {df_normalized['symbol'].n_unique()} symbols")
    print()
    
    # Step 2: Compute features
    print("Step 2: Computing technical indicators...")
    output_path = Path(__file__).parent.parent.parent.parent / "data" / "lake" / "features" / "equity"
    
    df_features = compute_features(
        df=df_normalized,
        output_path=str(output_path),
        sma_windows=[5, 20],
        ema_windows=[12, 26],
        rsi_window=14,
    )
    
    print(f"  Computed features for {len(df_features)} rows")
    print(f"  Columns: {list(df_features.columns)}")
    print()
    
    # Step 3: Show sample output
    print("Step 3: Sample features (last 5 rows per symbol):")
    for symbol in df_features["symbol"].unique().sort():
        symbol_df = df_features.filter(pl.col("symbol") == symbol).tail(5)
        print(f"\n{symbol}:")
        print(symbol_df.select(["trade_date", "sma_5", "sma_20", "ema_12", "rsi_14"]))
    print()
    
    # Step 4: Validate ClickHouse compatibility
    print("Step 4: Validating ClickHouse compatibility...")
    if LOADER_AVAILABLE:
        try:
            loader = ClickHouseLoader()
            stats = loader.load_parquet_files(
                table='features_equity_indicators',
                source_path=str(output_path),
                dry_run=True,
            )
            print(f"  ✓ Schema validation passed")
            print(f"  ✓ Would load {stats['total_rows']} rows from {stats['files_loaded']} file(s)")
        except Exception as e:
            print(f"  ✗ Validation failed: {e}")
    else:
        print("  ⚠ Skipped (loader not available)")
    print()
    
    # Step 5: Summary
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"✓ Features generated: {output_path}")
    print(f"✓ Total rows: {len(df_features)}")
    print(f"✓ Symbols: {df_features['symbol'].unique().sort().to_list()}")
    print(f"✓ Date range: {df_features['trade_date'].min()} to {df_features['trade_date'].max()}")
    print()
    print("Next steps:")
    print("  1. Start ClickHouse (if not running): docker-compose up -d clickhouse")
    print("  2. Load features into ClickHouse:")
    print(f"     python -m warehouse.loader.batch_loader \\")
    print(f"       --table features_equity_indicators \\")
    print(f"       --source {output_path}")
    print("  3. Query features in ClickHouse:")
    print("     SELECT symbol, trade_date, sma_5, ema_12, rsi_14")
    print("     FROM champion_market.features_equity_indicators")
    print("     ORDER BY symbol, trade_date DESC LIMIT 10;")
    print()


if __name__ == "__main__":
    main()
