#!/usr/bin/env python3
"""
Demonstration script for computing technical indicators.

This script:
1. Generates sample normalized OHLC data
2. Computes technical indicators (SMA, EMA, RSI)
3. Writes features to Parquet files
4. Shows sample output

Usage:
    python demo_features.py
"""

import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import structlog

# Add src to path to import features module
sys.path.insert(0, str(Path(__file__).parent.parent))

from features.indicators import compute_features

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ]
)

logger = structlog.get_logger()


def generate_sample_normalized_data(num_symbols: int = 5, num_days: int = 50) -> pl.DataFrame:
    """
    Generate sample normalized OHLC data for testing.
    
    Args:
        num_symbols: Number of stock symbols to generate
        num_days: Number of trading days
        
    Returns:
        DataFrame with normalized OHLC data
    """
    symbols = [f"SYMBOL{i:03d}" for i in range(num_symbols)]
    base_date = date(2024, 1, 1)
    
    logger.info("Generating sample data", symbols=num_symbols, days=num_days)
    
    records = []
    for symbol in symbols:
        base_price = 100.0 + (hash(symbol) % 400)
        
        for day in range(num_days):
            trade_date = base_date + timedelta(days=day)
            
            # Create realistic price variations
            daily_var = (day % 10) - 5  # Oscillates
            trend = day * 0.5  # Slight upward trend
            close_price = base_price + daily_var + trend
            
            record = {
                "symbol": symbol,
                "trade_date": trade_date,
                "open": close_price - 1.0,
                "high": close_price + 2.0,
                "low": close_price - 2.0,
                "close": close_price,
                "volume": 1000000 + day * 10000,
            }
            records.append(record)
    
    df = pl.DataFrame(records)
    logger.info("Sample data generated", total_rows=len(df))
    
    return df


def main():
    """Run the feature generation demonstration."""
    logger.info("=== Technical Indicators Demonstration ===")
    
    # Step 1: Generate sample normalized OHLC data
    logger.info("Step 1: Generating sample normalized OHLC data")
    df_normalized = generate_sample_normalized_data(num_symbols=5, num_days=50)
    
    logger.info("Sample normalized data (first 5 rows):")
    print(df_normalized.head())
    print()
    
    # Step 2: Compute features without writing to disk
    logger.info("Step 2: Computing technical indicators")
    df_features = compute_features(
        df=df_normalized,
        sma_windows=[5, 20],
        ema_windows=[12, 26],
        rsi_window=14,
    )
    
    logger.info("Features computed", rows=len(df_features))
    logger.info("Sample features (first 10 rows):")
    print(df_features.head(10))
    print()
    
    # Step 3: Show statistics
    logger.info("Step 3: Feature statistics")
    
    for symbol in df_features["symbol"].unique().sort():
        symbol_df = df_features.filter(pl.col("symbol") == symbol)
        
        # Calculate statistics (drop nulls for indicators that need warmup)
        stats = {
            "symbol": symbol,
            "rows": len(symbol_df),
            "avg_sma_5": symbol_df["sma_5"].drop_nulls().mean(),
            "avg_sma_20": symbol_df["sma_20"].drop_nulls().mean(),
            "avg_ema_12": symbol_df["ema_12"].drop_nulls().mean(),
            "avg_ema_26": symbol_df["ema_26"].drop_nulls().mean(),
            "avg_rsi_14": symbol_df["rsi_14"].drop_nulls().mean(),
        }
        
        logger.info("Symbol statistics", **stats)
    
    print()
    
    # Step 4: Write to Parquet
    logger.info("Step 4: Writing features to Parquet")
    output_path = Path(__file__).parent.parent.parent.parent / "data" / "lake" / "features" / "equity"
    
    df_features_saved = compute_features(
        df=df_normalized,
        output_path=str(output_path),
        sma_windows=[5, 20],
        ema_windows=[12, 26],
        rsi_window=14,
        partition_cols=None,  # No partitioning for this demo
    )
    
    logger.info("Features written to", path=str(output_path))
    
    # Verify the output
    parquet_files = list(output_path.rglob("*.parquet"))
    logger.info("Parquet files created", count=len(parquet_files))
    for pf in parquet_files:
        file_size = pf.stat().st_size / 1024  # KB
        logger.info("File details", name=pf.name, size_kb=f"{file_size:.2f}")
    
    print()
    logger.info("=== Demonstration Complete ===")
    logger.info("Features are ready to be loaded into ClickHouse using:")
    logger.info("  python -m warehouse.loader.batch_loader \\")
    logger.info("    --table features_equity_indicators \\")
    logger.info(f"    --source {output_path}")


if __name__ == "__main__":
    main()
