"""Manual test for macro ETL pipeline.

This script tests the macro ETL pipeline end-to-end without requiring
external dependencies like ClickHouse.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl

from src.parsers.macro_indicator_parser import MacroIndicatorParser
from src.scrapers.mospi_macro import MOSPIMacroScraper
from src.scrapers.rbi_macro import RBIMacroScraper


def test_rbi_scraper():
    """Test RBI scraper."""
    print("\n" + "=" * 60)
    print("Testing RBI Scraper")
    print("=" * 60)

    scraper = RBIMacroScraper()
    start_date = datetime.now() - timedelta(days=60)
    end_date = datetime.now()

    print(f"Scraping RBI data from {start_date.date()} to {end_date.date()}")

    json_path = scraper.scrape(start_date, end_date)
    print(f"✓ JSON file created: {json_path}")
    print(f"  File size: {json_path.stat().st_size:,} bytes")

    scraper.close()
    return json_path


def test_mospi_scraper():
    """Test MOSPI scraper."""
    print("\n" + "=" * 60)
    print("Testing MOSPI Scraper")
    print("=" * 60)

    scraper = MOSPIMacroScraper()
    start_date = datetime.now() - timedelta(days=90)
    end_date = datetime.now()

    print(f"Scraping MOSPI data from {start_date.date()} to {end_date.date()}")

    json_path = scraper.scrape(start_date, end_date)
    print(f"✓ JSON file created: {json_path}")
    print(f"  File size: {json_path.stat().st_size:,} bytes")

    scraper.close()
    return json_path


def test_parser(json_path, source_name):
    """Test macro indicator parser."""
    print("\n" + "=" * 60)
    print(f"Testing Parser for {source_name}")
    print("=" * 60)

    parser = MacroIndicatorParser()
    df = parser.parse(json_path)

    print(f"✓ Parsed {len(df)} records")
    print(f"  Columns: {', '.join(df.columns)}")
    print(f"  Unique indicators: {df['indicator_code'].n_unique()}")
    print(f"  Date range: {df['indicator_date'].min()} to {df['indicator_date'].max()}")
    print(f"  Sources: {', '.join(df['source'].unique().to_list())}")

    # Show sample data
    print("\n  Sample data (first 5 rows):")
    sample = df.select(
        ["indicator_code", "indicator_name", "indicator_date", "value", "unit"]
    ).head(5)
    print(sample)

    # Show indicator summary
    print("\n  Indicators summary:")
    summary = (
        df.group_by("indicator_code")
        .agg(
            [
                df["indicator_name"].first().alias("name"),
                df["value"].count().alias("observations"),
                df["value"].min().alias("min_value"),
                df["value"].max().alias("max_value"),
            ]
        )
        .sort("indicator_code")
    )
    print(summary)

    return df


def test_data_quality(df):
    """Test data quality checks."""
    print("\n" + "=" * 60)
    print("Data Quality Checks")
    print("=" * 60)

    # Check nulls
    null_counts = {col: df[col].null_count() for col in df.columns}
    print(f"✓ Null counts: {null_counts}")

    # Check for duplicates
    duplicates = df.group_by(["indicator_code", "indicator_date"]).count().filter(pl.col("count") > 1)
    print(f"✓ Duplicate entries: {len(duplicates)}")

    # Check value ranges
    print(f"✓ Value range: {df['value'].min():.2f} to {df['value'].max():.2f}")

    # Check dates
    print(
        f"✓ Date range: {df['indicator_date'].min()} to {df['indicator_date'].max()}"
    )

    # Check categories
    categories = df["indicator_category"].unique().to_list()
    print(f"✓ Categories: {', '.join(categories)}")


def test_parquet_write(df):
    """Test writing to Parquet."""
    print("\n" + "=" * 60)
    print("Testing Parquet Write")
    print("=" * 60)

    output_dir = Path("data/lake/macro/indicators")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "test_macro_indicators.parquet"
    df.write_parquet(output_path, compression="snappy", use_pyarrow=True)

    print(f"✓ Parquet file written: {output_path}")
    print(f"  File size: {output_path.stat().st_size:,} bytes")

    # Read back and verify
    df_read = pl.read_parquet(output_path)
    print(f"✓ Read back {len(df_read)} records")
    assert len(df_read) == len(df), "Row count mismatch!"

    return output_path


def main():
    """Run all manual tests."""
    print("\n" + "=" * 60)
    print("Macro ETL Pipeline - Manual Test")
    print("=" * 60)

    try:
        # Test RBI scraper
        rbi_json = test_rbi_scraper()
        rbi_df = test_parser(rbi_json, "RBI")

        # Test MOSPI scraper
        mospi_json = test_mospi_scraper()
        mospi_df = test_parser(mospi_json, "MOSPI")

        # Merge DataFrames
        print("\n" + "=" * 60)
        print("Merging DataFrames")
        print("=" * 60)

        merged_df = pl.concat([rbi_df, mospi_df], how="vertical")
        merged_df = merged_df.unique(subset=["entity_id"], keep="last")
        merged_df = merged_df.sort(["indicator_code", "indicator_date"])

        print(f"✓ Merged {len(merged_df)} total records")
        print(f"  RBI records: {len(rbi_df)}")
        print(f"  MOSPI records: {len(mospi_df)}")
        print(f"  Unique indicators: {merged_df['indicator_code'].n_unique()}")

        # Test data quality
        test_data_quality(merged_df)

        # Test Parquet write
        parquet_path = test_parquet_write(merged_df)

        print("\n" + "=" * 60)
        print("✓ All Tests Passed!")
        print("=" * 60)
        print(f"\nFinal output: {parquet_path}")
        print("\nNext steps:")
        print("  1. Run full ETL: python run_macro_etl.py")
        print("  2. Query in ClickHouse (if running)")
        print("  3. Run correlation analysis: python examples/macro_correlation_analysis.py")

    except Exception as e:
        print(f"\n✗ Test Failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
