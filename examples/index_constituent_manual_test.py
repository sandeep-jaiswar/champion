#!/usr/bin/env python3
"""Manual test script for index constituent pipeline.

This script tests the index constituent ETL pipeline without requiring
external services or network access.
"""

import json
import sys
from datetime import date
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from champion.parsers.index_constituent_parser import IndexConstituentParser


def create_sample_data(output_dir: Path) -> Path:
    """Create sample NIFTY50 data for testing.

    Args:
        output_dir: Directory to save sample JSON file

    Returns:
        Path to created JSON file
    """
    sample_data = {
        "name": "NIFTY 50",
        "index_name": "NIFTY50",
        "scraped_at": "2026-01-11T10:00:00+05:30",
        "data": [
            {
                "symbol": "RELIANCE",
                "series": "EQ",
                "open": 2850.0,
                "high": 2875.0,
                "low": 2840.0,
                "close": 2860.0,
                "last": 2860.0,
                "previousClose": 2855.0,
                "change": 5.0,
                "pChange": 0.18,
                "totalTradedVolume": 5000000,
                "totalTradedValue": 14300000000,
                "indexWeight": 10.5,
                "ffmc": 1500000.0,
                "sharesForIndex": 500000000,
                "meta": {
                    "isin": "INE002A01018",
                    "companyName": "Reliance Industries Ltd.",
                    "sector": "Energy",
                    "industry": "Refineries",
                },
            },
            {
                "symbol": "HDFCBANK",
                "series": "EQ",
                "open": 1650.0,
                "high": 1670.0,
                "low": 1645.0,
                "close": 1660.0,
                "last": 1660.0,
                "previousClose": 1655.0,
                "change": 5.0,
                "pChange": 0.30,
                "totalTradedVolume": 3000000,
                "totalTradedValue": 4980000000,
                "indexWeight": 8.5,
                "ffmc": 850000.0,
                "sharesForIndex": 400000000,
                "meta": {
                    "isin": "INE040A01034",
                    "companyName": "HDFC Bank Limited",
                    "sector": "Financial Services",
                    "industry": "Banks",
                },
            },
            {
                "symbol": "TCS",
                "series": "EQ",
                "open": 3900.0,
                "high": 3950.0,
                "low": 3890.0,
                "close": 3920.0,
                "last": 3920.0,
                "previousClose": 3910.0,
                "change": 10.0,
                "pChange": 0.26,
                "totalTradedVolume": 2000000,
                "totalTradedValue": 7840000000,
                "indexWeight": 7.2,
                "ffmc": 720000.0,
                "sharesForIndex": 300000000,
                "meta": {
                    "isin": "INE467B01029",
                    "companyName": "Tata Consultancy Services Ltd.",
                    "sector": "Information Technology",
                    "industry": "IT Services & Consulting",
                },
            },
        ],
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    json_file = output_dir / "NIFTY50_constituents.json"

    with open(json_file, "w") as f:
        json.dump(sample_data, f, indent=2)

    print(f"✅ Created sample data: {json_file}")
    return json_file


def test_parser(json_file: Path):
    """Test the parser with sample data.

    Args:
        json_file: Path to JSON file with sample data
    """
    print("\n" + "=" * 70)
    print("Testing Index Constituent Parser")
    print("=" * 70)

    parser = IndexConstituentParser()

    # Parse the sample data
    events = parser.parse(
        file_path=json_file,
        index_name="NIFTY50",
        effective_date=date(2026, 1, 11),
        action="ADD",
    )

    print(f"\n✅ Parsed {len(events)} constituent events")

    # Display sample event
    if events:
        print("\nSample Event:")
        print(f"  Event ID: {events[0]['event_id']}")
        print(f"  Source: {events[0]['source']}")
        print(f"  Entity ID: {events[0]['entity_id']}")
        print("\n  Payload:")
        payload = events[0]["payload"]
        print(f"    Index: {payload['index_name']}")
        print(f"    Symbol: {payload['symbol']}")
        print(f"    Company: {payload['company_name']}")
        print(f"    ISIN: {payload['isin']}")
        print(f"    Weight: {payload['weight']}%")
        print(f"    Sector: {payload['sector']}")
        print(f"    Industry: {payload['industry']}")
        print(f"    Action: {payload['action']}")
        print(f"    Effective Date: {payload['effective_date']} (days since epoch)")

    return events


def test_parquet_writer(parser: IndexConstituentParser, events: list, output_dir: Path):
    """Test writing events to Parquet.

    Args:
        parser: Parser instance
        events: List of events to write
        output_dir: Output directory for Parquet files
    """
    print("\n" + "=" * 70)
    print("Testing Parquet Writer")
    print("=" * 70)

    parquet_file = parser.write_parquet(
        events=events,
        output_base_path=output_dir,
        index_name="NIFTY50",
        effective_date=date(2026, 1, 11),
    )

    print(f"\n✅ Wrote Parquet file: {parquet_file}")
    print(f"  Size: {parquet_file.stat().st_size / 1024:.2f} KB")

    # Read and verify
    import polars as pl

    df = pl.read_parquet(parquet_file)
    print(f"  Rows: {len(df)}")
    print(f"  Columns: {len(df.columns)}")

    print("\n  Sample Data:")
    print(df.select(["symbol", "company_name", "weight", "sector"]).head(3))

    return parquet_file


def main():
    """Main entry point."""
    print("=" * 70)
    print("Index Constituent Pipeline Manual Test")
    print("=" * 70)

    # Setup test directories
    test_dir = Path("/tmp/index_constituent_test")
    test_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Step 1: Create sample data
        json_file = create_sample_data(test_dir / "raw")

        # Step 2: Test parser
        parser = IndexConstituentParser()
        events = test_parser(json_file)

        # Step 3: Test Parquet writer
        parquet_file = test_parquet_writer(parser, events, test_dir / "lake")

        # Summary
        print("\n" + "=" * 70)
        print("✅ All Tests Passed!")
        print("=" * 70)
        print(f"\nTest artifacts saved in: {test_dir}")
        print(f"  Raw JSON: {json_file}")
        print(f"  Parquet: {parquet_file}")

        return 0

    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ Test Failed")
        print("=" * 70)
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
