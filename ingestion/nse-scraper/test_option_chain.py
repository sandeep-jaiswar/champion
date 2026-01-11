#!/usr/bin/env python
"""Quick test script for option chain scraper.

This script demonstrates the option chain scraper functionality
without requiring a full deployment or extensive setup.
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.scrapers.option_chain import OptionChainScraper
from src.utils.logger import get_logger

logger = get_logger(__name__)


def test_single_scrape():
    """Test single option chain scrape."""
    logger.info("Testing single option chain scrape")

    output_dir = Path("./data/option_chain_test")
    output_dir.mkdir(parents=True, exist_ok=True)

    scraper = OptionChainScraper()

    try:
        # Test with NIFTY (most reliable symbol)
        logger.info("Scraping NIFTY option chain")
        df = scraper.scrape(symbol="NIFTY", output_dir=output_dir)

        if len(df) > 0:
            logger.info(
                "Scrape successful",
                rows=len(df),
                strikes=df["strike_price"].n_unique(),
                expiries=df["expiry_date"].n_unique(),
            )

            # Write to Parquet
            scraper._write_parquet(df, output_dir, "NIFTY")
            logger.info("Parquet written successfully")

            # Display sample data
            print("\n=== Sample Data (first 5 rows) ===")
            print(df.head(5))

            print("\n=== Summary Statistics ===")
            print(f"Total rows: {len(df)}")
            print(f"Unique strikes: {df['strike_price'].n_unique()}")
            print(f"Unique expiries: {df['expiry_date'].n_unique()}")
            print(f"Option types: {df['option_type'].unique().to_list()}")

            # Check for nulls
            print("\n=== Null Value Check ===")
            null_counts = df.null_count()
            print(null_counts)

            # Check IV and OI ranges
            print("\n=== IV and OI Ranges ===")
            if "implied_volatility" in df.columns:
                iv_stats = df.select("implied_volatility").describe()
                print("Implied Volatility:")
                print(iv_stats)

            if "open_interest" in df.columns:
                oi_stats = df.select("open_interest").describe()
                print("\nOpen Interest:")
                print(oi_stats)

            return True
        else:
            logger.error("No data returned from scraper")
            return False

    except Exception as e:
        logger.error("Scrape failed", error=str(e), exc_info=True)
        return False

    finally:
        scraper.close()


def test_continuous_scrape():
    """Test continuous scraping (1 iteration for demo)."""
    logger.info("Testing continuous scrape (single iteration)")

    output_dir = Path("./data/option_chain_test")
    output_dir.mkdir(parents=True, exist_ok=True)

    scraper = OptionChainScraper()

    try:
        # Run for 1 minute with 1 minute interval (just one iteration)
        scraper.scrape_continuous(
            symbol="NIFTY",
            interval_minutes=1,
            duration_minutes=1,
            output_dir=output_dir,
        )

        logger.info("Continuous scrape test complete")
        return True

    except Exception as e:
        logger.error("Continuous scrape failed", error=str(e), exc_info=True)
        return False

    finally:
        scraper.close()


if __name__ == "__main__":
    print("=" * 60)
    print("NSE Option Chain Scraper - Test Script")
    print("=" * 60)
    print()

    success = test_single_scrape()

    if success:
        print("\n✅ Test completed successfully!")
        print("\nCheck the following:")
        print("- data/option_chain_test/ directory for Parquet files")
        print("- Verify IV ranges are reasonable (5-50%)")
        print("- Verify OI values are populated")
        sys.exit(0)
    else:
        print("\n❌ Test failed!")
        sys.exit(1)
