#!/usr/bin/env python3
"""
Symbol Master Enrichment Demo

This script demonstrates the full symbol master enrichment pipeline:
1. Download NSE EQUITY_L.csv (symbol master)
2. Parse symbol master file
3. Read existing bhavcopy data from data lake
4. Enrich symbol master with FinInstrmId from bhavcopy
5. Create canonical instrument mapping
6. Verify one-to-many cases (e.g., IBULHSGFIN)
7. Write enriched symbol master to Parquet
8. Optionally load to ClickHouse

Usage:
    python run_symbol_enrichment.py [--load-clickhouse]
"""

import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

import polars as pl
from src.config import config
from src.parsers.symbol_master_parser import SymbolMasterParser
from src.parsers.symbol_enrichment import SymbolEnrichment
# SymbolMasterScraper is already defined in src/scrapers/symbol_master.py
from src.scrapers.symbol_master import SymbolMasterScraper
from src.utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """Run the symbol master enrichment pipeline."""
    logger.info("Starting symbol master enrichment pipeline")

    # Step 1: Scrape symbol master from NSE
    logger.info("Step 1: Scraping symbol master from NSE")
    scraper = SymbolMasterScraper()
    try:
        scraper.scrape(dry_run=True)
        symbol_master_path = config.storage.data_dir / "EQUITY_L.csv"
        logger.info("Symbol master downloaded", path=str(symbol_master_path))
    except Exception as e:
        logger.error("Failed to scrape symbol master", error=str(e))
        logger.info("Continuing with existing file if available...")
        symbol_master_path = config.storage.data_dir / "EQUITY_L.csv"
        if not symbol_master_path.exists():
            logger.error("No existing symbol master file found")
            return 1

    # Step 2: Parse symbol master (EQUITY_L.csv) to get basic info
    logger.info("Step 2: Parsing symbol master file")
    try:
        # Read raw CSV to get the dataframe
        df_symbol_master = pl.read_csv(
            symbol_master_path,
            null_values=["-", "", "null", "NULL", "N/A", "NA"],
        )
        logger.info("Symbol master parsed", rows=len(df_symbol_master))
    except Exception as e:
        logger.error("Failed to parse symbol master", error=str(e))
        return 1

    # Step 3: Find bhavcopy data in data lake
    logger.info("Step 3: Looking for bhavcopy data in data lake")
    data_lake_base = Path("data/lake")
    
    # Try both raw and normalized locations
    bhavcopy_patterns = [
        data_lake_base / "raw/equity_ohlc/**/*.parquet",
        data_lake_base / "normalized/equity_ohlc/**/*.parquet",
    ]
    
    bhavcopy_files = []
    for pattern in bhavcopy_patterns:
        bhavcopy_files.extend(list(Path(".").glob(str(pattern))))
    
    if not bhavcopy_files:
        logger.warning(
            "No bhavcopy data found in data lake. Enrichment will be limited to EQUITY_L data only."
        )
        logger.info("Please run ETL pipeline first to populate bhavcopy data.")
        # Continue anyway with basic symbol master
        enriched_df = df_symbol_master
    else:
        logger.info("Found bhavcopy files", count=len(bhavcopy_files))
        
        # Step 4: Enrich symbol master with FinInstrmId from bhavcopy
        logger.info("Step 4: Enriching symbol master with bhavcopy data")
        enricher = SymbolEnrichment()
        enriched_df = enricher.enrich_from_bhavcopy(df_symbol_master, bhavcopy_files[:30])  # Use first 30 files for demo
        logger.info("Enrichment complete", rows=len(enriched_df))
        
        # Step 5: Create canonical mapping
        logger.info("Step 5: Creating canonical instrument mapping")
        canonical_mapping = enricher.create_canonical_mapping(enriched_df)
        logger.info("Canonical mapping created", mappings=len(canonical_mapping))
        
        # Step 6: Verify one-to-many cases
        logger.info("Step 6: Verifying one-to-many ticker cases")
        verification_stats = enricher.verify_one_to_many_cases(enriched_df)
        logger.info("Verification complete", stats=verification_stats)
        
        # Look for IBULHSGFIN specifically
        ibulhsgfin_instruments = enriched_df.filter(pl.col("TckrSymb") == "IBULHSGFIN")
        if len(ibulhsgfin_instruments) > 0:
            logger.info(
                "IBULHSGFIN verification",
                count=len(ibulhsgfin_instruments),
                instruments=ibulhsgfin_instruments.select(
                    ["instrument_id", "TckrSymb", "FinInstrmId", "SctySrs", "FinInstrmNm"]
                ).to_dicts()[:5],  # Show first 5
            )

    # Step 7: Write enriched symbol master to Parquet
    logger.info("Step 7: Writing enriched symbol master to Parquet")
    output_dir = data_lake_base / "reference/symbol_master"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create versioned output with date
    output_date = datetime.now().strftime("%Y%m%d")
    output_path = output_dir / f"symbol_master_{output_date}.parquet"
    
    enriched_df.write_parquet(output_path, compression="snappy")
    logger.info("Enriched symbol master written", path=str(output_path), rows=len(enriched_df))
    
    # Also write latest version
    latest_path = output_dir / "symbol_master_latest.parquet"
    enriched_df.write_parquet(latest_path, compression="snappy")
    logger.info("Latest version written", path=str(latest_path))

    # Step 8: Display summary statistics
    logger.info("=" * 80)
    logger.info("ENRICHMENT SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total instruments: {len(enriched_df)}")
    
    if "TckrSymb" in enriched_df.columns:
        unique_symbols = enriched_df["TckrSymb"].n_unique()
        logger.info(f"Unique ticker symbols: {unique_symbols}")
        
        if "FinInstrmId" in enriched_df.columns:
            multi_instrument = (
                enriched_df.group_by("TckrSymb")
                .agg(pl.col("FinInstrmId").n_unique().alias("count"))
                .filter(pl.col("count") > 1)
            )
            logger.info(f"Symbols with multiple instruments: {len(multi_instrument)}")
    
    logger.info("=" * 80)
    
    # Check if we should load to ClickHouse
    if "--load-clickhouse" in sys.argv:
        logger.info("Step 9: Loading to ClickHouse")
        try:
            import clickhouse_connect
            
            client = clickhouse_connect.get_client(
                host="localhost",
                port=8123,
                username="champion_user",
                password="champion_pass",
                database="champion_market",
            )
            
            # Convert to format suitable for ClickHouse
            # TODO: Implement ClickHouse loading logic
            logger.info("ClickHouse loading not yet implemented")
            
        except Exception as e:
            logger.error("Failed to load to ClickHouse", error=str(e))
    
    logger.info("Symbol master enrichment pipeline complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
