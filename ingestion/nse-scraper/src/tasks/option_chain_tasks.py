"""Prefect tasks for option chain scraper."""

from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import structlog
from prefect import task

from src.scrapers.option_chain import OptionChainScraper

logger = structlog.get_logger()


@task(name="scrape-option-chain", retries=2)
def scrape_option_chain(
    symbol: str,
    output_dir: str = "data/option_chain",
    save_raw_json: bool = True,
) -> dict:
    """Scrape option chain data for a symbol and write to Parquet.

    This task scrapes option chain data from NSE and writes it to
    partitioned Parquet files for downstream analytics.

    Args:
        symbol: Underlying symbol (e.g., NIFTY, BANKNIFTY, RELIANCE)
        output_dir: Base directory for output files
        save_raw_json: If True, also save raw JSON response

    Returns:
        Dictionary with scraping results (rows, file_path, etc.)

    Raises:
        Exception: If scraping fails
    """
    logger.info(
        "starting_option_chain_scrape",
        symbol=symbol,
        output_dir=output_dir,
    )

    scraper = OptionChainScraper()
    output_path = Path(output_dir)

    try:
        # Scrape data
        df = scraper.scrape(
            symbol=symbol,
            output_dir=output_path if save_raw_json else None,
        )

        if len(df) == 0:
            logger.warning("no_option_chain_data", symbol=symbol)
            return {
                "symbol": symbol,
                "rows": 0,
                "success": False,
                "message": "No data returned",
            }

        # Write to Parquet with partitioning
        scraper._write_parquet(df, output_path, symbol)

        result = {
            "symbol": symbol,
            "rows": len(df),
            "strikes": df["strike_price"].n_unique(),
            "expiries": df["expiry_date"].n_unique(),
            "success": True,
            "timestamp": datetime.now().isoformat(),
        }

        logger.info(
            "option_chain_scrape_complete",
            **result,
        )

        return result

    finally:
        scraper.close()


@task(name="scrape-multiple-option-chains", retries=2)
def scrape_multiple_option_chains(
    symbols: list[str],
    output_dir: str = "data/option_chain",
    save_raw_json: bool = True,
) -> list[dict[str, Any]]:
    """Scrape option chain data for multiple symbols.

    Args:
        symbols: List of underlying symbols
        output_dir: Base directory for output files
        save_raw_json: If True, also save raw JSON responses

    Returns:
        List of dictionaries with scraping results for each symbol
    """
    logger.info(
        "starting_multi_symbol_scrape",
        symbols=symbols,
        count=len(symbols),
    )

    results = []

    for symbol in symbols:
        try:
            result = scrape_option_chain(
                symbol=symbol,
                output_dir=output_dir,
                save_raw_json=save_raw_json,
            )
            results.append(result)
        except (httpx.RequestError, httpx.HTTPError, RuntimeError) as e:
            # Log and continue for expected errors
            logger.error(
                "symbol_scrape_failed",
                symbol=symbol,
                error=str(e),
            )
            results.append(
                {
                    "symbol": symbol,
                    "success": False,
                    "error": str(e),
                }
            )

    logger.info(
        "multi_symbol_scrape_complete",
        total=len(symbols),
        successful=sum(1 for r in results if r.get("success")),
        failed=sum(1 for r in results if not r.get("success")),
    )

    return results


@task(name="read-option-chain-parquet", retries=2)
def read_option_chain_parquet(
    base_path: str,
    symbol: str,
    date: str | None = None,
) -> pl.DataFrame:
    """Read option chain Parquet data for a symbol.

    Args:
        base_path: Base path for data lake
        symbol: Underlying symbol
        date: Optional date filter in YYYY-MM-DD format

    Returns:
        Polars DataFrame with option chain data

    Raises:
        FileNotFoundError: If data doesn't exist
    """
    if date:
        partition_path = Path(base_path) / f"date={date}" / f"symbol={symbol}"
    else:
        partition_path = Path(base_path) / f"symbol={symbol}"

    logger.info(
        "reading_option_chain_parquet",
        partition_path=str(partition_path),
        symbol=symbol,
        date=date,
    )

    # Read all parquet files in the partition
    df = pl.scan_parquet(partition_path / "**/*.parquet").collect()

    logger.info(
        "option_chain_parquet_read_complete",
        partition_path=str(partition_path),
        rows=len(df),
        columns=len(df.columns),
    )

    return df
