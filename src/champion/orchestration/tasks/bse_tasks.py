"""Prefect tasks for BSE equity data ETL.

This module provides task wrappers for BSE scraping and parsing,
used by combined equity flows that integrate NSE and BSE data.
"""

from datetime import date
from pathlib import Path

import polars as pl
import structlog

try:
    from prefect import task
except Exception:  # pragma: no cover - fallback for environments without prefect

    def task(*_args, **_kwargs):
        def _decorator(func):
            return func

        return _decorator


from champion.parsers.polars_bse_parser import PolarsBseParser
from champion.scrapers.nse.bse_bhavcopy import BseBhavcopyScraper

logger = structlog.get_logger()


@task(
    name="scrape-bse-bhavcopy",
    retries=2,
    retry_delay_seconds=30,
)
def scrape_bse_bhavcopy(trade_date: date) -> str:
    """Scrape BSE bhavcopy data for a given date.

    Args:
        trade_date: Date to scrape for

    Returns:
        Path to downloaded CSV file
    """
    from champion.utils.circuit_breaker_registry import bse_breaker

    scraper = BseBhavcopyScraper()
    logger.info("scraping_bse_bhavcopy", trade_date=trade_date)
    # Wrap scraper call with circuit breaker
    # The BSE scraper expects `target_date` named parameter
    csv_path = bse_breaker.call(scraper.scrape, target_date=trade_date)
    logger.info("bse_bhavcopy_scraped", trade_date=trade_date, path=csv_path)
    return str(csv_path)


@task(
    name="parse-bse-polars",
    retries=1,
    retry_delay_seconds=10,
)
def parse_bse_polars(csv_path: str, trade_date: date) -> pl.DataFrame | None:
    """Parse BSE bhavcopy CSV into Polars DataFrame.

    Args:
        csv_path: Path to CSV file
        trade_date: Date the data represents

    Returns:
        Parsed DataFrame or None if parsing fails/empty
    """
    try:
        parser = PolarsBseParser()
        logger.info("parsing_bse_bhavcopy", path=csv_path, trade_date=trade_date)
        df = parser.parse(Path(csv_path), trade_date)
        if df is None or len(df) == 0:
            logger.warning("bse_bhavcopy_empty_after_parse", path=csv_path, trade_date=trade_date)
            return None
        logger.info("bse_bhavcopy_parsed", rows=len(df), columns=len(df.columns))
        return df
    except (FileNotFoundError, OSError) as e:
        logger.error("bse_file_read_failed", error=str(e), path=csv_path, retryable=True)
        return None
    except ValueError as e:
        logger.error("bse_parsing_validation_failed", error=str(e), path=csv_path, retryable=False)
        return None
    except Exception as e:
        logger.critical("fatal_bse_parsing_error", error=str(e), path=csv_path, retryable=False)
        return None
