"""BSE ETL tasks for Prefect orchestration."""

import time
from datetime import date, timedelta
from pathlib import Path

import mlflow
import polars as pl
import structlog
from prefect import task
from prefect.tasks import task_input_hash

from src.parsers.polars_bse_parser import PolarsBseParser
from src.scrapers.bse_bhavcopy import BseBhavcopyScraper

logger = structlog.get_logger()


@task(
    name="scrape-bse-bhavcopy",
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=24),
)
def scrape_bse_bhavcopy(trade_date: date) -> str | None:
    """Scrape BSE bhavcopy for a given date.

    Args:
        trade_date: Trading date to scrape

    Returns:
        Path to downloaded CSV file, or None if download fails
    """
    start_time = time.time()
    logger.info("starting_bse_bhavcopy_scrape", trade_date=str(trade_date))

    try:
        scraper = BseBhavcopyScraper()
        csv_path = scraper.scrape(trade_date)

        duration = time.time() - start_time

        logger.info(
            "bse_bhavcopy_scrape_complete",
            trade_date=str(trade_date),
            file_path=str(csv_path),
            duration_seconds=duration,
        )

        # Log metrics to MLflow
        mlflow.log_metric("bse_scrape_duration_seconds", duration)
        mlflow.log_param("bse_trade_date", str(trade_date))

        return str(csv_path)

    except Exception as e:
        logger.warning(
            "bse_bhavcopy_scrape_failed",
            trade_date=str(trade_date),
            error=str(e),
            msg="Continuing pipeline without BSE data",
        )
        # Return None to indicate failure but don't raise - pipeline should continue
        return None


@task(
    name="parse-bse-polars",
    retries=2,
    retry_delay_seconds=30,
)
def parse_bse_polars(csv_file_path: str | None, trade_date: date) -> pl.DataFrame | None:
    """Parse BSE bhavcopy CSV to Polars DataFrame.

    Args:
        csv_file_path: Path to CSV file (or None if scrape failed)
        trade_date: Trading date

    Returns:
        Parsed Polars DataFrame with normalized data, or None if parsing fails
    """
    if csv_file_path is None:
        logger.info("skipping_bse_parse", reason="scrape_failed")
        return None

    start_time = time.time()
    logger.info("starting_bse_polars_parse", csv_file_path=csv_file_path)

    try:
        parser = PolarsBseParser()
        df = parser.parse_to_dataframe(
            file_path=Path(csv_file_path),
            trade_date=trade_date,
        )

        duration = time.time() - start_time
        rows = len(df)

        logger.info(
            "bse_polars_parse_complete",
            csv_file_path=csv_file_path,
            rows=rows,
            duration_seconds=duration,
        )

        # Log metrics to MLflow
        mlflow.log_metric("bse_parse_duration_seconds", duration)
        mlflow.log_metric("bse_raw_rows_parsed", rows)

        return df

    except Exception as e:
        logger.warning(
            "bse_polars_parse_failed",
            csv_file_path=csv_file_path,
            error=str(e),
            msg="Continuing pipeline without BSE data",
        )
        # Return None to indicate failure but don't raise
        return None
