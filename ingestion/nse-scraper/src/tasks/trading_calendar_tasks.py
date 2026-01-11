"""Prefect tasks for trading calendar ingestion."""

import time
from datetime import date
from pathlib import Path

import mlflow
import polars as pl
import structlog
from prefect import task
from prefect.tasks import task_input_hash

from src.config import config
from src.parsers.trading_calendar_parser import TradingCalendarParser
from src.scrapers.trading_calendar import TradingCalendarScraper
from src.utils import metrics

logger = structlog.get_logger()


@task(
    name="scrape-trading-calendar",
    retries=3,
    retry_delay_seconds=60,
)
def scrape_trading_calendar(year: int) -> str:
    """Scrape NSE trading calendar for a given year.

    Args:
        year: Year to scrape

    Returns:
        Path to downloaded JSON file

    Raises:
        RuntimeError: If download fails after retries
    """
    start_time = time.time()
    logger.info("starting_trading_calendar_scrape", year=year)

    try:
        # Use scraper to download
        scraper = TradingCalendarScraper()
        json_path = scraper.scrape(year=year)
        scraper.close()

        duration = time.time() - start_time

        logger.info(
            "trading_calendar_scrape_complete",
            year=year,
            file_path=str(json_path),
            duration_seconds=duration,
        )

        # Log metrics to MLflow
        mlflow.log_metric("scrape_duration_seconds", duration)
        mlflow.log_param("year", year)

        return str(json_path)

    except Exception as e:
        logger.error("trading_calendar_scrape_failed", year=year, error=str(e))
        raise


@task(
    name="parse-trading-calendar",
    retries=2,
    retry_delay_seconds=30,
)
def parse_trading_calendar(json_file_path: str, year: int) -> pl.DataFrame:
    """Parse trading calendar JSON file.

    Args:
        json_file_path: Path to NSE calendar JSON file
        year: Year for calendar

    Returns:
        Polars DataFrame with trading calendar data
    """
    start_time = time.time()
    logger.info("starting_trading_calendar_parse", file=json_file_path)

    try:
        parser = TradingCalendarParser()
        df = parser.parse(Path(json_file_path), year)

        duration = time.time() - start_time

        logger.info(
            "trading_calendar_parse_complete",
            rows=len(df),
            duration_seconds=duration,
        )

        # Log metrics
        mlflow.log_metric("parse_duration_seconds", duration)
        mlflow.log_metric("rows_parsed", len(df))
        mlflow.log_metric(
            "trading_days_count", df.filter(pl.col("is_trading_day"))["is_trading_day"].sum()
        )
        mlflow.log_metric(
            "holidays_count",
            df.filter(pl.col("day_type") == "MARKET_HOLIDAY")["day_type"].count(),
        )

        return df

    except Exception as e:
        logger.error("trading_calendar_parse_failed", file=json_file_path, error=str(e))
        raise


@task(
    name="write-trading-calendar-parquet",
    retries=2,
    retry_delay_seconds=30,
)
def write_trading_calendar_parquet(df: pl.DataFrame, year: int) -> str:
    """Write trading calendar DataFrame to Parquet.

    Args:
        df: Trading calendar DataFrame
        year: Year for partitioning

    Returns:
        Path to written Parquet file
    """
    start_time = time.time()
    logger.info("starting_parquet_write", rows=len(df))

    try:
        # Create output directory
        output_dir = Path("data/lake/reference/trading_calendar") / f"year={year}"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / f"trading_calendar_{year}.parquet"

        # Write to Parquet
        df.write_parquet(
            output_path,
            compression="snappy",
            use_pyarrow=True,
        )

        duration = time.time() - start_time

        logger.info(
            "parquet_write_complete",
            path=str(output_path),
            duration_seconds=duration,
        )

        # Log metrics
        mlflow.log_metric("parquet_write_duration_seconds", duration)
        mlflow.log_artifact(str(output_path))

        return str(output_path)

    except Exception as e:
        logger.error("parquet_write_failed", error=str(e))
        raise


@task(
    name="load-trading-calendar-clickhouse",
    retries=2,
    retry_delay_seconds=30,
)
def load_trading_calendar_clickhouse(parquet_path: str) -> int:
    """Load trading calendar from Parquet to ClickHouse.

    Args:
        parquet_path: Path to Parquet file

    Returns:
        Number of rows loaded
    """
    start_time = time.time()
    logger.info("starting_clickhouse_load", parquet_path=parquet_path)

    try:
        import clickhouse_connect

        # Read Parquet
        df = pl.read_parquet(parquet_path)

        # Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host=config.clickhouse.host if hasattr(config, "clickhouse") else "localhost",
            port=config.clickhouse.port if hasattr(config, "clickhouse") else 9000,
            username=config.clickhouse.username
            if hasattr(config, "clickhouse")
            else "champion_user",
            password=config.clickhouse.password
            if hasattr(config, "clickhouse")
            else "champion_pass",
            database=config.clickhouse.database
            if hasattr(config, "clickhouse")
            else "champion_market",
        )

        # Convert DataFrame to records
        records = df.to_dicts()

        # Insert into ClickHouse
        client.insert(
            "trading_calendar",
            records,
            column_names=[
                "trade_date",
                "is_trading_day",
                "day_type",
                "holiday_name",
                "exchange",
                "year",
                "month",
                "day",
                "weekday",
            ],
        )

        rows_loaded = len(records)
        duration = time.time() - start_time

        logger.info(
            "clickhouse_load_complete",
            rows=rows_loaded,
            duration_seconds=duration,
        )

        # Log metrics
        mlflow.log_metric("clickhouse_load_duration_seconds", duration)
        mlflow.log_metric("rows_loaded", rows_loaded)

        return rows_loaded

    except Exception as e:
        logger.error("clickhouse_load_failed", error=str(e))
        raise
