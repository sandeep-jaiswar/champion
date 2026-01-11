"""Prefect tasks for macro indicator data ingestion."""

import time
from datetime import datetime
from pathlib import Path

import mlflow
import polars as pl
import structlog
from prefect import task

from src.config import config
from src.parsers.macro_indicator_parser import MacroIndicatorParser
from src.scrapers.mospi_macro import MOSPIMacroScraper
from src.scrapers.rbi_macro import RBIMacroScraper

logger = structlog.get_logger()


@task(
    name="scrape-rbi-macro",
    retries=3,
    retry_delay_seconds=60,
)
def scrape_rbi_macro(
    start_date: datetime, end_date: datetime, indicators: list[str] | None = None
) -> str:
    """Scrape RBI macro indicators.

    Args:
        start_date: Start date for data
        end_date: End date for data
        indicators: List of indicator codes to scrape

    Returns:
        Path to downloaded JSON file
    """
    start_time = time.time()
    logger.info("starting_rbi_macro_scrape", start_date=start_date, end_date=end_date)

    try:
        scraper = RBIMacroScraper()
        json_path = scraper.scrape(start_date, end_date, indicators)
        scraper.close()

        duration = time.time() - start_time

        logger.info(
            "rbi_macro_scrape_complete",
            file_path=str(json_path),
            duration_seconds=duration,
        )

        mlflow.log_metric("rbi_scrape_duration_seconds", duration)
        mlflow.log_param("start_date", start_date.isoformat())
        mlflow.log_param("end_date", end_date.isoformat())

        return str(json_path)

    except Exception as e:
        logger.error("rbi_macro_scrape_failed", error=str(e))
        raise


@task(
    name="scrape-mospi-macro",
    retries=3,
    retry_delay_seconds=60,
)
def scrape_mospi_macro(
    start_date: datetime, end_date: datetime, indicators: list[str] | None = None
) -> str:
    """Scrape MOSPI macro indicators.

    Args:
        start_date: Start date for data
        end_date: End date for data
        indicators: List of indicator codes to scrape

    Returns:
        Path to downloaded JSON file
    """
    start_time = time.time()
    logger.info("starting_mospi_macro_scrape", start_date=start_date, end_date=end_date)

    try:
        scraper = MOSPIMacroScraper()
        json_path = scraper.scrape(start_date, end_date, indicators)
        scraper.close()

        duration = time.time() - start_time

        logger.info(
            "mospi_macro_scrape_complete",
            file_path=str(json_path),
            duration_seconds=duration,
        )

        mlflow.log_metric("mospi_scrape_duration_seconds", duration)
        mlflow.log_param("start_date", start_date.isoformat())
        mlflow.log_param("end_date", end_date.isoformat())

        return str(json_path)

    except Exception as e:
        logger.error("mospi_macro_scrape_failed", error=str(e))
        raise


@task(
    name="parse-macro-indicators",
    retries=2,
    retry_delay_seconds=30,
)
def parse_macro_indicators(json_file_path: str) -> pl.DataFrame:
    """Parse macro indicator JSON file.

    Args:
        json_file_path: Path to JSON file

    Returns:
        Polars DataFrame with macro indicator data
    """
    start_time = time.time()
    logger.info("starting_macro_parse", file=json_file_path)

    try:
        parser = MacroIndicatorParser()
        df = parser.parse(Path(json_file_path))

        duration = time.time() - start_time

        logger.info(
            "macro_parse_complete",
            rows=len(df),
            duration_seconds=duration,
        )

        mlflow.log_metric("parse_duration_seconds", duration)
        mlflow.log_metric("rows_parsed", len(df))
        mlflow.log_metric("unique_indicators", df.select("indicator_code").unique().height)

        return df

    except Exception as e:
        logger.error("macro_parse_failed", file=json_file_path, error=str(e))
        raise


@task(
    name="merge-macro-dataframes",
    retries=2,
    retry_delay_seconds=30,
)
def merge_macro_dataframes(dataframes: list[pl.DataFrame]) -> pl.DataFrame:
    """Merge multiple macro indicator DataFrames.

    Args:
        dataframes: List of DataFrames to merge

    Returns:
        Merged DataFrame
    """
    start_time = time.time()
    logger.info("starting_macro_merge", num_dataframes=len(dataframes))

    try:
        # Filter out empty DataFrames
        non_empty_dfs = [df for df in dataframes if len(df) > 0]

        if not non_empty_dfs:
            logger.warning("No non-empty DataFrames to merge")
            return MacroIndicatorParser()._create_empty_dataframe()

        # Concatenate all DataFrames
        merged_df = pl.concat(non_empty_dfs, how="vertical")

        # Remove duplicates based on entity_id (indicator_code + date)
        merged_df = merged_df.unique(subset=["entity_id"], keep="last")

        # Sort by indicator and date
        merged_df = merged_df.sort(["indicator_code", "indicator_date"])

        duration = time.time() - start_time

        logger.info(
            "macro_merge_complete",
            total_rows=len(merged_df),
            duration_seconds=duration,
        )

        mlflow.log_metric("merge_duration_seconds", duration)
        mlflow.log_metric("merged_rows", len(merged_df))

        return merged_df

    except Exception as e:
        logger.error("macro_merge_failed", error=str(e))
        raise


@task(
    name="write-macro-parquet",
    retries=2,
    retry_delay_seconds=30,
)
def write_macro_parquet(df: pl.DataFrame, start_date: datetime, end_date: datetime) -> str:
    """Write macro indicator DataFrame to Parquet.

    Args:
        df: Macro indicator DataFrame
        start_date: Start date for data
        end_date: End date for data

    Returns:
        Path to written Parquet file
    """
    start_time = time.time()
    logger.info("starting_parquet_write", rows=len(df))

    try:
        # Create output directory
        output_dir = Path("data/lake/macro/indicators")
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = (
            output_dir
            / f"macro_indicators_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
        )

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

        mlflow.log_metric("parquet_write_duration_seconds", duration)
        mlflow.log_artifact(str(output_path))

        return str(output_path)

    except Exception as e:
        logger.error("parquet_write_failed", error=str(e))
        raise


@task(
    name="load-macro-clickhouse",
    retries=2,
    retry_delay_seconds=30,
)
def load_macro_clickhouse(parquet_path: str) -> int:
    """Load macro indicators from Parquet to ClickHouse.

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

        # Convert metadata column to Map type for ClickHouse
        # If metadata is None, convert to empty map
        df = df.with_columns(
            pl.when(pl.col("metadata").is_null())
            .then(pl.lit("{}"))
            .otherwise(pl.col("metadata"))
            .alias("metadata")
        )

        # Convert DataFrame to records
        records = df.to_dicts()

        # Parse metadata JSON strings to dicts for ClickHouse Map type
        for record in records:
            if record.get("metadata"):
                import json

                try:
                    record["metadata"] = json.loads(record["metadata"])
                except (json.JSONDecodeError, TypeError):
                    record["metadata"] = {}
            else:
                record["metadata"] = {}

        # Insert into ClickHouse
        client.insert(
            "macro_indicators",
            [list(r.values()) for r in records],
            column_names=[
                "event_id",
                "event_time",
                "ingest_time",
                "source",
                "schema_version",
                "entity_id",
                "indicator_date",
                "indicator_code",
                "indicator_name",
                "indicator_category",
                "value",
                "unit",
                "frequency",
                "source_url",
                "metadata",
            ],
        )

        rows_loaded = len(records)
        duration = time.time() - start_time

        logger.info(
            "clickhouse_load_complete",
            rows=rows_loaded,
            duration_seconds=duration,
        )

        mlflow.log_metric("clickhouse_load_duration_seconds", duration)
        mlflow.log_metric("rows_loaded", rows_loaded)

        return rows_loaded

    except Exception as e:
        logger.error("clickhouse_load_failed", error=str(e))
        raise
