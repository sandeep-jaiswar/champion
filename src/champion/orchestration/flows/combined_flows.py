"""Combined NSE and BSE ETL flows with deduplication.

This module orchestrates ETL for both NSE and BSE data sources:
- Scrapes from both exchanges
- Normalizes to common schema
- Deduplicates by ISIN
- Loads to unified ClickHouse table
- Tolerates source failures
"""

import os
import time
from datetime import date, timedelta

import mlflow
import polars as pl
import structlog
from prefect import flow, task

from champion.orchestration.flows.flows import (
    load_clickhouse,
    normalize_polars,
    parse_polars_raw,
    scrape_bhavcopy,
    write_parquet,
)
from champion.orchestration.tasks.bse_tasks import parse_bse_polars, scrape_bse_bhavcopy
from champion.utils import metrics

logger = structlog.get_logger()

# Configure MLflow tracking URI from environment
os.environ.setdefault("MLFLOW_TRACKING_URI", "file:./mlruns")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


@task(
    name="deduplicate-by-isin",
    retries=1,
    retry_delay_seconds=10,
)
def deduplicate_by_isin(nse_df: pl.DataFrame | None, bse_df: pl.DataFrame | None) -> pl.DataFrame:
    """Deduplicate overlapping symbols by ISIN, preferring NSE data.

    Args:
        nse_df: NSE DataFrame (may be None)
        bse_df: BSE DataFrame (may be None)

    Returns:
        Combined DataFrame with duplicates removed

    Raises:
        ValueError: If both DataFrames are None
    """
    start_time = time.time()
    logger.info("starting_deduplication")

    # Handle edge cases
    if nse_df is None and bse_df is None:
        raise ValueError("Both NSE and BSE DataFrames are None - no data to process")

    if nse_df is None:
        logger.info("nse_data_unavailable", using="bse_only")
        mlflow.log_param("data_sources", "BSE")
        return bse_df  # type: ignore

    if bse_df is None:
        logger.info("bse_data_unavailable", using="nse_only")
        mlflow.log_param("data_sources", "NSE")
        return nse_df

    # Both available - deduplicate
    mlflow.log_param("data_sources", "NSE+BSE")

    nse_rows = len(nse_df)
    bse_rows = len(bse_df)
    logger.info("deduplicating_data", nse_rows=nse_rows, bse_rows=bse_rows)

    # Get ISINs from NSE (these take priority)
    nse_isins = set(nse_df.filter(pl.col("ISIN").is_not_null())["ISIN"].to_list())

    # Filter BSE to exclude symbols already in NSE (by ISIN)
    bse_unique = bse_df.filter(pl.col("ISIN").is_null() | ~pl.col("ISIN").is_in(nse_isins))

    # Combine NSE (all rows) with BSE (unique only)
    combined_df = pl.concat([nse_df, bse_unique], how="vertical")

    duration = time.time() - start_time
    final_rows = len(combined_df)
    bse_unique_rows = len(bse_unique)
    duplicates_removed = bse_rows - bse_unique_rows

    logger.info(
        "deduplication_complete",
        nse_rows=nse_rows,
        bse_rows=bse_rows,
        bse_unique_rows=bse_unique_rows,
        duplicates_removed=duplicates_removed,
        final_rows=final_rows,
        duration_seconds=duration,
    )

    # Log metrics
    mlflow.log_metric("nse_rows", nse_rows)
    mlflow.log_metric("bse_rows", bse_rows)
    mlflow.log_metric("bse_unique_rows", bse_unique_rows)
    mlflow.log_metric("duplicates_removed", duplicates_removed)
    mlflow.log_metric("final_rows", final_rows)
    mlflow.log_metric("dedup_duration_seconds", duration)

    return combined_df


@flow(
    name="combined-equity-etl",
    description="Combined ETL pipeline for NSE and BSE equity data with deduplication",
    log_prints=True,
)
def combined_equity_etl_flow(
    trade_date: date | None = None,
    output_base_path: str | None = None,
    load_to_clickhouse: bool = True,
    clickhouse_host: str | None = None,
    clickhouse_port: int | None = None,
    clickhouse_user: str | None = None,
    clickhouse_password: str | None = None,
    clickhouse_database: str | None = None,
    metrics_port: int = 9090,
    start_metrics_server_flag: bool = True,
    enable_bse: bool = True,
) -> dict:
    """Combined ETL flow for NSE and BSE equity data.

    This flow orchestrates:
    1. Scrape from both NSE and BSE (tolerates failures)
    2. Parse to normalized schema
    3. Deduplicate by ISIN (NSE takes priority)
    4. Write to Parquet
    5. Load to ClickHouse

    Args:
        trade_date: Trading date to process (defaults to previous business day)
        output_base_path: Base path for data lake output
        load_to_clickhouse: Whether to load data to ClickHouse
        clickhouse_host: ClickHouse host
        clickhouse_port: ClickHouse port
        clickhouse_user: ClickHouse user
        clickhouse_password: ClickHouse password
        clickhouse_database: ClickHouse database
        metrics_port: Port for Prometheus metrics server (default: 9090)
        start_metrics_server_flag: Whether to start metrics server (default: True)
        enable_bse: Whether to include BSE data (default: True)

    Returns:
        Dictionary with pipeline statistics

    Raises:
        Exception: If critical steps fail (but tolerates source failures)
    """
    flow_start_time = time.time()

    # Start Prometheus metrics server if requested
    if start_metrics_server_flag:
        try:
            metrics.start_metrics_server(port=metrics_port)
            logger.info("metrics_server_started", port=metrics_port)
        except OSError as e:
            logger.warning("metrics_server_already_running", port=metrics_port, error=str(e))

    # Default to previous business day if not specified
    if trade_date is None:
        today = date.today()
        trade_date = today - timedelta(days=1)

    logger.info("starting_combined_etl_flow", trade_date=str(trade_date), enable_bse=enable_bse)

    # Start MLflow run
    experiment_name = "combined-equity-etl"
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=f"equity-etl-{trade_date}"):
        # Log parameters
        mlflow.log_param("trade_date", str(trade_date))
        mlflow.log_param("enable_bse", enable_bse)
        mlflow.log_param("load_to_clickhouse", load_to_clickhouse)

        try:
            # Step 1: Scrape NSE data
            nse_csv_path = scrape_bhavcopy(trade_date)

            # Step 2: Parse NSE data
            nse_df = parse_polars_raw(nse_csv_path, trade_date)

            # Step 3: Normalize NSE data
            nse_normalized = normalize_polars(nse_df)

            # Step 4: Scrape and parse BSE data (if enabled)
            bse_normalized = None
            if enable_bse:
                bse_csv_path = scrape_bse_bhavcopy(trade_date)
                bse_df = parse_bse_polars(bse_csv_path, trade_date)

                # Normalize BSE data if available
                if bse_df is not None:
                    bse_normalized = normalize_polars(bse_df)

            # Step 5: Deduplicate by ISIN
            combined_df = deduplicate_by_isin(nse_normalized, bse_normalized)

            # Step 6: Write to Parquet
            parquet_file = write_parquet(combined_df, trade_date, output_base_path)

            # Step 7: Load to ClickHouse (optional)
            load_stats = None
            if load_to_clickhouse:
                load_stats = load_clickhouse(
                    parquet_file=parquet_file,
                    table="normalized_equity_ohlc",
                    host=clickhouse_host,
                    port=clickhouse_port,
                    user=clickhouse_user,
                    password=clickhouse_password,
                    database=clickhouse_database,
                )

            # Calculate flow statistics
            flow_duration = time.time() - flow_start_time

            stats = {
                "trade_date": str(trade_date),
                "total_rows": len(combined_df),
                "parquet_file": parquet_file,
                "flow_duration_seconds": flow_duration,
                "load_stats": load_stats,
            }

            logger.info("combined_etl_flow_complete", **stats)

            # Log flow-level metrics
            mlflow.log_metric("flow_duration_seconds", flow_duration)
            mlflow.log_metric("total_rows_final", len(combined_df))

            return stats

        except Exception as e:
            logger.error("combined_etl_flow_failed", error=str(e))
            mlflow.log_param("status", "failed")
            raise


@flow(
    name="bse-only-etl",
    description="BSE-only ETL pipeline for testing",
    log_prints=True,
)
def bse_only_etl_flow(
    trade_date: date | None = None,
    output_base_path: str | None = None,
    load_to_clickhouse: bool = True,
    clickhouse_host: str | None = None,
    clickhouse_port: int | None = None,
    clickhouse_user: str | None = None,
    clickhouse_password: str | None = None,
    clickhouse_database: str | None = None,
) -> dict:
    """BSE-only ETL flow for testing and BSE-specific operations.

    Args:
        trade_date: Trading date to process
        output_base_path: Base path for data lake output
        load_to_clickhouse: Whether to load data to ClickHouse
        clickhouse_host: ClickHouse host
        clickhouse_port: ClickHouse port
        clickhouse_user: ClickHouse user
        clickhouse_password: ClickHouse password
        clickhouse_database: ClickHouse database

    Returns:
        Dictionary with pipeline statistics
    """
    flow_start_time = time.time()

    if trade_date is None:
        today = date.today()
        trade_date = today - timedelta(days=1)

    logger.info("starting_bse_only_etl_flow", trade_date=str(trade_date))

    # Start MLflow run
    experiment_name = "bse-equity-etl"
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=f"bse-etl-{trade_date}"):
        mlflow.log_param("trade_date", str(trade_date))
        mlflow.log_param("source", "BSE")

        try:
            # Scrape BSE data
            bse_csv_path = scrape_bse_bhavcopy(trade_date)

            if bse_csv_path is None:
                raise RuntimeError("Failed to scrape BSE data")

            # Parse BSE data
            bse_df = parse_bse_polars(bse_csv_path, trade_date)

            if bse_df is None:
                raise RuntimeError("Failed to parse BSE data")

            # Normalize
            bse_normalized = normalize_polars(bse_df)

            # Write to Parquet
            parquet_file = write_parquet(bse_normalized, trade_date, output_base_path)

            # Load to ClickHouse (optional)
            load_stats = None
            if load_to_clickhouse:
                load_stats = load_clickhouse(
                    parquet_file=parquet_file,
                    table="normalized_equity_ohlc",
                    host=clickhouse_host,
                    port=clickhouse_port,
                    user=clickhouse_user,
                    password=clickhouse_password,
                    database=clickhouse_database,
                )

            flow_duration = time.time() - flow_start_time

            stats = {
                "trade_date": str(trade_date),
                "total_rows": len(bse_normalized),
                "parquet_file": parquet_file,
                "flow_duration_seconds": flow_duration,
                "load_stats": load_stats,
            }

            logger.info("bse_only_etl_flow_complete", **stats)
            mlflow.log_metric("flow_duration_seconds", flow_duration)
            mlflow.log_metric("total_rows", len(bse_normalized))

            return stats

        except Exception as e:
            logger.error("bse_only_etl_flow_failed", error=str(e))
            mlflow.log_param("status", "failed")
            raise
