"""Prefect flows for NSE data pipeline orchestration.

This module defines Prefect flows and tasks for:
- Scraping NSE bhavcopy data
- Parsing and normalizing with Polars
- Writing to Parquet format
- Loading into ClickHouse
- Logging metrics to MLflow

The main flow runs on a schedule (weekdays at 6pm IST) and handles
the complete ETL pipeline with retry logic and observability.
"""

import os
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import mlflow
import polars as pl
import structlog
from prefect import flow, task
from prefect.tasks import task_input_hash

from src.config import config
from src.parsers.polars_bhavcopy_parser import PolarsBhavcopyParser
from src.scrapers.bhavcopy import BhavcopyScraper
from src.utils import metrics

logger = structlog.get_logger()

# Configure MLflow tracking URI from environment
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

logger.info("mlflow_configured", tracking_uri=MLFLOW_TRACKING_URI)


@task(
    name="scrape-bhavcopy",
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=24),
)
def scrape_bhavcopy(trade_date: date) -> str:
    """Scrape NSE bhavcopy for a given date.

    Args:
        trade_date: Trading date to scrape

    Returns:
        Path to downloaded CSV file

    Raises:
        RuntimeError: If download fails after retries
    """
    start_time = time.time()
    logger.info("starting_bhavcopy_scrape", trade_date=str(trade_date))

    try:
        # Format date for NSE URL (YYYYMMDD)
        date_str = trade_date.strftime("%Y%m%d")
        url = config.nse.bhavcopy_url.format(date=date_str)

        # Download file
        local_path = config.storage.data_dir / f"BhavCopy_NSE_CM_{date_str}.csv"
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Use scraper to download
        scraper = BhavcopyScraper()
        if not scraper.download_file(url, str(local_path)):
            raise RuntimeError(f"Failed to download bhavcopy for {trade_date}")

        duration = time.time() - start_time

        logger.info(
            "bhavcopy_scrape_complete",
            trade_date=str(trade_date),
            file_path=str(local_path),
            duration_seconds=duration,
        )

        # Log metrics to MLflow
        mlflow.log_metric("scrape_duration_seconds", duration)
        mlflow.log_param("trade_date", str(trade_date))

        return str(local_path)

    except Exception as e:
        logger.error("bhavcopy_scrape_failed", trade_date=str(trade_date), error=str(e))
        raise


@task(
    name="parse-polars-raw",
    retries=2,
    retry_delay_seconds=30,
)
def parse_polars_raw(csv_file_path: str, trade_date: date) -> pl.DataFrame:
    """Parse raw bhavcopy CSV to Polars DataFrame.

    Args:
        csv_file_path: Path to CSV file
        trade_date: Trading date

    Returns:
        Parsed Polars DataFrame with raw data

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        Exception: If parsing fails
    """
    start_time = time.time()
    logger.info("starting_polars_parse", csv_file_path=csv_file_path)

    try:
        parser = PolarsBhavcopyParser()
        df = parser.parse_to_dataframe(
            file_path=Path(csv_file_path),
            trade_date=trade_date,
        )

        duration = time.time() - start_time
        rows = len(df)

        logger.info(
            "polars_parse_complete",
            csv_file_path=csv_file_path,
            rows=rows,
            duration_seconds=duration,
        )

        # Log metrics to MLflow
        mlflow.log_metric("parse_duration_seconds", duration)
        mlflow.log_metric("raw_rows_parsed", rows)

        return df

    except Exception as e:
        logger.error("polars_parse_failed", csv_file_path=csv_file_path, error=str(e))
        raise


@task(
    name="normalize-polars",
    retries=2,
    retry_delay_seconds=30,
)
def normalize_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize and validate Polars DataFrame.

    This task performs data quality checks and normalization:
    - Filter out invalid rows
    - Validate data types
    - Add derived columns if needed

    Args:
        df: Raw Polars DataFrame

    Returns:
        Normalized Polars DataFrame

    Raises:
        ValueError: If validation fails
    """
    start_time = time.time()
    logger.info("starting_normalization", input_rows=len(df))

    try:
        # Filter out rows with missing critical fields
        initial_rows = len(df)
        df = df.filter(
            pl.col("TckrSymb").is_not_null()
            & (pl.col("TckrSymb") != "")
            & pl.col("ClsPric").is_not_null()
            & (pl.col("ClsPric") > 0)
        )

        # Validate we have data
        if len(df) == 0:
            raise ValueError("No valid rows after normalization")

        filtered_rows = initial_rows - len(df)
        duration = time.time() - start_time

        logger.info(
            "normalization_complete",
            input_rows=initial_rows,
            output_rows=len(df),
            filtered_rows=filtered_rows,
            duration_seconds=duration,
        )

        # Log metrics to MLflow
        mlflow.log_metric("normalize_duration_seconds", duration)
        mlflow.log_metric("normalized_rows", len(df))
        mlflow.log_metric("filtered_rows", filtered_rows)

        return df

    except Exception as e:
        logger.error("normalization_failed", error=str(e))
        raise


@task(
    name="write-parquet",
    retries=2,
    retry_delay_seconds=30,
)
def write_parquet(
    df: pl.DataFrame,
    trade_date: date,
    base_path: str | None = None,
) -> str:
    """Write DataFrame to Parquet with partitioned layout.

    Args:
        df: DataFrame to write
        trade_date: Trading date for partitioning
        base_path: Base path for data lake (defaults to config)

    Returns:
        Path to written Parquet file

    Raises:
        Exception: If write fails
    """
    start_time = time.time()
    logger.info("starting_parquet_write", rows=len(df), trade_date=str(trade_date))

    try:
        parser = PolarsBhavcopyParser()

        resolved_base_path: Path
        if base_path is None:
            resolved_base_path = Path("data/lake")
        else:
            resolved_base_path = Path(base_path)

        output_file = parser.write_parquet(
            df=df,
            trade_date=trade_date,
            base_path=resolved_base_path,
        )

        duration = time.time() - start_time
        file_size_mb = output_file.stat().st_size / (1024 * 1024)

        logger.info(
            "parquet_write_complete",
            output_file=str(output_file),
            rows=len(df),
            size_mb=file_size_mb,
            duration_seconds=duration,
        )

        # Log metrics to MLflow
        mlflow.log_metric("write_duration_seconds", duration)
        mlflow.log_metric("parquet_size_mb", file_size_mb)
        mlflow.log_metric("rows_written", len(df))

        # Track Prometheus metrics
        metrics.parquet_write_success.labels(table="normalized_equity_ohlc").inc()

        return str(output_file)

    except Exception as e:
        logger.error("parquet_write_failed", error=str(e))
        # Track failure in Prometheus
        metrics.parquet_write_failed.labels(table="normalized_equity_ohlc").inc()
        raise


@task(
    name="load-clickhouse",
    retries=3,
    retry_delay_seconds=60,
)
def load_clickhouse(
    parquet_file: str,
    table: str = "normalized_equity_ohlc",
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
) -> dict:
    """Load Parquet file into ClickHouse table.

    Args:
        parquet_file: Path to Parquet file
        table: Target ClickHouse table name
        host: ClickHouse host (defaults to localhost)
        port: ClickHouse port (defaults to 8123)
        user: ClickHouse user (defaults to champion_user)
        password: ClickHouse password (defaults to champion_pass)
        database: ClickHouse database (defaults to champion_market)

    Returns:
        Dictionary with load statistics

    Raises:
        Exception: If load fails
    """
    start_time = time.time()
    logger.info("starting_clickhouse_load", parquet_file=parquet_file, table=table)

    try:
        # Import here to avoid dependency if ClickHouse not available
        import clickhouse_connect

        # Use defaults from environment or parameters
        ch_host = host or "localhost"
        ch_port = port or 8123
        ch_user = user or "champion_user"
        ch_password = password or "champion_pass"
        ch_database = database or "champion_market"

        # Read Parquet file
        df = pl.read_parquet(parquet_file)
        rows = len(df)

        logger.info("read_parquet_for_load", parquet_file=parquet_file, rows=rows)

        # Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_user,
            password=ch_password,
            database=ch_database,
        )

        # Convert to pandas for clickhouse-connect compatibility
        pdf = df.to_pandas()

        # Insert data
        client.insert_df(
            table=table,
            df=pdf,
            settings={"async_insert": 0, "wait_for_async_insert": 0},
        )

        client.close()

        duration = time.time() - start_time

        stats = {
            "table": table,
            "rows_loaded": rows,
            "duration_seconds": duration,
        }

        logger.info(
            "clickhouse_load_complete",
            table=table,
            rows=rows,
            duration_seconds=duration,
        )

        # Log metrics to MLflow
        mlflow.log_metric("load_duration_seconds", duration)
        mlflow.log_metric("rows_loaded", rows)
        mlflow.log_param("clickhouse_table", table)

        # Track Prometheus metrics
        metrics.clickhouse_load_success.labels(table=table).inc()

        return stats

    except Exception as e:
        logger.error("clickhouse_load_failed", parquet_file=parquet_file, error=str(e))
        # Track failure in Prometheus
        metrics.clickhouse_load_failed.labels(table=table).inc()
        # Don't fail the flow if ClickHouse is unavailable
        logger.warning("continuing_without_clickhouse_load")
        return {
            "table": table,
            "rows_loaded": 0,
            "duration_seconds": 0,
            "error": str(e),
        }


@flow(
    name="nse-bhavcopy-etl",
    description="Complete ETL pipeline for NSE bhavcopy data",
    log_prints=True,
)
def nse_bhavcopy_etl_flow(
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
) -> dict:
    """Main ETL flow for NSE bhavcopy data pipeline.

    This flow orchestrates the complete pipeline:
    1. Scrape bhavcopy from NSE
    2. Parse CSV to Polars DataFrame
    3. Normalize and validate data
    4. Write to Parquet format
    5. Load into ClickHouse (optional)

    All metrics are logged to MLflow for observability and exposed via Prometheus.

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

    Returns:
        Dictionary with pipeline statistics

    Raises:
        Exception: If any critical step fails
    """
    flow_start_time = time.time()

    # Start Prometheus metrics server if requested
    if start_metrics_server_flag:
        try:
            metrics.start_metrics_server(port=metrics_port)
            logger.info("metrics_server_started", port=metrics_port)
        except OSError as e:
            # Server might already be running
            logger.warning("metrics_server_already_running", port=metrics_port, error=str(e))

    # Default to previous business day if not specified
    if trade_date is None:
        today = date.today()
        # Simple logic: go back 1 day for now (should check trading calendar)
        trade_date = today - timedelta(days=1)

    logger.info("starting_etl_flow", trade_date=str(trade_date))

    # Start MLflow run
    with mlflow.start_run(run_name=f"bhavcopy-etl-{trade_date}"):
        try:
            # Log flow parameters
            mlflow.log_param("trade_date", str(trade_date))
            mlflow.log_param("load_to_clickhouse", load_to_clickhouse)

            # Step 1: Scrape bhavcopy
            csv_file = scrape_bhavcopy(trade_date)

            # Step 2: Parse raw CSV
            raw_df = parse_polars_raw(csv_file, trade_date)

            # Step 3: Normalize data
            normalized_df = normalize_polars(raw_df)

            # Step 4: Write to Parquet
            parquet_file = write_parquet(
                df=normalized_df,
                trade_date=trade_date,
                base_path=output_base_path,
            )

            # Step 5: Load to ClickHouse (optional)
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

            # Calculate total flow duration
            flow_duration = time.time() - flow_start_time

            # Prepare result
            result = {
                "trade_date": str(trade_date),
                "csv_file": csv_file,
                "parquet_file": parquet_file,
                "rows_processed": len(normalized_df),
                "flow_duration_seconds": flow_duration,
                "load_stats": load_stats,
                "status": "success",
            }

            logger.info(
                "etl_flow_complete",
                trade_date=str(trade_date),
                rows=len(normalized_df),
                duration_seconds=flow_duration,
            )

            # Log final metrics to MLflow
            mlflow.log_metric("flow_duration_seconds", flow_duration)
            mlflow.log_param("status", "success")

            # Track Prometheus flow duration metric
            metrics.flow_duration.labels(flow_name="nse-bhavcopy-etl", status="success").observe(
                flow_duration
            )

            return result

        except Exception as e:
            flow_duration = time.time() - flow_start_time

            logger.error(
                "etl_flow_failed",
                trade_date=str(trade_date),
                error=str(e),
                duration_seconds=flow_duration,
            )

            # Log failure to MLflow
            mlflow.log_metric("flow_duration_seconds", flow_duration)
            mlflow.log_param("status", "failed")
            mlflow.log_param("error", str(e))

            # Track Prometheus flow duration metric for failure
            metrics.flow_duration.labels(flow_name="nse-bhavcopy-etl", status="failed").observe(
                flow_duration
            )

            raise


# Deployment configuration for scheduling
def create_deployment():
    """Create Prefect deployment with scheduling configuration.

    This function configures the deployment to run:
    - On weekdays (Monday-Friday)
    - At 6:00 PM IST (12:30 PM UTC)
    - With appropriate parameters

    Usage:
        python -m src.orchestration.flows
    """
    from prefect.deployments import Deployment
    from prefect.server.schemas.schedules import CronSchedule

    # Schedule: Weekdays at 6pm IST (12:30pm UTC, considering IST is UTC+5:30)
    # Cron: 30 12 * * 1-5 (12:30 PM UTC on Mon-Fri)
    schedule = CronSchedule(
        cron="30 12 * * 1-5",
        timezone="UTC",
    )

    deployment = Deployment.build_from_flow(
        flow=nse_bhavcopy_etl_flow,
        name="nse-bhavcopy-daily",
        version="1.0.0",
        description="Daily NSE bhavcopy ETL pipeline - runs weekdays at 6pm IST",
        schedule=schedule,
        parameters={
            "load_to_clickhouse": True,
            "output_base_path": "data/lake",
        },
        work_queue_name="default",
        tags=["nse", "bhavcopy", "daily", "production"],
    )

    deployment.apply()

    logger.info(
        "deployment_created",
        name="nse-bhavcopy-daily",
        schedule="weekdays at 6pm IST",
    )

    return deployment


@flow(
    name="nse-option-chain-scrape-flow",
    description="Scrape NSE option chain data for multiple symbols",
    log_prints=True,
)
def nse_option_chain_flow(
    symbols: list[str] | None = None,
    output_dir: str = "data/option_chain",
    save_raw_json: bool = True,
) -> dict:
    """Flow to scrape option chain data for multiple symbols.

    This flow:
    1. Scrapes option chain data from NSE for specified symbols
    2. Parses and writes to Parquet with partitioning
    3. Logs metrics to MLflow

    Args:
        symbols: List of symbols to scrape (default: NIFTY, BANKNIFTY)
        output_dir: Output directory for Parquet files
        save_raw_json: Whether to save raw JSON responses

    Returns:
        Dictionary with scraping results

    Example:
        >>> result = nse_option_chain_flow(
        ...     symbols=["NIFTY", "BANKNIFTY", "RELIANCE"],
        ...     output_dir="data/option_chain"
        ... )
    """
    from src.tasks.option_chain_tasks import scrape_multiple_option_chains

    # Default symbols if not provided
    if symbols is None:
        symbols = ["NIFTY", "BANKNIFTY"]

    logger.info(
        "starting_option_chain_flow",
        symbols=symbols,
        output_dir=output_dir,
    )

    # Start MLflow run
    with mlflow.start_run(run_name=f"option_chain_{'-'.join(symbols)}"):
        mlflow.set_tag("flow_type", "option_chain")
        mlflow.log_param("symbols", ",".join(symbols))
        mlflow.log_param("output_dir", output_dir)

        try:
            # Scrape all symbols
            results = scrape_multiple_option_chains(
                symbols=symbols,
                output_dir=output_dir,
                save_raw_json=save_raw_json,
            )

            # Calculate summary metrics
            total_symbols = len(results)
            successful = sum(1 for r in results if r.get("success"))
            failed = total_symbols - successful
            total_rows = sum(r.get("rows", 0) for r in results)

            summary = {
                "total_symbols": total_symbols,
                "successful": successful,
                "failed": failed,
                "total_rows": total_rows,
                "results": results,
            }

            # Log metrics
            mlflow.log_metric("total_symbols", total_symbols)
            mlflow.log_metric("successful_symbols", successful)
            mlflow.log_metric("failed_symbols", failed)
            mlflow.log_metric("total_rows", total_rows)

            logger.info(
                "option_chain_flow_complete",
                **summary,
            )

            return summary

        except Exception as e:
            logger.error("option_chain_flow_failed", error=str(e))
            mlflow.log_param("error", str(e))
            raise


@flow(
    name="index-constituent-etl",
    description="ETL flow for NSE index constituent data ingestion",
)
def index_constituent_etl_flow(
    indices: list[str] | None = None,
    effective_date: date | None = None,
    load_to_clickhouse: bool = True,
) -> dict[str, Any]:
    """Complete ETL flow for index constituent data.

    This flow:
    1. Scrapes index constituent data from NSE
    2. Parses JSON to event structures
    3. Writes to partitioned Parquet files
    4. Optionally loads into ClickHouse
    5. Logs metrics to MLflow

    Args:
        indices: List of index names to scrape (e.g., ['NIFTY50', 'BANKNIFTY'])
                 If None, defaults to ['NIFTY50', 'BANKNIFTY']
        effective_date: Date when constituents are effective (defaults to today)
        load_to_clickhouse: Whether to load data into ClickHouse

    Returns:
        Dictionary with status and file paths

    Raises:
        Exception: If any step fails after retries
    """
    from src.tasks.index_constituent_tasks import (
        load_index_constituents_clickhouse,
        parse_index_constituents,
        scrape_index_constituents,
        write_index_constituents_parquet,
    )

    start_time = time.time()

    # Default values
    if indices is None:
        indices = ["NIFTY50", "BANKNIFTY"]
    if effective_date is None:
        effective_date = date.today()

    effective_date_str = effective_date.isoformat()

    logger.info(
        "starting_index_constituent_etl_flow",
        indices=indices,
        effective_date=effective_date_str,
        load_to_clickhouse=load_to_clickhouse,
    )

    # Start MLflow run
    experiment_name = "nse-index-constituent-etl"
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=f"index-constituent-etl-{effective_date_str}"):
        try:
            # Log parameters
            mlflow.log_param("indices", ",".join(indices))
            mlflow.log_param("effective_date", effective_date_str)
            mlflow.log_param("load_to_clickhouse", load_to_clickhouse)

            # Step 1: Scrape index constituent data
            scraped_files = scrape_index_constituents(
                indices=indices,
                output_dir=None,  # Use default
            )

            mlflow.log_metric("indices_scraped", len(scraped_files))

            # Process each index
            results = {}
            for index_name, file_path in scraped_files.items():
                logger.info(
                    "processing_index",
                    index_name=index_name,
                    file_path=file_path,
                )

                # Step 2: Parse to events
                events = parse_index_constituents(
                    file_path=file_path,
                    index_name=index_name,
                    effective_date=effective_date_str,
                    action="ADD",
                )

                mlflow.log_metric(f"{index_name}_constituents", len(events))

                # Step 3: Write to Parquet
                parquet_file = write_index_constituents_parquet(
                    events=events,
                    index_name=index_name,
                    effective_date=effective_date_str,
                    output_base_path="data/lake",
                )

                results[index_name] = {
                    "json_file": file_path,
                    "parquet_file": parquet_file,
                    "constituents": len(events),
                }

                # Step 4: Load to ClickHouse (if enabled)
                if load_to_clickhouse and parquet_file:
                    rows_loaded = load_index_constituents_clickhouse(
                        parquet_file=parquet_file,
                        index_name=index_name,
                    )
                    mlflow.log_metric(f"{index_name}_rows_loaded", rows_loaded)
                    results[index_name]["rows_loaded"] = rows_loaded

            # Log overall metrics
            duration = time.time() - start_time
            mlflow.log_metric("total_duration_seconds", duration)
            mlflow.log_metric("total_indices_processed", len(results))

            total_constituents: int = sum(
                int(r.get("constituents", 0)) for r in results.values()  # type: ignore
            )
            mlflow.log_metric("total_constituents", total_constituents)

            logger.info(
                "index_constituent_etl_flow_complete",
                duration_seconds=duration,
                indices_processed=list(results.keys()),
                total_constituents=total_constituents,
            )

            # Return summary
            return {
                "status": "success",
                "duration_seconds": duration,
                "results": results,
            }

        except Exception as e:
            logger.error("index_constituent_etl_flow_failed", error=str(e))
            mlflow.log_param("status", "failed")
            mlflow.log_param("error", str(e))
            raise


if __name__ == "__main__":
    # For local testing, run the flow directly
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        # Create deployment
        print("Creating Prefect deployment...")
        create_deployment()
        print("✅ Deployment created successfully!")
        print("\nTo start the agent:")
        print("  prefect agent start -q default")
    else:
        # Run flow locally for testing
        print("Running flow locally...")
        result = nse_bhavcopy_etl_flow()
        print(f"\n✅ Flow completed: {result}")
