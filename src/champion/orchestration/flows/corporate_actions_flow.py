"""Prefect flow for NSE corporate actions ETL."""

import os
from datetime import date
from pathlib import Path

import mlflow
import structlog
from prefect import flow, task

from champion.config import config
from champion.scrapers.nse.corporate_actions import CorporateActionsScraper

logger = structlog.get_logger()

# Ensure file-based MLflow by default
os.environ.setdefault("MLFLOW_TRACKING_URI", "file:./mlruns")


@task(
    name="scrape-corporate-actions",
    retries=2,
    retry_delay_seconds=30,
)
def scrape_ca_task(effective_date: date | None = None) -> Path:
    """Scrape NSE corporate actions data.

    Args:
        effective_date: Date to scrape for (used for logging; scraper doesn't filter by date)

    Returns:
        Path to downloaded JSON file
    """
    scraper = CorporateActionsScraper()
    logger.info("scraping_corporate_actions", effective_date=effective_date)
    # Note: CorporateActionsScraper.scrape() doesn't accept date parameter
    # It's a stub that logs a warning about manual file download
    scraper.scrape(dry_run=False)
    # Return a placeholder path - actual scraper needs implementation
    ca_dir = Path(config.storage.data_dir) / "lake" / "reference" / "corporate_actions"
    ca_dir.mkdir(parents=True, exist_ok=True)
    return ca_dir


@task(
    name="parse-corporate-actions",
    retries=1,
    retry_delay_seconds=10,
)
def parse_ca_task(ca_dir: Path, effective_date: date | None = None) -> dict:
    """Parse corporate actions data from directory.

    Args:
        ca_dir: Directory where corporate actions data would be
        effective_date: Effective date for events

    Returns:
        Dictionary with parsed events and metadata (empty for stub implementation)
    """
    eff_date = effective_date or date.today()
    logger.info("parsing_corporate_actions", effective_date=eff_date)
    # Stub: CorporateActionsScraper doesn't actually fetch data yet
    # When implemented, this would parse JSON files from ca_dir
    return {
        "events": [],
        "count": 0,
        "effective_date": str(eff_date),
        "status": "stub_implementation_no_data_available",
    }


@task(
    name="write-corporate-actions-parquet",
    retries=1,
    retry_delay_seconds=10,
)
def write_ca_parquet_task(parse_result: dict, output_base_path: str | None = None) -> str:
    """Write parsed corporate actions to Parquet.

    Args:
        parse_result: Output from parse_ca_task
        output_base_path: Base path for data lake

    Returns:
        Path to written Parquet file
    """
    import polars as pl

    events = parse_result["events"]
    if not events:
        logger.warning("no_corporate_actions_to_write")
        return ""

    output_base = Path(output_base_path or config.storage.data_dir) / "lake"
    output_dir = output_base / "reference" / "corporate_actions"
    output_dir.mkdir(parents=True, exist_ok=True)

    eff_date = parse_result["effective_date"]
    out_file = output_dir / f"corporate_actions_{eff_date}.parquet"

    df = pl.DataFrame(events)
    df.write_parquet(out_file, compression="snappy")
    logger.info("wrote_corporate_actions", path=out_file, count=len(events))
    return str(out_file)


@flow(name="corporate-actions-etl", log_prints=True)
def corporate_actions_etl_flow(
    effective_date: date | None = None,
    output_base_path: str | None = None,
    load_to_clickhouse: bool = False,
) -> dict:
    """ETL flow for NSE corporate actions data.

    Args:
        effective_date: Date to process (defaults to today)
        output_base_path: Base path for data lake
        load_to_clickhouse: Whether to load to ClickHouse (stub for now)

    Returns:
        Dictionary with pipeline statistics
    """
    eff_date = effective_date or date.today()

    logger.info(
        "starting_corporate_actions_etl_flow",
        effective_date=eff_date,
        load_to_clickhouse=load_to_clickhouse,
    )

    # Start MLflow run
    mlflow.set_experiment("corporate-actions-etl")
    with mlflow.start_run(run_name=f"ca-etl-{eff_date}"):
        mlflow.log_param("effective_date", str(eff_date))
        mlflow.log_param("load_to_clickhouse", load_to_clickhouse)

        try:
            # Step 1: Scrape
            ca_dir = scrape_ca_task(eff_date)
            mlflow.log_param("ca_directory", str(ca_dir))

            # Step 2: Parse
            parse_result = parse_ca_task(ca_dir, eff_date)
            mlflow.log_metric("events_parsed", parse_result["count"])

            # Step 3: Write to Parquet
            parquet_path = write_ca_parquet_task(parse_result, output_base_path)
            mlflow.log_param("parquet_path", parquet_path)

            # Step 4: (Optional) Load to ClickHouse
            if load_to_clickhouse and parquet_path:
                logger.info("clickhouse_loading_stubbed", path=parquet_path)
                mlflow.log_metric("clickhouse_rows", parse_result["count"])

            result = {
                "status": "completed",
                "effective_date": str(eff_date),
                "events_processed": parse_result["count"],
                "parquet_path": parquet_path,
                "note": parse_result.get("status", ""),
            }

            logger.info("corporate_actions_etl_flow_complete", **result)
            return result

        except Exception as e:
            logger.error("corporate_actions_etl_flow_failed", error=str(e))
            mlflow.log_param("error", str(e))
            raise
