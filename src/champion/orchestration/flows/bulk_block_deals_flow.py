"""Prefect flow for bulk and block deals ETL pipeline."""

import os
from datetime import date, timedelta
from typing import Any

import mlflow
import structlog
from prefect import flow

from champion.orchestration.tasks.bulk_block_deals_tasks import (
    load_bulk_block_deals_clickhouse,
    parse_bulk_block_deals,
    scrape_bulk_block_deals,
    write_bulk_block_deals_parquet,
)

logger = structlog.get_logger()

# Configure MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


@flow(name="bulk-block-deals-etl", log_prints=True)
def bulk_block_deals_etl_flow(
    target_date: str | None = None,
    deal_type: str = "both",
    load_to_clickhouse: bool = True,
    output_base_path: str = "data/lake",
) -> dict[str, Any]:
    """ETL flow for bulk and block deals ingestion.

    This flow:
    1. Scrapes bulk and/or block deals from NSE
    2. Parses JSON data into standardized events
    3. Writes events to partitioned Parquet files with validation
    4. Optionally loads data into ClickHouse

    Args:
        target_date: Date to scrape in YYYY-MM-DD format (default: yesterday)
        deal_type: Type of deals - 'bulk', 'block', or 'both' (default)
        load_to_clickhouse: Whether to load data into ClickHouse
        output_base_path: Base path for Parquet output

    Returns:
        Dictionary with execution summary
    """
    logger.info(
        "starting_bulk_block_deals_etl_flow",
        target_date=target_date,
        deal_type=deal_type,
        load_to_clickhouse=load_to_clickhouse,
    )

    # Default to yesterday if no date provided
    if target_date is None:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    # Start MLflow run
    mlflow.set_experiment("bulk-block-deals-etl")
    with mlflow.start_run(run_name=f"bulk-block-deals-{target_date}"):
        mlflow.log_param("target_date", target_date)
        mlflow.log_param("deal_type", deal_type)
        mlflow.log_param("load_to_clickhouse", load_to_clickhouse)

        # Step 1: Scrape bulk and block deals
        scraped_files = scrape_bulk_block_deals(
            target_date=target_date,
            deal_type=deal_type,
        )

        results: dict[str, Any] = {
            "target_date": target_date,
            "deal_types_processed": [],
            "total_events": 0,
            "parquet_files": [],
            "clickhouse_rows": 0,
        }

        validation_succeeded = True

        # Process each deal type
        for dt, file_path in scraped_files.items():
            deal_type_upper = dt.upper()
            results["deal_types_processed"].append(deal_type_upper)  # type: ignore

            # Step 2: Parse deals
            events = parse_bulk_block_deals(
                file_path=file_path,
                deal_date=target_date,
                deal_type=deal_type_upper,
            )

            results["total_events"] += len(events)  # type: ignore

            if events:
                # Step 3: Write to Parquet with validation
                try:
                    parquet_file = write_bulk_block_deals_parquet(
                        events=events,
                        deal_date=target_date,
                        deal_type=deal_type_upper,
                        output_base_path=output_base_path,
                    )

                    results["parquet_files"].append(parquet_file)  # type: ignore

                    # Log validation success for this deal type
                    mlflow.log_metric(f"validation_pass_{deal_type_upper.lower()}", 1.0)
                    mlflow.log_metric(f"rows_validated_{deal_type_upper.lower()}", len(events))

                    # Step 4: Load to ClickHouse (optional)
                    if load_to_clickhouse and parquet_file:
                        rows_loaded = load_bulk_block_deals_clickhouse(
                            parquet_file=parquet_file,
                            deal_type=deal_type_upper,
                        )
                        results["clickhouse_rows"] += rows_loaded  # type: ignore

                except ValueError as validation_error:
                    # Log validation failure
                    logger.error(
                        "validation_failed_for_deal_type",
                        deal_type=deal_type_upper,
                        error=str(validation_error),
                    )
                    mlflow.log_metric(f"validation_pass_{deal_type_upper.lower()}", 0.0)
                    mlflow.log_metric(f"validation_failures_{deal_type_upper.lower()}", 1)
                    validation_succeeded = False
                    raise

        # Log overall validation metrics
        mlflow.log_metric("validation_pass_rate", 1.0 if validation_succeeded else 0.0)
        if validation_succeeded:
            mlflow.log_metric("validation_failures", 0)
        mlflow.log_param("status", "SUCCESS" if validation_succeeded else "FAILED")

        logger.info(
            "bulk_block_deals_etl_flow_complete",
            target_date=target_date,
            deal_types=results["deal_types_processed"],
            total_events=results["total_events"],
            clickhouse_rows=results["clickhouse_rows"],
        )

        return results


@flow(name="bulk-block-deals-date-range-etl", log_prints=True)
def bulk_block_deals_date_range_etl_flow(
    start_date: str,
    end_date: str,
    deal_type: str = "both",
    load_to_clickhouse: bool = True,
    output_base_path: str = "data/lake",
) -> dict[str, Any]:
    """ETL flow for bulk and block deals ingestion over a date range.

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive)
        end_date: End date in YYYY-MM-DD format (inclusive)
        deal_type: Type of deals - 'bulk', 'block', or 'both' (default)
        load_to_clickhouse: Whether to load data into ClickHouse
        output_base_path: Base path for Parquet output

    Returns:
        Dictionary with execution summary for all dates
    """
    logger.info(
        "starting_bulk_block_deals_date_range_etl_flow",
        start_date=start_date,
        end_date=end_date,
        deal_type=deal_type,
    )

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    summary: dict[str, Any] = {
        "start_date": start_date,
        "end_date": end_date,
        "dates_processed": [],
        "total_events": 0,
        "total_clickhouse_rows": 0,
        "failed_dates": [],
    }

    current_date = start
    while current_date <= end:
        try:
            result = bulk_block_deals_etl_flow(
                target_date=current_date.isoformat(),
                deal_type=deal_type,
                load_to_clickhouse=load_to_clickhouse,
                output_base_path=output_base_path,
            )

            summary["dates_processed"].append(current_date.isoformat())  # type: ignore
            summary["total_events"] += result.get("total_events", 0)  # type: ignore
            summary["total_clickhouse_rows"] += result.get("clickhouse_rows", 0)  # type: ignore

        except Exception as e:
            logger.error(
                "bulk_block_deals_etl_failed_for_date",
                date=current_date.isoformat(),
                error=str(e),
            )
            summary["failed_dates"].append(  # type: ignore
                {
                    "date": current_date.isoformat(),
                    "error": str(e),
                }
            )

        current_date += timedelta(days=1)

    logger.info(
        "bulk_block_deals_date_range_etl_flow_complete",
        dates_processed=len(summary["dates_processed"]),
        total_events=summary["total_events"],
        failed_dates=len(summary["failed_dates"]),
    )

    return summary
