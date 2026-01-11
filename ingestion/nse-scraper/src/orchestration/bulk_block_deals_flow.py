"""Prefect flow for bulk and block deals ETL pipeline."""

from datetime import date, timedelta

import structlog
from prefect import flow

from src.tasks.bulk_block_deals_tasks import (
    load_bulk_block_deals_clickhouse,
    parse_bulk_block_deals,
    scrape_bulk_block_deals,
    write_bulk_block_deals_parquet,
)

logger = structlog.get_logger()


@flow(name="bulk-block-deals-etl", log_prints=True)
def bulk_block_deals_etl_flow(
    target_date: str | None = None,
    deal_type: str = "both",
    load_to_clickhouse: bool = True,
    output_base_path: str = "data/lake",
) -> dict:
    """ETL flow for bulk and block deals ingestion.

    This flow:
    1. Scrapes bulk and/or block deals from NSE
    2. Parses JSON data into standardized events
    3. Writes events to partitioned Parquet files
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

    # Step 1: Scrape bulk and block deals
    scraped_files = scrape_bulk_block_deals(
        target_date=target_date,
        deal_type=deal_type,
    )

    results = {
        "target_date": target_date,
        "deal_types_processed": [],
        "total_events": 0,
        "parquet_files": [],
        "clickhouse_rows": 0,
    }

    # Process each deal type
    for dt, file_path in scraped_files.items():
        deal_type_upper = dt.upper()
        results["deal_types_processed"].append(deal_type_upper)

        # Step 2: Parse deals
        events = parse_bulk_block_deals(
            file_path=file_path,
            deal_date=target_date,
            deal_type=deal_type_upper,
        )

        results["total_events"] += len(events)

        if events:
            # Step 3: Write to Parquet
            parquet_file = write_bulk_block_deals_parquet(
                events=events,
                deal_date=target_date,
                deal_type=deal_type_upper,
                output_base_path=output_base_path,
            )

            results["parquet_files"].append(parquet_file)

            # Step 4: Load to ClickHouse (optional)
            if load_to_clickhouse and parquet_file:
                rows_loaded = load_bulk_block_deals_clickhouse(
                    parquet_file=parquet_file,
                    deal_type=deal_type_upper,
                )
                results["clickhouse_rows"] += rows_loaded

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
) -> dict:
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

    summary = {
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

            summary["dates_processed"].append(current_date.isoformat())
            summary["total_events"] += result.get("total_events", 0)
            summary["total_clickhouse_rows"] += result.get("clickhouse_rows", 0)

        except Exception as e:
            logger.error(
                "bulk_block_deals_etl_failed_for_date",
                date=current_date.isoformat(),
                error=str(e),
            )
            summary["failed_dates"].append({
                "date": current_date.isoformat(),
                "error": str(e),
            })

        current_date += timedelta(days=1)

    logger.info(
        "bulk_block_deals_date_range_etl_flow_complete",
        dates_processed=len(summary["dates_processed"]),
        total_events=summary["total_events"],
        failed_dates=len(summary["failed_dates"]),
    )

    return summary
