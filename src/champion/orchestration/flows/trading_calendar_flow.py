"""Prefect flow for trading calendar ETL pipeline."""

import os
from datetime import datetime

import mlflow
from prefect import flow

from champion.orchestration.tasks.trading_calendar_tasks import (
    load_trading_calendar_clickhouse,
    parse_trading_calendar,
    scrape_trading_calendar,
    write_trading_calendar_parquet,
)

# Configure MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


@flow(name="trading-calendar-etl", log_prints=True)
def trading_calendar_etl_flow(
    year: int | None = None,
    load_to_clickhouse: bool = True,
) -> dict:
    """Complete ETL flow for NSE trading calendar.

    Args:
        year: Year to process (defaults to current year)
        load_to_clickhouse: Whether to load data to ClickHouse

    Returns:
        Dictionary with flow results
    """
    # Default to current year
    if year is None:
        year = datetime.now().year

    # Start MLflow run
    experiment_name = "nse-trading-calendar-etl"
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=f"trading-calendar-etl-{year}"):
        # Log parameters
        mlflow.log_param("year", year)
        mlflow.log_param("load_to_clickhouse", load_to_clickhouse)

        # Step 1: Scrape
        json_path = scrape_trading_calendar(year)

        # Step 2: Parse
        df = parse_trading_calendar(json_path, year)

        # Step 3: Write to Parquet
        parquet_path = write_trading_calendar_parquet(df, year)

        # Step 4: Load to ClickHouse (optional)
        rows_loaded = 0
        if load_to_clickhouse:
            rows_loaded = load_trading_calendar_clickhouse(parquet_path)

        return {
            "year": year,
            "json_path": json_path,
            "parquet_path": parquet_path,
            "rows_loaded": rows_loaded,
            "success": True,
        }


if __name__ == "__main__":
    # Run for current year
    result = trading_calendar_etl_flow()
    print(f"Trading calendar ETL complete: {result}")
