"""Orchestration flow for macro indicator data ingestion.

Integrates RBI and MOSPI data sources into a unified pipeline.
"""

import mlflow
import structlog
from prefect import flow

from src.tasks.macro_tasks import (
    load_macro_clickhouse,
    merge_macro_dataframes,
    parse_macro_indicators,
    scrape_mospi_macro,
    scrape_rbi_macro,
    write_macro_parquet,
)

logger = structlog.get_logger()


@flow(
    name="macro-indicators-etl",
    description="ETL flow for RBI and MOSPI macro indicators",
    flow_run_name="macro-etl-{start_date}-{end_date}",
)
def macro_indicators_flow(
    start_date,
    end_date,
    rbi_indicators=None,
    mospi_indicators=None,
    load_to_clickhouse: bool = True,
):
    """Complete ETL flow for macro indicators.

    Args:
        start_date: Start date for data (datetime)
        end_date: End date for data (datetime)
        rbi_indicators: List of RBI indicator codes (None = all defaults)
        mospi_indicators: List of MOSPI indicator codes (None = all defaults)
        load_to_clickhouse: Whether to load data to ClickHouse

    Returns:
        Path to final Parquet file
    """
    logger.info(
        "starting_macro_etl_flow",
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        load_to_clickhouse=load_to_clickhouse,
    )

    # Start MLflow run
    mlflow.set_experiment("macro-indicators-etl")
    with mlflow.start_run(run_name=f"macro-etl-{start_date.strftime('%Y%m%d')}"):
        # Log parameters
        mlflow.log_param("start_date", start_date.isoformat())
        mlflow.log_param("end_date", end_date.isoformat())
        mlflow.log_param("load_to_clickhouse", load_to_clickhouse)
        mlflow.log_param("rbi_indicators", rbi_indicators or "default")
        mlflow.log_param("mospi_indicators", mospi_indicators or "default")

        try:
            # Step 1: Scrape RBI data
            logger.info("step_1_scrape_rbi")
            rbi_json_path = scrape_rbi_macro(start_date, end_date, rbi_indicators)

            # Step 2: Scrape MOSPI data
            logger.info("step_2_scrape_mospi")
            mospi_json_path = scrape_mospi_macro(start_date, end_date, mospi_indicators)

            # Step 3: Parse both sources
            logger.info("step_3_parse_data")
            rbi_df = parse_macro_indicators(rbi_json_path)
            mospi_df = parse_macro_indicators(mospi_json_path)

            # Step 4: Merge DataFrames
            logger.info("step_4_merge_dataframes")
            merged_df = merge_macro_dataframes([rbi_df, mospi_df])

            # Step 5: Write to Parquet
            logger.info("step_5_write_parquet")
            parquet_path = write_macro_parquet(merged_df, start_date, end_date)

            # Step 6: Load to ClickHouse (optional)
            if load_to_clickhouse:
                logger.info("step_6_load_clickhouse")
                rows_loaded = load_macro_clickhouse(parquet_path)
                logger.info("clickhouse_load_complete", rows=rows_loaded)

            logger.info("macro_etl_flow_complete", parquet_path=parquet_path)
            mlflow.log_param("status", "SUCCESS")

            return parquet_path

        except Exception as e:
            logger.error("macro_etl_flow_failed", error=str(e))
            mlflow.log_param("status", "FAILED")
            mlflow.log_param("error", str(e))
            raise
