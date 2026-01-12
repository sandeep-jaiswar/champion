"""Orchestration flow for macro indicator data ingestion.

Integrates RBI and MOSPI data sources into a unified pipeline.
"""

import mlflow
import structlog
from prefect import flow

from champion.orchestration.tasks.macro_tasks import (
    load_macro_clickhouse,
    try_sources_in_order,
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
    source_order: list[str] | None = None,
    fail_on_empty: bool = False,
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

        source_order = source_order or ["MoSPI", "RBI", "DEA", "NITI Aayog"]
        mlflow.log_param("source_order", source_order)

        try:
            # Step 1: Try sources in order
            logger.info("step_1_try_sources", source_order=source_order)
            df, chosen_source = try_sources_in_order(
                source_order=source_order,
                start_date=start_date,
                end_date=end_date,
                rbi_indicators=rbi_indicators,
                mospi_indicators=mospi_indicators,
            )

            if chosen_source is None or df.height == 0:
                mlflow.log_param("status", "EMPTY")
                logger.warning("macro_no_data", source_order=source_order)
                if fail_on_empty:
                    raise RuntimeError("No macro data retrieved from any source")
                return None

            mlflow.log_param("chosen_source", chosen_source)

            # Step 2: Write to Parquet
            logger.info("step_2_write_parquet")
            parquet_path = write_macro_parquet(df, start_date, end_date)

            # Step 3: Load to ClickHouse (optional)
            if load_to_clickhouse:
                logger.info("step_3_load_clickhouse")
                rows_loaded = load_macro_clickhouse(parquet_path)
                logger.info("clickhouse_load_complete", rows=rows_loaded)

            logger.info("macro_etl_flow_complete", parquet_path=parquet_path, source=chosen_source)
            mlflow.log_param("status", "SUCCESS")

            return parquet_path

        except Exception as e:
            logger.error("macro_etl_flow_failed", error=str(e))
            mlflow.log_param("status", "FAILED")
            mlflow.log_param("error", str(e))
            raise
