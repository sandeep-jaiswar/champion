from datetime import date
from pathlib import Path

import polars as pl
import structlog
from prefect import flow

from champion.orchestration.flows.flows import load_clickhouse, normalize_polars, write_parquet
from champion.utils.idempotency import (
    check_idempotency_marker,
    create_idempotency_marker,
    is_task_completed,
)
from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

logger = structlog.get_logger()


@flow(name="raw-to-normalized", log_prints=True)
def raw_to_normalized_flow(
    trade_date: date,
    base_path: str | None = None,
    clickhouse_host: str | None = None,
    clickhouse_port: int | None = None,
    clickhouse_user: str | None = None,
    clickhouse_password: str | None = None,
    clickhouse_database: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Idempotent job: read from ClickHouse raw table, normalize, write Parquet, load normalized table.

    - Skips if Parquet idempotency marker already exists for the date.
    - If no rows in raw table, creates a zero-row idempotency marker and exits.
    - Optionally skips the ClickHouse load when `dry_run=True`.
    """
    # Resolve base path and expected output file
    resolved_base = Path(base_path) if base_path else Path("data/lake")
    year = trade_date.year
    month = trade_date.month
    day = trade_date.day
    partition_path = (
        resolved_base
        / "normalized"
        / "equity_ohlc"
        / f"year={year}"
        / f"month={month:02d}"
        / f"day={day:02d}"
    )
    expected_output = partition_path / f"bhavcopy_{trade_date.strftime('%Y%m%d')}.parquet"

    # Early idempotency check
    if is_task_completed(expected_output, trade_date.isoformat()):
        marker = check_idempotency_marker(expected_output, trade_date.isoformat())
        logger.info(
            "raw_to_normalized_skipping_already_completed",
            output_file=str(expected_output),
            marker=marker,
        )
        return {
            "status": "skipped",
            "output_file": str(expected_output),
            "rows": marker.get("rows") if marker else 0,
        }

    # Read raw rows: prefer local lake when dry_run, otherwise read from ClickHouse
    date_str = trade_date.strftime("%Y-%m-%d")
    date_int = trade_date.strftime("%Y%m%d")

    if dry_run:
        # Try to read raw Parquet from lake for dry-run/testing
        raw_parquet = (
            resolved_base
            / "raw"
            / "equity_ohlc"
            / f"year={year}"
            / f"month={month:02d}"
            / f"day={day:02d}"
            / f"bhavcopy_{date_int}.parquet"
        )
        if raw_parquet.exists():
            try:
                df = pl.read_parquet(raw_parquet)
            except Exception as e:
                logger.error(f"failed_reading_local_raw_parquet error={e}")
                df = pl.DataFrame([])
        else:
            logger.warning(f"dry_run_no_local_raw_parquet path={raw_parquet}")
            df = pl.DataFrame([])

    else:
        import os

        loader = ClickHouseLoader(
            host=clickhouse_host or os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=clickhouse_port or int(os.getenv("CLICKHOUSE_PORT", "8123")),
            user=clickhouse_user or os.getenv("CLICKHOUSE_USER"),
            password=clickhouse_password or os.getenv("CLICKHOUSE_PASSWORD"),
            database=clickhouse_database or os.getenv("CLICKHOUSE_DATABASE"),
        )
        try:
            loader.connect()
        except Exception as e:
            logger.error("raw_to_normalized_clickhouse_connect_failed", error=str(e))
            raise

        query = f"SELECT * FROM {loader.database}.raw_equity_ohlc WHERE toString(TradDt) IN ('{date_str}','{date_int}')"

        try:
            # Try to use client.query_df if available
            try:
                pdf = loader.client.query_df(query)
                df = pl.from_pandas(pdf)
            except AttributeError:
                res = loader.client.query(query)
                cols = list(res.column_names)
                rows = [tuple(r) for r in res.result_rows]
                if not rows:
                    df = pl.DataFrame([])
                else:
                    df = pl.DataFrame(rows, schema=cols)
        finally:
            try:
                loader.disconnect()
            except Exception:
                pass

    # If no rows found, create idempotency marker and exit
    if df is None or len(df) == 0:
        # Ensure partition exists
        partition_path.mkdir(parents=True, exist_ok=True)
        create_idempotency_marker(
            output_file=expected_output,
            trade_date=trade_date.isoformat(),
            rows=0,
            metadata={"source": "clickhouse_raw", "note": "no_rows"},
        )
        logger.info("raw_to_normalized_no_raw_rows", trade_date=str(trade_date))
        return {"status": "no_rows", "output_file": str(expected_output), "rows": 0}

    # Normalize using existing task (call directly to keep logic in tasks)
    normalized_df = normalize_polars(df)

    # Write Parquet (this function is idempotent)
    parquet_path = write_parquet(
        df=normalized_df, trade_date=trade_date, base_path=str(resolved_base)
    )

    # Optionally load to ClickHouse
    if dry_run:
        return {
            "status": "completed_dry_run",
            "parquet_file": parquet_path,
            "rows": len(normalized_df),
        }

    load_result = load_clickhouse(
        parquet_file=parquet_path,
        table="normalized_equity_ohlc",
        host=clickhouse_host,
        port=clickhouse_port,
        user=clickhouse_user,
        password=clickhouse_password,
        database=clickhouse_database,
    )

    return {
        "status": "completed",
        "parquet_file": parquet_path,
        "rows": len(normalized_df),
        "load_result": load_result,
    }
