from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import structlog

from champion.scrapers.nse.bulk_block_deals import BulkBlockDealsScraper
from champion.storage.parquet_io import write_df_safe
from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader
from champion.utils.idempotency import (
    check_idempotency_marker,
    create_idempotency_marker,
    is_task_completed,
)

logger = structlog.get_logger()


def scrape_bulk_block_deals(
    target_date: str | date,
    deal_type: str = "both",
) -> dict[str, Path]:
    """Scrape bulk and/or block deals for a date."""
    d = target_date if isinstance(target_date, date) else date.fromisoformat(target_date)
    with BulkBlockDealsScraper() as scraper:
        return scraper.scrape(target_date=d, deal_type=deal_type)


def parse_bulk_block_deals(
    file_path: str | Path,
    deal_date: str | date,
    deal_type: str,
) -> list[dict[str, Any]]:
    """Parse CSV deals file into standardized event dictionaries."""
    d = deal_date if isinstance(deal_date, date) else date.fromisoformat(deal_date)
    path = Path(file_path)
    if not path.exists():
        return []

    with path.open() as f:
        reader = csv.reader(f)
        columns = next(reader, [])

    dtype_map = dict.fromkeys(columns, pl.Utf8)
    df = pl.read_csv(
        str(path),
        dtypes=dtype_map,
        infer_schema_length=0,
        try_parse_dates=False,
    )

    def _to_int(val: Any) -> int:
        if val is None or val == "":
            return 0
        if isinstance(val, int):
            return val
        try:
            return int(float(str(val).replace(",", "").strip()))
        except (ValueError, TypeError) as e:
            logger.debug("int_conversion_failed", value=val, error=str(e))
            return 0
        except Exception as e:
            logger.critical("unexpected_int_conversion_error", value=val, error=str(e))
            return 0

    def _to_float(val: Any) -> float:
        if val is None or val == "":
            return 0.0
        if isinstance(val, int | float):
            return float(val)
        try:
            return float(str(val).replace(",", "").strip())
        except (ValueError, TypeError) as e:
            logger.debug("float_conversion_failed", value=val, error=str(e))
            return 0.0
        except Exception as e:
            logger.critical("unexpected_float_conversion_error", value=val, error=str(e))
            return 0.0

    events: list[dict[str, Any]] = []
    for row in df.iter_rows(named=True):
        symbol = str(row.get("Symbol") or row.get("SYMBOL") or "").strip()
        client = str(row.get("ClientName") or row.get("CLIENT_NAME") or "").strip()
        security_name = str(row.get("SecurityName") or row.get("SECURITY_NAME") or "").strip()
        buy_sell_raw = str(
            row.get("Buy/Sell") or row.get("BuySell") or row.get("BUY_SELL") or ""
        ).strip()
        transaction_type = buy_sell_raw.upper() if buy_sell_raw else ""
        quantity = _to_int(row.get("QuantityTraded") or row.get("Qty") or row.get("QTY"))
        price = _to_float(
            row.get("TradePrice/Wght.Avg.Price")
            or row.get("TradePriceWght.Avg.Price")
            or row.get("TradePrice")
            or row.get("PRICE")
        )
        remarks = str(row.get("Remarks") or row.get("REMARKS") or "").strip()

        if symbol and transaction_type and quantity > 0:
            events.append(
                _event(
                    deal_date=d,
                    symbol=symbol,
                    client_name=client,
                    quantity=quantity,
                    avg_price=price,
                    deal_type=deal_type.upper(),
                    transaction_type=transaction_type,
                    security_name=security_name,
                    remarks=remarks,
                    raw_buy_sell=buy_sell_raw,
                )
            )

    return events


def _event(
    deal_date: date,
    symbol: str,
    client_name: str,
    quantity: int,
    avg_price: float,
    deal_type: str,
    transaction_type: str,
    security_name: str,
    remarks: str,
    raw_buy_sell: str,
) -> dict[str, Any]:
    import uuid
    from datetime import UTC, datetime

    now = datetime.now(UTC)
    event_id = str(uuid.uuid4())
    entity_id = f"{symbol}:{deal_type}:{transaction_type}:{deal_date.strftime('%Y%m%d')}"
    return {
        "event_id": event_id,
        "event_time": now,
        "ingest_time": now,
        "source": "nse.bulk_block_deals",
        "schema_version": "1.0.0",
        "entity_id": entity_id,
        "deal_date": deal_date,
        "symbol": symbol.upper(),
        "client_name": client_name,
        "quantity": int(quantity),
        "avg_price": float(avg_price),
        "deal_type": deal_type.upper(),
        "transaction_type": transaction_type.upper(),
        "exchange": "NSE",
        "security_name": security_name,
        "remarks": remarks,
        "raw_buy_sell": raw_buy_sell,
        "year": deal_date.year,
        "month": deal_date.month,
        "day": deal_date.day,
    }


def write_bulk_block_deals_parquet(
    events: list[dict[str, Any]],
    deal_date: str | date,
    deal_type: str,
    output_base_path: str | Path = "data/lake",
) -> str:
    """Write events to partitioned Parquet with validation and return file path.

    This function is idempotent: it checks for an existing marker file before writing.
    If the task has already completed successfully for the given date and deal type,
    it returns the path to the existing file without rewriting.

    Args:
        events: List of event dictionaries to write
        deal_date: Date of the deals
        deal_type: Type of deals (BULK or BLOCK)
        output_base_path: Base path for data lake

    Returns:
        Path to written Parquet file

    Raises:
        ValueError: If validation fails
        OSError: If file write fails
    """
    if not events:
        return ""

    d = deal_date if isinstance(deal_date, date) else date.fromisoformat(deal_date)

    base_path = Path(output_base_path)
    dataset = (
        f"bulk_block_deals/deal_type={deal_type.upper()}"
        f"/year={d.year}/month={d.month:02d}/day={d.day:02d}"
    )

    # Construct expected output path
    output_path = base_path / dataset
    out_file = output_path / "data.parquet"

    # Check idempotency marker using date and deal type as key
    date_key = f"{d.isoformat()}-{deal_type.upper()}"
    if is_task_completed(out_file, date_key):
        marker_data = check_idempotency_marker(out_file, date_key)
        logger.info(
            "bulk_block_deals_already_written_skipping",
            output_file=str(out_file),
            deal_date=str(d),
            deal_type=deal_type,
            rows=marker_data.get("rows", 0) if marker_data else 0,
        )
        return str(out_file)

    logger.info(
        "writing_bulk_block_deals_with_validation",
        event_count=len(events),
        deal_date=str(d),
        deal_type=deal_type,
    )

    # Create DataFrame and coerce types
    df = pl.DataFrame(events).with_columns(
        [
            pl.col("deal_date").cast(pl.Date),
            pl.col("event_time").cast(pl.Datetime, strict=False),
            pl.col("ingest_time").cast(pl.Datetime, strict=False),
            pl.col("quantity").cast(pl.Int64),
            pl.col("avg_price").cast(pl.Float64),
            pl.col("year").cast(pl.Int64),
            pl.col("month").cast(pl.Int64),
            pl.col("day").cast(pl.Int64),
        ]
    )

    # Adjust column names and temporal encodings to match JSON schema expectations:
    # - `trade_date`: ISO date string (schema expects format date)
    # - `price`: numeric price (schema expects `price` not `avg_price`)
    # - `event_time` and `ingest_time`: integers (milliseconds since epoch)
    df = df.with_columns(
        [
            pl.col("deal_date").dt.strftime("%Y-%m-%d").alias("trade_date"),
            pl.col("avg_price").alias("price"),
            pl.col("event_time").dt.timestamp("ms").cast(pl.Int64),
            pl.col("ingest_time").dt.timestamp("ms").cast(pl.Int64),
        ]
    )

    try:
        # Use write_df_safe with validation
        output_path = write_df_safe(
            df=df,
            dataset=dataset,
            base_path=base_path,
            schema_name="bulk_block_deals_jsonschema",
            schema_dir="schemas/parquet",
            compression="snappy",
            fail_on_validation_errors=True,
            quarantine_dir=base_path / "quarantine",
        )

        # Return path to the parquet file
        out_file = output_path / "data.parquet"

        # Create idempotency marker
        create_idempotency_marker(
            output_file=out_file,
            trade_date=date_key,
            rows=len(df),
            metadata={
                "deal_date": d.isoformat(),
                "deal_type": deal_type.upper(),
                "table": "bulk_block_deals",
            },
        )

        return str(out_file)

    except (FileNotFoundError, OSError) as e:
        logger.error(
            "bulk_block_deals_file_write_failed",
            error=str(e),
            deal_date=str(d),
            deal_type=deal_type,
        )
        raise
    except ValueError as e:
        logger.error(
            "bulk_block_deals_validation_failed",
            error=str(e),
            deal_date=str(d),
            deal_type=deal_type,
        )
        raise
    except Exception as e:
        logger.critical(
            "unexpected_bulk_block_deals_write_error",
            error=str(e),
            deal_date=str(d),
            deal_type=deal_type,
        )
        raise


def load_bulk_block_deals_clickhouse(
    parquet_file: str | Path,
    deal_type: str,
) -> int:
    """Load Parquet file into ClickHouse using the batch loader.

    Reads the Parquet file with hive_partitioning disabled to avoid
    partition-schema conflicts, then streams the Polars DataFrame into
    ClickHouse using the `ClickHouseLoader` which will prefer the native
    client when available (port 9000).
    """
    try:
        # Read parquet without hive partition inference
        df = pl.read_parquet(str(parquet_file), hive_partitioning=False)

        # Instantiate loader (will pick up CLICKHOUSE_* env vars if present)
        loader = ClickHouseLoader()
        try:
            # Use insert_polars_dataframe which connects and retries
            rows = loader.insert_polars_dataframe(table="bulk_block_deals", df=df, dry_run=False)
            return int(rows)
        except Exception as e:
            # If HTTP auth/connection failed, attempt native TCP on port 9000 as a fallback
            err = str(e)
            logger.debug("clickhouse_load_error", error=err)
            try:
                if "9000" not in str(loader.port):
                    logger.info("Attempting ClickHouse native client fallback on port 9000")
                    native_loader = ClickHouseLoader(port=9000)
                    try:
                        rows = native_loader.insert_polars_dataframe(table="bulk_block_deals", df=df, dry_run=False)
                        return int(rows)
                    finally:
                        try:
                            native_loader.disconnect()
                        except Exception:
                            pass
            except Exception:
                pass
            finally:
                try:
                    loader.disconnect()
                except Exception:
                    pass
            # re-raise to be handled by outer exception logging
            raise

    except (FileNotFoundError, OSError) as e:
        logger.error(
            "parquet_read_failed",
            error=str(e),
            path=str(parquet_file),
            deal_type=deal_type,
            retryable=True,
        )
        return 0
    except ValueError as e:
        logger.error(
            "parquet_invalid_format",
            error=str(e),
            path=str(parquet_file),
            deal_type=deal_type,
            retryable=False,
        )
        return 0
    except Exception as e:
        logger.critical(
            "fatal_parquet_read_error",
            error=str(e),
            path=str(parquet_file),
            deal_type=deal_type,
            retryable=False,
        )
        return 0
