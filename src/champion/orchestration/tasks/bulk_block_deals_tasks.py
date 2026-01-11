from __future__ import annotations

from datetime import date
from pathlib import Path
import csv
from typing import Any, Dict, List

import polars as pl

from champion.config import config
from champion.scrapers.nse.bulk_block_deals import BulkBlockDealsScraper


def scrape_bulk_block_deals(
    target_date: str | date,
    deal_type: str = "both",
) -> Dict[str, Path]:
    """Scrape bulk and/or block deals for a date."""
    d = target_date if isinstance(target_date, date) else date.fromisoformat(target_date)
    with BulkBlockDealsScraper() as scraper:
        return scraper.scrape(target_date=d, deal_type=deal_type)


def parse_bulk_block_deals(
    file_path: str | Path,
    deal_date: str | date,
    deal_type: str,
) -> List[Dict[str, Any]]:
    """Parse CSV deals file into standardized event dictionaries."""
    d = deal_date if isinstance(deal_date, date) else date.fromisoformat(deal_date)
    path = Path(file_path)
    if not path.exists():
        return []

    with path.open() as f:
        reader = csv.reader(f)
        columns = next(reader, [])

    dtype_map = {col: pl.Utf8 for col in columns}
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
        except Exception:
            return 0

    def _to_float(val: Any) -> float:
        if val is None or val == "":
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        try:
            return float(str(val).replace(",", "").strip())
        except Exception:
            return 0.0

    events: List[Dict[str, Any]] = []
    for row in df.iter_rows(named=True):
        symbol = str(row.get("Symbol") or row.get("SYMBOL") or "").strip()
        client = str(row.get("ClientName") or row.get("CLIENT_NAME") or "").strip()
        security_name = str(row.get("SecurityName") or row.get("SECURITY_NAME") or "").strip()
        buy_sell_raw = str(row.get("Buy/Sell") or row.get("BuySell") or row.get("BUY_SELL") or "").strip()
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
) -> Dict[str, Any]:
    import uuid
    from datetime import datetime, UTC

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
    events: List[Dict[str, Any]],
    deal_date: str | date,
    deal_type: str,
    output_base_path: str | Path = "data/lake",
) -> str:
    """Write events to partitioned Parquet and return file path."""
    if not events:
        return ""

    d = deal_date if isinstance(deal_date, date) else date.fromisoformat(deal_date)

    base = Path(output_base_path)
    out_dir = (
        base
        / "bulk_block_deals"
        / f"deal_type={deal_type.upper()}"
        / f"year={d.year}"
        / f"month={d.month:02d}"
        / f"day={d.day:02d}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{deal_type.lower()}_deals_{d.strftime('%Y%m%d')}.parquet"

    df = pl.DataFrame(events).with_columns([
        pl.col("deal_date").cast(pl.Date),
        pl.col("event_time").cast(pl.Datetime, strict=False),
        pl.col("ingest_time").cast(pl.Datetime, strict=False),
        pl.col("quantity").cast(pl.Int64),
        pl.col("avg_price").cast(pl.Float64),
        pl.col("year").cast(pl.Int64),
        pl.col("month").cast(pl.Int64),
        pl.col("day").cast(pl.Int64),
    ])

    partition_cols = [c for c in ("year", "month", "day", "deal_type") if c in df.columns]
    to_write = df.drop(partition_cols) if partition_cols else df

    to_write.write_parquet(out_file, compression="snappy")
    return str(out_file)


def load_bulk_block_deals_clickhouse(
    parquet_file: str | Path,
    deal_type: str,
) -> int:
    """Stub loader: return row count from Parquet file."""
    try:
        df = pl.read_parquet(str(parquet_file))
        return len(df)
    except Exception:
        return 0
