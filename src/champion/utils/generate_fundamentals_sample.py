from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl


def _date_range(start: date, end: date, step_days: int = 90):
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=step_days)


def generate_quarterly_financials_sample(
    symbols: Iterable[str], start_date: date, end_date: date, output_dir: Path
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    now = datetime.now()
    for sym in symbols:
        for period_end in _date_range(start_date, end_date, step_days=90):
            records.append(
                {
                    "symbol": sym,
                    "period_end_date": period_end.isoformat(),
                    "revenue": 1000.0,
                    "net_profit": 100.0,
                    "eps": 10.0,
                    "roe": 0.15,
                    "debt_to_equity": 0.5,
                    "current_ratio": 1.2,
                    "net_margin": 0.1,
                    "event_time": now,
                    "ingest_time": now,
                }
            )

    df = pl.DataFrame(records)
    out = (
        output_dir
        / f"quarterly_financials_sample_{start_date.isoformat()}_{end_date.isoformat()}.parquet"
    )
    df.write_parquet(out)
    return out


def generate_shareholding_pattern_sample(
    symbols: Iterable[str], start_date: date, end_date: date, output_dir: Path
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for sym in symbols:
        for quarter_end in _date_range(start_date, end_date, step_days=90):
            records.append(
                {
                    "symbol": sym,
                    "quarter_end_date": quarter_end.isoformat(),
                    "public_holders": 100,
                    "promoter_holding": 60.0,
                    "mutual_funds": 10.0,
                }
            )

    df = pl.DataFrame(records)
    out = (
        output_dir
        / f"shareholding_pattern_sample_{start_date.isoformat()}_{end_date.isoformat()}.parquet"
    )
    df.write_parquet(out)
    return out
