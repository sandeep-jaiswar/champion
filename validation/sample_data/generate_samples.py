"""Generate sample data for validation testing."""

import uuid
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl


def generate_sample_raw_ohlc(num_rows: int = 100, include_invalid: bool = False) -> pl.DataFrame:
    """Generate sample raw OHLC data.

    Args:
        num_rows: Number of rows to generate
        include_invalid: If True, include some invalid records

    Returns:
        DataFrame with raw OHLC data
    """
    symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ITC"]
    base_date = datetime(2024, 1, 1)

    data = {
        "event_id": [str(uuid.uuid4()) for _ in range(num_rows)],
        "event_time": [
            int((base_date + timedelta(days=i % 30)).timestamp() * 1000) for i in range(num_rows)
        ],
        "ingest_time": [
            int((base_date + timedelta(days=i % 30, minutes=5)).timestamp() * 1000)
            for i in range(num_rows)
        ],
        "source": ["nse_bhavcopy"] * num_rows,
        "schema_version": ["v1"] * num_rows,
        "entity_id": [f"{symbols[i % len(symbols)]}:NSE" for i in range(num_rows)],
        "TradDt": [(base_date + timedelta(days=i % 30)).strftime("%Y-%m-%d") for i in range(num_rows)],
        "BizDt": [(base_date + timedelta(days=i % 30)).strftime("%Y-%m-%d") for i in range(num_rows)],
        "Sgmt": ["CM"] * num_rows,
        "Src": ["NSE"] * num_rows,
        "FinInstrmTp": ["STK"] * num_rows,
        "FinInstrmId": [2885 + i for i in range(num_rows)],
        "ISIN": [f"INE{str(i+100).zfill(6)}A0101{i%10}" for i in range(num_rows)],
        "TckrSymb": [symbols[i % len(symbols)] for i in range(num_rows)],
        "SctySrs": ["EQ"] * num_rows,
        "XpryDt": [None] * num_rows,
        "FininstrmActlXpryDt": [None] * num_rows,
        "StrkPric": [None] * num_rows,
        "OptnTp": [None] * num_rows,
        "FinInstrmNm": [f"{symbols[i % len(symbols)]} Ltd" for i in range(num_rows)],
        "OpnPric": [100.0 + i * 10 for i in range(num_rows)],
        "HghPric": [110.0 + i * 10 for i in range(num_rows)],
        "LwPric": [95.0 + i * 10 for i in range(num_rows)],
        "ClsPric": [105.0 + i * 10 for i in range(num_rows)],
        "LastPric": [104.5 + i * 10 for i in range(num_rows)],
        "PrvsClsgPric": [100.0 + i * 10 for i in range(num_rows)],
        "UndrlygPric": [None] * num_rows,
        "SttlmPric": [105.0 + i * 10 for i in range(num_rows)],
        "OpnIntrst": [None] * num_rows,
        "ChngInOpnIntrst": [None] * num_rows,
        "TtlTradgVol": [10000 + i * 1000 for i in range(num_rows)],
        "TtlTrfVal": [1000000.0 + i * 100000 for i in range(num_rows)],
        "TtlNbOfTxsExctd": [1000 + i * 100 for i in range(num_rows)],
        "SsnId": ["F1"] * num_rows,
        "NewBrdLotQty": [1] * num_rows,
        "Rmks": [None] * num_rows,
        "Rsvd1": [None] * num_rows,
        "Rsvd2": [None] * num_rows,
        "Rsvd3": [None] * num_rows,
        "Rsvd4": [None] * num_rows,
    }

    df = pl.DataFrame(data)

    if include_invalid:
        # Add some invalid records
        # Row with negative price
        df = df.with_columns(
            pl.when(pl.col("event_id") == df["event_id"][10])
            .then(-50.0)
            .otherwise(pl.col("OpnPric"))
            .alias("OpnPric")
        )

        # Row with negative volume
        df = df.with_columns(
            pl.when(pl.col("event_id") == df["event_id"][20])
            .then(-1000)
            .otherwise(pl.col("TtlTradgVol"))
            .alias("TtlTradgVol")
        )

    return df


def generate_sample_normalized_ohlc(num_rows: int = 100, include_invalid: bool = False) -> pl.DataFrame:
    """Generate sample normalized OHLC data.

    Args:
        num_rows: Number of rows to generate
        include_invalid: If True, include some invalid records

    Returns:
        DataFrame with normalized OHLC data
    """
    symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ITC"]
    base_date = datetime(2024, 1, 1)
    epoch = datetime(1970, 1, 1)

    data = {
        "event_id": [str(uuid.uuid4()) for _ in range(num_rows)],
        "event_time": [
            int((base_date + timedelta(days=i % 30)).timestamp() * 1000) for i in range(num_rows)
        ],
        "ingest_time": [
            int((base_date + timedelta(days=i % 30, minutes=5)).timestamp() * 1000)
            for i in range(num_rows)
        ],
        "source": ["nse_bhavcopy"] * num_rows,
        "schema_version": ["v1"] * num_rows,
        "entity_id": [f"{symbols[i % len(symbols)]}:NSE" for i in range(num_rows)],
        "instrument_id": [f"{symbols[i % len(symbols)]}:NSE" for i in range(num_rows)],
        "symbol": [symbols[i % len(symbols)] for i in range(num_rows)],
        "exchange": ["NSE"] * num_rows,
        "isin": [f"INE{str(i+100).zfill(6)}A0101{i%10}" for i in range(num_rows)],
        "instrument_type": ["STK"] * num_rows,
        "series": ["EQ"] * num_rows,
        "trade_date": [
            ((base_date + timedelta(days=i % 30)) - epoch).days for i in range(num_rows)
        ],
        "prev_close": [100.0 + i * 10 for i in range(num_rows)],
        "open": [100.0 + i * 10 for i in range(num_rows)],
        "high": [110.0 + i * 10 for i in range(num_rows)],
        "low": [95.0 + i * 10 for i in range(num_rows)],
        "close": [105.0 + i * 10 for i in range(num_rows)],
        "last_price": [104.5 + i * 10 for i in range(num_rows)],
        "settlement_price": [105.0 + i * 10 for i in range(num_rows)],
        "volume": [10000 + i * 1000 for i in range(num_rows)],
        "turnover": [1000000.0 + i * 100000 for i in range(num_rows)],
        "trades": [1000 + i * 100 for i in range(num_rows)],
        "adjustment_factor": [1.0] * num_rows,
        "adjustment_date": [None] * num_rows,
        "is_trading_day": [True] * num_rows,
    }

    df = pl.DataFrame(data)

    if include_invalid:
        # Add some invalid records
        # Row with high < low (OHLC violation)
        df = df.with_columns([
            pl.when(pl.col("event_id") == df["event_id"][10])
            .then(80.0)
            .otherwise(pl.col("high"))
            .alias("high"),
            pl.when(pl.col("event_id") == df["event_id"][10])
            .then(100.0)
            .otherwise(pl.col("low"))
            .alias("low"),
        ])

        # Row with negative volume
        df = df.with_columns(
            pl.when(pl.col("event_id") == df["event_id"][20])
            .then(-1000)
            .otherwise(pl.col("volume"))
            .alias("volume")
        )

        # Row with zero adjustment factor (invalid)
        df = df.with_columns(
            pl.when(pl.col("event_id") == df["event_id"][30])
            .then(0.0)
            .otherwise(pl.col("adjustment_factor"))
            .alias("adjustment_factor")
        )

    return df


def main():
    """Generate and save sample data files."""
    output_dir = Path(__file__).parent
    output_dir.mkdir(exist_ok=True)

    # Generate valid raw OHLC data
    print("Generating valid raw OHLC sample data...")
    raw_valid = generate_sample_raw_ohlc(num_rows=100, include_invalid=False)
    raw_valid.write_parquet(output_dir / "raw_equity_ohlc_valid.parquet")
    print(f"✓ Created: {output_dir / 'raw_equity_ohlc_valid.parquet'} ({len(raw_valid)} rows)")

    # Generate invalid raw OHLC data
    print("Generating raw OHLC sample data with invalid records...")
    raw_invalid = generate_sample_raw_ohlc(num_rows=100, include_invalid=True)
    raw_invalid.write_parquet(output_dir / "raw_equity_ohlc_with_errors.parquet")
    print(f"✓ Created: {output_dir / 'raw_equity_ohlc_with_errors.parquet'} ({len(raw_invalid)} rows)")

    # Generate valid normalized OHLC data
    print("Generating valid normalized OHLC sample data...")
    norm_valid = generate_sample_normalized_ohlc(num_rows=100, include_invalid=False)
    norm_valid.write_parquet(output_dir / "normalized_equity_ohlc_valid.parquet")
    print(f"✓ Created: {output_dir / 'normalized_equity_ohlc_valid.parquet'} ({len(norm_valid)} rows)")

    # Generate invalid normalized OHLC data
    print("Generating normalized OHLC sample data with invalid records...")
    norm_invalid = generate_sample_normalized_ohlc(num_rows=100, include_invalid=True)
    norm_invalid.write_parquet(output_dir / "normalized_equity_ohlc_with_errors.parquet")
    print(f"✓ Created: {output_dir / 'normalized_equity_ohlc_with_errors.parquet'} ({len(norm_invalid)} rows)")

    print("\n✅ Sample data generation complete!")


if __name__ == "__main__":
    main()
