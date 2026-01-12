"""
Sample Data Generator for ClickHouse Testing

Generates sample Parquet files for testing the ClickHouse loader.
Creates realistic market data samples for raw, normalized, and features layers.
"""

import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

# Constants for data generation
MAX_INSTRUMENT_ID = 100000  # Maximum FinInstrmId value for deterministic hash-based IDs


def generate_raw_equity_ohlc_sample(
    num_symbols: int = 10,
    num_days: int = 5,
    output_dir: str = "data/lake/raw/equity_ohlc",
) -> Path:
    """
    Generate sample raw equity OHLC data.

    Args:
        num_symbols: Number of unique symbols
        num_days: Number of trading days
        output_dir: Output directory for Parquet files

    Returns:
        Path to generated data
    """
    symbols = [f"SYMBOL{i:03d}" for i in range(num_symbols)]
    base_date = date(2024, 1, 15)

    records = []
    for day in range(num_days):
        trade_date = base_date + timedelta(days=day)

        for symbol in symbols:
            # Generate realistic OHLC data
            base_price = 100.0 + (hash(symbol) % 900)
            open_price = base_price + (hash(f"{symbol}{day}open") % 100 - 50) / 10
            high_price = open_price + (hash(f"{symbol}{day}high") % 50) / 10
            low_price = open_price - (hash(f"{symbol}{day}low") % 50) / 10
            close_price = (open_price + high_price + low_price) / 3
            volume = 100000 + (hash(f"{symbol}{day}vol") % 1000000)

            record = {
                # Envelope
                "event_id": str(uuid.uuid4()),
                "event_time": int(
                    datetime.combine(trade_date, datetime.min.time()).timestamp() * 1000
                ),
                "ingest_time": int(datetime.now().timestamp() * 1000),
                "source": "nse_bhavcopy",
                "schema_version": "v1",
                "entity_id": f"{symbol}:NSE",
                # Payload
                "TradDt": trade_date.isoformat(),
                "BizDt": trade_date.isoformat(),
                "Sgmt": "CM",
                "Src": "NSE",
                "FinInstrmTp": "STK",
                "FinInstrmId": hash(symbol) % MAX_INSTRUMENT_ID,
                "ISIN": f"INE{hash(symbol) % 900000:06d}01",
                "TckrSymb": symbol,
                "SctySrs": "EQ",
                "XpryDt": None,
                "FininstrmActlXpryDt": None,
                "StrkPric": None,
                "OptnTp": None,
                "FinInstrmNm": f"{symbol} Ltd",
                "OpnPric": open_price,
                "HghPric": high_price,
                "LwPric": low_price,
                "ClsPric": close_price,
                "LastPric": close_price,
                "PrvsClsgPric": open_price - 1.0,
                "UndrlygPric": None,
                "SttlmPric": close_price,
                "OpnIntrst": None,
                "ChngInOpnIntrst": None,
                "TtlTradgVol": volume,
                "TtlTrfVal": volume * close_price,
                "TtlNbOfTxsExctd": volume // 100,
                "SsnId": "F1",
                "NewBrdLotQty": 1,
                "Rmks": None,
                "Rsvd01": None,
                "Rsvd02": None,
                "Rsvd03": None,
                "Rsvd04": None,
            }
            records.append(record)

    # Create DataFrame
    df = pl.DataFrame(records)

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Write partitioned by date
    for day in range(num_days):
        trade_date = base_date + timedelta(days=day)
        date_str = trade_date.isoformat()

        day_df = df.filter(pl.col("TradDt") == date_str)

        partition_dir = output_path / f"date={date_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "part-00000.parquet"
        day_df.write_parquet(output_file, compression="snappy")
        print(f"Generated: {output_file} ({len(day_df)} rows)")

    return output_path


def generate_normalized_equity_ohlc_sample(
    num_symbols: int = 10,
    num_days: int = 5,
    output_dir: str = "data/lake/normalized/equity_ohlc",
) -> Path:
    """
    Generate sample normalized equity OHLC data.

    Args:
        num_symbols: Number of unique symbols
        num_days: Number of trading days
        output_dir: Output directory for Parquet files

    Returns:
        Path to generated data
    """
    symbols = [f"SYMBOL{i:03d}" for i in range(num_symbols)]
    base_date = date(2024, 1, 15)

    records = []
    for day in range(num_days):
        trade_date = base_date + timedelta(days=day)

        for symbol in symbols:
            # Generate realistic OHLC data
            base_price = 100.0 + (hash(symbol) % 900)
            open_price = base_price + (hash(f"{symbol}{day}open") % 100 - 50) / 10
            high_price = open_price + (hash(f"{symbol}{day}high") % 50) / 10
            low_price = open_price - (hash(f"{symbol}{day}low") % 50) / 10
            close_price = (open_price + high_price + low_price) / 3
            volume = 100000 + (hash(f"{symbol}{day}vol") % 1000000)

            # Generate deterministic FinInstrmId for the symbol
            fin_instrm_id = hash(symbol) % MAX_INSTRUMENT_ID

            record = {
                # Envelope
                "event_id": str(uuid.uuid4()),
                "event_time": int(
                    datetime.combine(trade_date, datetime.min.time()).timestamp() * 1000
                ),
                "ingest_time": int(datetime.now().timestamp() * 1000),
                "source": "nse_bhavcopy",
                "schema_version": "v1",
                "entity_id": f"{symbol}:{fin_instrm_id}:NSE",
                # Normalized payload - using NSE column names matching ClickHouse schema
                "TradDt": trade_date.isoformat(),
                "BizDt": trade_date.isoformat(),
                "Sgmt": "CM",
                "Src": "NSE",
                "FinInstrmTp": "STK",
                "FinInstrmId": fin_instrm_id,
                "ISIN": f"INE{hash(symbol) % 900000:06d}01",
                "TckrSymb": symbol,
                "SctySrs": "EQ",
                "XpryDt": None,
                "FininstrmActlXpryDt": None,
                "StrkPric": None,
                "OptnTp": None,
                "FinInstrmNm": f"{symbol} Ltd",
                "OpnPric": open_price,
                "HghPric": high_price,
                "LwPric": low_price,
                "ClsPric": close_price,
                "LastPric": close_price,
                "PrvsClsgPric": open_price - 1.0,
                "UndrlygPric": None,
                "SttlmPric": close_price,
                "OpnIntrst": None,
                "ChngInOpnIntrst": None,
                "TtlTradgVol": volume,
                "TtlTrfVal": volume * close_price,
                "TtlNbOfTxsExctd": volume // 100,
                "SsnId": "F1",
                "NewBrdLotQty": 1,
                "Rmks": None,
                "Rsvd01": None,
                "Rsvd02": None,
                "Rsvd03": None,
                "Rsvd04": None,
            }
            records.append(record)

    # Create DataFrame
    df = pl.DataFrame(records)

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Write partitioned by year/month/day
    for day in range(num_days):
        trade_date = base_date + timedelta(days=day)
        date_str = trade_date.isoformat()

        day_df = df.filter(pl.col("TradDt") == date_str)

        partition_dir = (
            output_path
            / f"year={trade_date.year}"
            / f"month={trade_date.month:02d}"
            / f"day={trade_date.day:02d}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "part-00000.parquet"
        day_df.write_parquet(output_file, compression="snappy")
        print(f"Generated: {output_file} ({len(day_df)} rows)")

    return output_path


def generate_features_equity_indicators_sample(
    num_symbols: int = 10,
    num_days: int = 5,
    output_dir: str = "data/lake/features/equity_indicators",
) -> Path:
    """
    Generate sample feature indicators data.

    Args:
        num_symbols: Number of unique symbols
        num_days: Number of trading days
        output_dir: Output directory for Parquet files

    Returns:
        Path to generated data
    """
    symbols = [f"SYMBOL{i:03d}" for i in range(num_symbols)]
    base_date = date(2024, 1, 15)

    records = []
    for day in range(num_days):
        trade_date = base_date + timedelta(days=day)

        for symbol in symbols:
            # Generate realistic indicator values
            base_price = 100.0 + (hash(symbol) % 900)

            record = {
                # Metadata
                "symbol": symbol,
                "trade_date": trade_date,
                "feature_timestamp": int(
                    datetime.combine(trade_date, datetime.min.time()).timestamp() * 1000
                ),
                "feature_version": "v1",
                # Moving averages
                "sma_5": base_price + (hash(f"{symbol}{day}sma5") % 20 - 10),
                "sma_10": base_price + (hash(f"{symbol}{day}sma10") % 15 - 7),
                "sma_20": base_price + (hash(f"{symbol}{day}sma20") % 10 - 5),
                "sma_50": base_price,
                "sma_100": base_price - (hash(f"{symbol}{day}sma100") % 10),
                "sma_200": base_price - (hash(f"{symbol}{day}sma200") % 20),
                "ema_12": base_price + (hash(f"{symbol}{day}ema12") % 15 - 7),
                "ema_26": base_price,
                "ema_50": base_price - (hash(f"{symbol}{day}ema50") % 10),
                # Momentum indicators
                "rsi_14": 30.0 + (hash(f"{symbol}{day}rsi") % 40),
                "macd": (hash(f"{symbol}{day}macd") % 100 - 50) / 10,
                "macd_signal": (hash(f"{symbol}{day}macdsig") % 100 - 50) / 10,
                "macd_histogram": (hash(f"{symbol}{day}macdhist") % 50 - 25) / 10,
                "stochastic_k": 20.0 + (hash(f"{symbol}{day}stochk") % 60),
                "stochastic_d": 20.0 + (hash(f"{symbol}{day}stochd") % 60),
                # Volatility indicators
                "bb_upper": base_price + (hash(f"{symbol}{day}bbupper") % 20),
                "bb_middle": base_price,
                "bb_lower": base_price - (hash(f"{symbol}{day}bblower") % 20),
                "bb_width": (hash(f"{symbol}{day}bbwidth") % 30) + 10,
                "atr_14": (hash(f"{symbol}{day}atr") % 50) + 10,
                # Volume indicators
                "vwap": base_price + (hash(f"{symbol}{day}vwap") % 10 - 5),
                "obv": (hash(f"{symbol}{day}obv") % 10000000),
                # Computed timestamp
                "computed_at": datetime.now(),
            }
            records.append(record)

    # Create DataFrame
    df = pl.DataFrame(records)

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Write partitioned by year/month/day
    for day in range(num_days):
        trade_date = base_date + timedelta(days=day)

        day_df = df.filter(pl.col("trade_date") == trade_date)

        partition_dir = (
            output_path
            / f"year={trade_date.year}"
            / f"month={trade_date.month:02d}"
            / f"day={trade_date.day:02d}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "part-00000.parquet"
        day_df.write_parquet(output_file, compression="snappy")
        print(f"Generated: {output_file} ({len(day_df)} rows)")

    return output_path


def main():
    """Generate all sample datasets."""
    print("Generating sample data for ClickHouse testing...\n")

    print("=== Raw Equity OHLC ===")
    raw_path = generate_raw_equity_ohlc_sample()
    print(f"Generated raw data at: {raw_path}\n")

    print("=== Normalized Equity OHLC ===")
    normalized_path = generate_normalized_equity_ohlc_sample()
    print(f"Generated normalized data at: {normalized_path}\n")

    print("=== Features Equity Indicators ===")
    features_path = generate_features_equity_indicators_sample()
    print(f"Generated features data at: {features_path}\n")

    print("✓ Sample data generation complete!")


if __name__ == "__main__":
    main()
