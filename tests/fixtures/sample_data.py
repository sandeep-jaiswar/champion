"""Sample data fixtures for integration tests."""

from datetime import date, timedelta

import polars as pl


def create_sample_ohlc_data(
    symbols: list[str] = None,
    start_date: date = None,
    num_days: int = 50,
    base_prices: dict[str, float] = None,
) -> pl.DataFrame:
    """
    Create sample OHLC data for testing.

    Args:
        symbols: List of stock symbols (default: ["RELIANCE", "TCS", "INFY"])
        start_date: Starting date (default: 50 days ago from today)
        num_days: Number of trading days to generate
        base_prices: Base prices for each symbol (default: generated)

    Returns:
        DataFrame with OHLC data
    """
    if symbols is None:
        symbols = ["RELIANCE", "TCS", "INFY"]

    if start_date is None:
        start_date = date.today() - timedelta(days=num_days)

    if base_prices is None:
        base_prices = {
            "RELIANCE": 2500.0,
            "TCS": 3500.0,
            "INFY": 1500.0,
            "HDFC": 1600.0,
            "ICICIBANK": 950.0,
        }

    records = []
    for symbol in symbols:
        base_price = base_prices.get(symbol, 1000.0)

        for day in range(num_days):
            trade_date = start_date + timedelta(days=day)

            # Create realistic price variations
            price_variation = (day % 10 - 5) * 0.01 * base_price
            close_price = base_price + price_variation

            open_price = close_price * (1 + ((day % 7 - 3) * 0.002))
            high_price = max(open_price, close_price) * 1.005
            low_price = min(open_price, close_price) * 0.995

            records.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "open": round(open_price, 2),
                    "high": round(high_price, 2),
                    "low": round(low_price, 2),
                    "close": round(close_price, 2),
                    "volume": int(1000000 + day * 10000),
                    "turnover": round(close_price * (1000000 + day * 10000), 2),
                }
            )

    return pl.DataFrame(records)


def create_sample_nse_bhavcopy_data(trade_date: date = None) -> pl.DataFrame:
    """
    Create sample NSE bhavcopy data in NSE format.

    Args:
        trade_date: Trading date (default: today)

    Returns:
        DataFrame with NSE bhavcopy format
    """
    if trade_date is None:
        trade_date = date.today()

    # NSE bhavcopy uses different column names
    records = [
        {
            "TckrSymb": "RELIANCE",
            "TradDt": trade_date,
            "OpnPric": 2500.50,
            "HghPric": 2515.75,
            "LwPric": 2495.25,
            "ClsPric": 2510.00,
            "LastPric": 2510.00,
            "PrvsClsgPric": 2505.00,
            "TtlTradgVol": 5000000,
            "TtlTrfVal": 12550000000.0,
            "TtlNbOfTxsExctd": 150000,
            "ISIN": "INE002A01018",
            "FinInstrmId": "RELIANCE_EQ",
            "FinInstrmTp": "EQ",
            "FinInstrmNm": "RELIANCE INDUSTRIES LTD",
            "Sgmt": "EQ",
            "SctySrs": "EQ",
            "Src": "NSE",
        },
        {
            "TckrSymb": "TCS",
            "TradDt": trade_date,
            "OpnPric": 3500.00,
            "HghPric": 3520.50,
            "LwPric": 3495.00,
            "ClsPric": 3515.25,
            "LastPric": 3515.25,
            "PrvsClsgPric": 3510.00,
            "TtlTradgVol": 3000000,
            "TtlTrfVal": 10545750000.0,
            "TtlNbOfTxsExctd": 120000,
            "ISIN": "INE467B01029",
            "FinInstrmId": "TCS_EQ",
            "FinInstrmTp": "EQ",
            "FinInstrmNm": "TATA CONSULTANCY SERVICES LTD",
            "Sgmt": "EQ",
            "SctySrs": "EQ",
            "Src": "NSE",
        },
        {
            "TckrSymb": "INFY",
            "TradDt": trade_date,
            "OpnPric": 1500.00,
            "HghPric": 1508.75,
            "LwPric": 1495.50,
            "ClsPric": 1505.00,
            "LastPric": 1505.00,
            "PrvsClsgPric": 1502.00,
            "TtlTradgVol": 4000000,
            "TtlTrfVal": 6020000000.0,
            "TtlNbOfTxsExctd": 130000,
            "ISIN": "INE009A01021",
            "FinInstrmId": "INFY_EQ",
            "FinInstrmTp": "EQ",
            "FinInstrmNm": "INFOSYS LIMITED",
            "Sgmt": "EQ",
            "SctySrs": "EQ",
            "Src": "NSE",
        },
    ]

    return pl.DataFrame(records)


def create_sample_corporate_actions(
    symbols: list[str] = None,
    start_date: date = None,
) -> pl.DataFrame:
    """
    Create sample corporate actions data.

    Args:
        symbols: List of stock symbols (default: ["RELIANCE", "TCS"])
        start_date: Starting date for events (default: 60 days ago)

    Returns:
        DataFrame with corporate action events
    """
    if symbols is None:
        symbols = ["RELIANCE", "TCS"]

    if start_date is None:
        start_date = date.today() - timedelta(days=60)

    records = []

    # Stock split for RELIANCE: 1:2 split (1 old share becomes 2 new shares)
    records.append(
        {
            "symbol": "RELIANCE",
            "ex_date": start_date + timedelta(days=30),
            "action_type": "SPLIT",
            "adjustment_factor": 2.0,  # Divide old prices by 2
            "split_ratio": "1:2",
            "description": "Stock Split - 1 share split into 2 shares",
        }
    )

    # Bonus issue for TCS: 1:1 bonus (1 bonus share for every 1 existing share)
    records.append(
        {
            "symbol": "TCS",
            "ex_date": start_date + timedelta(days=20),
            "action_type": "BONUS",
            "adjustment_factor": 2.0,  # (1 + 1) / 1 = 2
            "bonus_ratio": "1:1",
            "description": "Bonus Issue - 1:1",
        }
    )

    # Dividend for RELIANCE
    records.append(
        {
            "symbol": "RELIANCE",
            "ex_date": start_date + timedelta(days=10),
            "action_type": "DIVIDEND",
            "adjustment_factor": 0.992,  # Approximately (price - dividend) / price
            "dividend_amount": 20.0,
            "description": "Dividend - Rs 20 per share",
        }
    )

    # Dividend for TCS
    records.append(
        {
            "symbol": "TCS",
            "ex_date": start_date + timedelta(days=40),
            "action_type": "DIVIDEND",
            "adjustment_factor": 0.995,
            "dividend_amount": 18.0,
            "description": "Dividend - Rs 18 per share",
        }
    )

    return pl.DataFrame(records)


def create_sample_features_data(
    symbols: list[str] = None,
    start_date: date = None,
    num_days: int = 30,
) -> pl.DataFrame:
    """
    Create sample features data with technical indicators.

    Args:
        symbols: List of stock symbols (default: ["RELIANCE", "TCS"])
        start_date: Starting date (default: 30 days ago)
        num_days: Number of trading days

    Returns:
        DataFrame with computed features
    """
    if symbols is None:
        symbols = ["RELIANCE", "TCS"]

    if start_date is None:
        start_date = date.today() - timedelta(days=num_days)

    # First, create base OHLC data
    ohlc_df = create_sample_ohlc_data(symbols=symbols, start_date=start_date, num_days=num_days)

    # Add simple computed features (not using real indicator logic)
    records = []
    for row in ohlc_df.iter_rows(named=True):
        records.append(
            {
                "symbol": row["symbol"],
                "trade_date": row["trade_date"],
                "close": row["close"],
                "sma_5": row["close"] * 0.98,  # Simplified
                "sma_20": row["close"] * 0.95,  # Simplified
                "ema_12": row["close"] * 0.99,  # Simplified
                "ema_26": row["close"] * 0.97,  # Simplified
                "rsi_14": 50.0 + (row["trade_date"].day % 20),  # Simplified
                "feature_version": "v1",
                "feature_timestamp": int(row["trade_date"].strftime("%s")) * 1000
                if hasattr(row["trade_date"], "strftime")
                else 0,
            }
        )

    return pl.DataFrame(records)
