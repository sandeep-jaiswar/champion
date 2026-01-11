"""Corporate actions processor for computing adjustment factors.

This module computes adjustment factors from corporate action events
(splits, bonuses, dividends) that are applied to historical OHLC prices.
"""

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Dict, List, Optional

import polars as pl


class CorporateActionType(str, Enum):
    """Corporate action types."""

    SPLIT = "SPLIT"
    BONUS = "BONUS"
    DIVIDEND = "DIVIDEND"
    RIGHTS = "RIGHTS"
    INTEREST_PAYMENT = "INTEREST_PAYMENT"
    EGMMEETING = "EGMMEETING"
    DEMERGER = "DEMERGER"
    MERGER = "MERGER"
    BUYBACK = "BUYBACK"
    OTHER = "OTHER"


@dataclass
class CorporateAction:
    """Corporate action event."""

    symbol: str
    ex_date: date
    action_type: CorporateActionType
    adjustment_factor: float
    split_ratio: Optional[tuple[int, int]] = None  # (old_shares, new_shares)
    bonus_ratio: Optional[tuple[int, int]] = None  # (new_shares, existing_shares)
    dividend_amount: Optional[float] = None
    face_value: Optional[float] = None


class CorporateActionsProcessor:
    """Processor for corporate actions data."""

    def __init__(self):
        """Initialize processor."""
        pass

    def compute_split_adjustment(
        self, old_shares: int, new_shares: int
    ) -> float:
        """Compute adjustment factor for stock split.

        For a 1:5 split (1 old share becomes 5 new shares):
        - Prices should be divided by 5
        - Adjustment factor = new_shares / old_shares = 5 / 1 = 5.0

        Args:
            old_shares: Number of old shares
            new_shares: Number of new shares

        Returns:
            Adjustment factor to divide prices by
        """
        if old_shares <= 0 or new_shares <= 0:
            raise ValueError(
                f"Invalid split ratio: {old_shares}:{new_shares}"
            )
        return new_shares / old_shares

    def compute_bonus_adjustment(
        self, new_shares: int, existing_shares: int
    ) -> float:
        """Compute adjustment factor for bonus issue.

        For a 1:2 bonus (1 bonus share for every 2 existing shares):
        - Total shares = existing + new = 2 + 1 = 3
        - Prices should be multiplied by 2/3
        - Adjustment factor = (existing + new) / existing = 3 / 2 = 1.5

        Args:
            new_shares: Number of bonus shares
            existing_shares: Number of existing shares

        Returns:
            Adjustment factor to divide prices by
        """
        if new_shares <= 0 or existing_shares <= 0:
            raise ValueError(
                f"Invalid bonus ratio: {new_shares}:{existing_shares}"
            )
        return (existing_shares + new_shares) / existing_shares

    def compute_dividend_adjustment(
        self, dividend_amount: float, close_price: float
    ) -> float:
        """Compute adjustment factor for dividend.

        For a dividend payment:
        - Previous prices should be reduced by dividend amount
        - Adjustment factor = (close_price - dividend) / close_price

        Args:
            dividend_amount: Dividend per share
            close_price: Closing price on ex-date

        Returns:
            Adjustment factor for prices before ex-date
        """
        if dividend_amount < 0:
            raise ValueError(f"Invalid dividend amount: {dividend_amount}")
        if close_price <= 0:
            raise ValueError(f"Invalid close price: {close_price}")

        # Dividend adjustment is multiplicative
        # Prices before ex-date = prices * (1 - dividend/price)
        return 1.0 - (dividend_amount / close_price)

    def parse_ca_event(self, ca_data: Dict) -> CorporateAction:
        """Parse corporate action event from dictionary.

        Args:
            ca_data: Corporate action data dictionary

        Returns:
            CorporateAction object
        """
        action_type = CorporateActionType(ca_data["action_type"])
        adjustment_factor = ca_data.get("adjustment_factor", 1.0)

        # If adjustment_factor not provided, compute it
        if adjustment_factor == 1.0:
            if action_type == CorporateActionType.SPLIT:
                split_ratio = ca_data.get("split_ratio")
                if split_ratio:
                    adjustment_factor = self.compute_split_adjustment(
                        split_ratio["old_shares"], split_ratio["new_shares"]
                    )
            elif action_type == CorporateActionType.BONUS:
                bonus_ratio = ca_data.get("bonus_ratio")
                if bonus_ratio:
                    adjustment_factor = self.compute_bonus_adjustment(
                        bonus_ratio["new_shares"],
                        bonus_ratio["existing_shares"],
                    )

        return CorporateAction(
            symbol=ca_data["symbol"],
            ex_date=ca_data["ex_date"],
            action_type=action_type,
            adjustment_factor=adjustment_factor,
            split_ratio=ca_data.get("split_ratio"),
            bonus_ratio=ca_data.get("bonus_ratio"),
            dividend_amount=ca_data.get("dividend_amount"),
            face_value=ca_data.get("face_value"),
        )


def compute_adjustment_factors(
    ca_df: pl.DataFrame,
) -> pl.DataFrame:
    """Compute cumulative adjustment factors from corporate actions.

    This function processes corporate actions in chronological order
    and computes cumulative adjustment factors for each symbol.

    Args:
        ca_df: DataFrame with corporate action events
            Required columns: symbol, ex_date, action_type, adjustment_factor

    Returns:
        DataFrame with cumulative adjustment factors per symbol/date
            Columns: symbol, ex_date, adjustment_factor, cumulative_factor
    """
    if ca_df.is_empty():
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "ex_date": pl.Date,
                "action_type": pl.Utf8,
                "adjustment_factor": pl.Float64,
                "cumulative_factor": pl.Float64,
            }
        )

    # Sort by symbol and ex_date (descending to apply from most recent)
    ca_sorted = ca_df.sort(["symbol", "ex_date"], descending=[False, True])

    # Compute cumulative product of adjustment factors per symbol
    # This represents the total adjustment from most recent to oldest
    result = ca_sorted.with_columns(
        pl.col("adjustment_factor")
        .cum_prod()
        .over("symbol")
        .alias("cumulative_factor")
    )

    return result


def load_corporate_actions(file_path: str) -> pl.DataFrame:
    """Load corporate actions from Parquet file.

    Args:
        file_path: Path to Parquet file(s) with CA data

    Returns:
        DataFrame with corporate action events
    """
    df = pl.read_parquet(file_path)

    # Extract relevant fields from nested structure if needed
    if "payload" in df.columns:
        # Avro format with nested payload
        df = df.select(
            [
                pl.col("payload").struct.field("symbol").alias("symbol"),
                pl.col("payload").struct.field("ex_date").alias("ex_date"),
                pl.col("payload")
                .struct.field("action_type")
                .alias("action_type"),
                pl.col("payload")
                .struct.field("adjustment_factor")
                .alias("adjustment_factor"),
                pl.col("payload")
                .struct.field("split_ratio")
                .alias("split_ratio"),
                pl.col("payload")
                .struct.field("bonus_ratio")
                .alias("bonus_ratio"),
                pl.col("payload")
                .struct.field("dividend_amount")
                .alias("dividend_amount"),
            ]
        )

    return df
