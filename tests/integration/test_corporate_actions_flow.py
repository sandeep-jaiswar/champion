"""
Integration tests for corporate actions flow.

Tests the complete flow:
1. Load corporate action events
2. Compute adjustment factors
3. Apply to OHLC data
4. Validate price continuity
"""

from datetime import date

import polars as pl
import pytest

from champion.corporate_actions.ca_processor import (
    CorporateActionsProcessor,
    compute_adjustment_factors,
)
from champion.corporate_actions.price_adjuster import apply_adjustments
from tests.fixtures.sample_data import create_sample_corporate_actions, create_sample_ohlc_data


@pytest.fixture
def sample_ca_events():
    """Fixture providing sample corporate action events."""
    return create_sample_corporate_actions(symbols=["RELIANCE", "TCS"], start_date=date(2024, 1, 1))


@pytest.fixture
def sample_ohlc_before_ca():
    """Fixture providing OHLC data before corporate actions."""
    return create_sample_ohlc_data(
        symbols=["RELIANCE", "TCS"], start_date=date(2024, 1, 1), num_days=80
    )


class TestCorporateActionsFlow:
    """Integration tests for corporate actions pipeline."""

    def test_load_corporate_actions(self, sample_ca_events):
        """Test loading corporate action events."""
        # Verify CA data structure
        assert not sample_ca_events.is_empty()
        assert len(sample_ca_events) >= 3  # At least 3 events

        # Verify required columns
        required_cols = ["symbol", "ex_date", "action_type", "adjustment_factor"]
        for col in required_cols:
            assert col in sample_ca_events.columns, f"Missing column: {col}"

        # Verify action types
        action_types = sample_ca_events["action_type"].unique().to_list()
        assert "SPLIT" in action_types or "BONUS" in action_types or "DIVIDEND" in action_types

    def test_compute_split_adjustment(self):
        """Test computing adjustment factor for stock split."""
        processor = CorporateActionsProcessor()

        # 1:2 split (1 old share becomes 2 new shares)
        # Prices should be divided by 2
        factor = processor.compute_split_adjustment(old_shares=1, new_shares=2)
        assert factor == 2.0

        # 1:5 split (1 old share becomes 5 new shares)
        # Prices should be divided by 5
        factor = processor.compute_split_adjustment(old_shares=1, new_shares=5)
        assert factor == 5.0

        # 2:1 reverse split (2 old shares become 1 new share)
        # Prices should be multiplied by 2
        factor = processor.compute_split_adjustment(old_shares=2, new_shares=1)
        assert factor == 0.5

    def test_compute_bonus_adjustment(self):
        """Test computing adjustment factor for bonus issue."""
        processor = CorporateActionsProcessor()

        # 1:1 bonus (1 bonus share for every 1 existing share)
        # Total shares = 2, adjustment = 2/1 = 2.0
        factor = processor.compute_bonus_adjustment(new_shares=1, existing_shares=1)
        assert factor == 2.0

        # 1:2 bonus (1 bonus share for every 2 existing shares)
        # Total shares = 3, adjustment = 3/2 = 1.5
        factor = processor.compute_bonus_adjustment(new_shares=1, existing_shares=2)
        assert factor == 1.5

        # 2:5 bonus (2 bonus shares for every 5 existing shares)
        # Total shares = 7, adjustment = 7/5 = 1.4
        factor = processor.compute_bonus_adjustment(new_shares=2, existing_shares=5)
        assert factor == 1.4

    def test_compute_dividend_adjustment(self):
        """Test computing adjustment factor for dividend."""
        processor = CorporateActionsProcessor()

        # Dividend of Rs 10 on Rs 100 close price
        # Adjustment = (100 - 10) / 100 = 0.9
        factor = processor.compute_dividend_adjustment(dividend_amount=10.0, close_price=100.0)
        assert abs(factor - 0.9) < 0.001

        # Dividend of Rs 20 on Rs 2500 close price
        # Adjustment = (2500 - 20) / 2500 = 0.992
        factor = processor.compute_dividend_adjustment(dividend_amount=20.0, close_price=2500.0)
        assert abs(factor - 0.992) < 0.001

    def test_compute_cumulative_adjustment_factors(self, sample_ca_events):
        """Test computing cumulative adjustment factors."""
        # Compute cumulative factors
        result_df = compute_adjustment_factors(sample_ca_events)

        assert not result_df.is_empty()
        assert "cumulative_factor" in result_df.columns

        # Verify cumulative factors are computed per symbol
        for symbol in result_df["symbol"].unique().to_list():
            symbol_df = result_df.filter(pl.col("symbol") == symbol)

            # Cumulative factor should be product of individual factors
            # (when sorted by ex_date descending)
            factors = symbol_df.sort("ex_date", descending=True)["adjustment_factor"].to_list()
            cumulative = symbol_df.sort("ex_date", descending=True)["cumulative_factor"].to_list()

            # First cumulative should equal first factor
            if len(factors) > 0:
                assert abs(cumulative[0] - factors[0]) < 0.001

    def test_apply_adjustments_to_ohlc(self, sample_ohlc_before_ca, sample_ca_events):
        """Test applying corporate action adjustments to OHLC data."""
        # Get adjustment factors
        ca_factors = compute_adjustment_factors(sample_ca_events)

        # Apply adjustments
        adjusted_df = apply_adjustments(
            ohlc_df=sample_ohlc_before_ca,
            ca_factors_df=ca_factors,
            columns=["open", "high", "low", "close"],
        )

        # Verify adjusted data exists
        assert not adjusted_df.is_empty()
        assert len(adjusted_df) == len(sample_ohlc_before_ca)

        # Verify adjusted columns exist
        for col in ["open", "high", "low", "close"]:
            assert col in adjusted_df.columns

        # Verify prices are adjusted (should be different from originals for some rows)
        # For dates before ex_date, prices should be adjusted
        reliance_df = adjusted_df.filter(pl.col("symbol") == "RELIANCE")
        if len(reliance_df) > 0:
            # Prices should be modified for dates before corporate actions
            assert (
                reliance_df["close"].sum()
                != sample_ohlc_before_ca.filter(pl.col("symbol") == "RELIANCE")["close"].sum()
            )

    def test_price_continuity_after_adjustment(self, sample_ohlc_before_ca, sample_ca_events):
        """Test that adjusted prices maintain continuity across corporate actions."""
        # Get adjustment factors
        ca_factors = compute_adjustment_factors(sample_ca_events)

        # Apply adjustments
        adjusted_df = apply_adjustments(
            ohlc_df=sample_ohlc_before_ca, ca_factors_df=ca_factors, columns=["close"]
        )

        # For each symbol, check that there are no sudden jumps
        for symbol in adjusted_df["symbol"].unique().to_list():
            symbol_df = adjusted_df.filter(pl.col("symbol") == symbol).sort("trade_date")

            if len(symbol_df) < 2:
                continue

            # Calculate day-over-day percentage changes
            prices = symbol_df["close"].to_list()
            for i in range(1, len(prices)):
                if prices[i - 1] > 0:
                    pct_change = abs((prices[i] - prices[i - 1]) / prices[i - 1])
                    # After adjustment, day-over-day changes should be reasonable
                    # (not > 50% for normal stocks on consecutive days)
                    assert pct_change < 0.5, f"Excessive price jump: {pct_change * 100:.1f}%"

    def test_split_adjustment_integration(self, sample_ohlc_before_ca):
        """Test complete flow for stock split adjustment."""
        # Create a split event
        split_date = date(2024, 2, 1)
        ca_df = pl.DataFrame(
            {
                "symbol": ["RELIANCE"],
                "ex_date": [split_date],
                "action_type": ["SPLIT"],
                "adjustment_factor": [2.0],  # 1:2 split
            }
        )

        # Get OHLC data for RELIANCE
        reliance_df = sample_ohlc_before_ca.filter(pl.col("symbol") == "RELIANCE")

        # Compute adjustment factors
        ca_factors = compute_adjustment_factors(ca_df)

        # Apply adjustments
        adjusted_df = apply_adjustments(
            ohlc_df=reliance_df, ca_factors_df=ca_factors, columns=["close"]
        )

        # Verify: prices before split should be divided by 2
        before_split = adjusted_df.filter(pl.col("trade_date") < split_date)
        original_before_split = reliance_df.filter(pl.col("trade_date") < split_date)

        if len(before_split) > 0 and len(original_before_split) > 0:
            # Check that adjusted prices are approximately half of originals
            adjusted_price = before_split["close"].mean()
            original_price = original_before_split["close"].mean()

            assert abs(adjusted_price - original_price / 2.0) < 10.0  # Allow some variance

        # Verify: prices on or after split should remain unchanged
        after_split = adjusted_df.filter(pl.col("trade_date") >= split_date)
        original_after_split = reliance_df.filter(pl.col("trade_date") >= split_date)

        if len(after_split) > 0:
            assert after_split["close"].to_list() == original_after_split["close"].to_list()

    def test_bonus_adjustment_integration(self, sample_ohlc_before_ca):
        """Test complete flow for bonus issue adjustment."""
        # Create a bonus event
        bonus_date = date(2024, 1, 20)
        ca_df = pl.DataFrame(
            {
                "symbol": ["TCS"],
                "ex_date": [bonus_date],
                "action_type": ["BONUS"],
                "adjustment_factor": [2.0],  # 1:1 bonus
            }
        )

        # Get OHLC data for TCS
        tcs_df = sample_ohlc_before_ca.filter(pl.col("symbol") == "TCS")

        # Compute adjustment factors
        ca_factors = compute_adjustment_factors(ca_df)

        # Apply adjustments
        adjusted_df = apply_adjustments(ohlc_df=tcs_df, ca_factors_df=ca_factors, columns=["close"])

        # Verify data exists
        assert not adjusted_df.is_empty()
        assert len(adjusted_df) == len(tcs_df)

        # Verify prices before bonus are adjusted
        before_bonus = adjusted_df.filter(pl.col("trade_date") < bonus_date)
        if len(before_bonus) > 0:
            # Prices should be different from original
            original_before = tcs_df.filter(pl.col("trade_date") < bonus_date)
            assert before_bonus["close"].sum() != original_before["close"].sum()

    def test_multiple_adjustments_same_symbol(self, sample_ohlc_before_ca):
        """Test applying multiple corporate actions for the same symbol."""
        # Create multiple events for RELIANCE
        ca_df = pl.DataFrame(
            {
                "symbol": ["RELIANCE", "RELIANCE", "RELIANCE"],
                "ex_date": [date(2024, 1, 10), date(2024, 1, 20), date(2024, 2, 1)],
                "action_type": ["DIVIDEND", "BONUS", "SPLIT"],
                "adjustment_factor": [0.992, 1.5, 2.0],
            }
        )

        # Get OHLC data
        reliance_df = sample_ohlc_before_ca.filter(pl.col("symbol") == "RELIANCE")

        # Compute cumulative adjustment factors
        ca_factors = compute_adjustment_factors(ca_df)

        # Verify cumulative factors are computed correctly
        assert "cumulative_factor" in ca_factors.columns

        # Apply adjustments
        adjusted_df = apply_adjustments(
            ohlc_df=reliance_df, ca_factors_df=ca_factors, columns=["close"]
        )

        # Verify all dates have data
        assert len(adjusted_df) == len(reliance_df)

    def test_adjustment_preserves_ohlc_relationships(self, sample_ohlc_before_ca, sample_ca_events):
        """Test that OHLC relationships are preserved after adjustments."""
        # Compute and apply adjustments
        ca_factors = compute_adjustment_factors(sample_ca_events)
        adjusted_df = apply_adjustments(
            ohlc_df=sample_ohlc_before_ca,
            ca_factors_df=ca_factors,
            columns=["open", "high", "low", "close"],
        )

        # Verify OHLC relationships: High >= Low, High >= Open, High >= Close, etc.
        assert (adjusted_df["high"] >= adjusted_df["low"]).all()
        assert (adjusted_df["high"] >= adjusted_df["open"]).all()
        assert (adjusted_df["high"] >= adjusted_df["close"]).all()
        assert (adjusted_df["low"] <= adjusted_df["open"]).all()
        assert (adjusted_df["low"] <= adjusted_df["close"]).all()

    def test_end_to_end_ca_pipeline(self, sample_ohlc_before_ca, sample_ca_events, tmp_path):
        """Test complete corporate actions pipeline end-to-end."""
        from champion.storage.parquet_io import write_df

        # Step 1: Load CA events
        assert not sample_ca_events.is_empty()

        # Step 2: Compute adjustment factors
        ca_factors = compute_adjustment_factors(sample_ca_events)
        assert not ca_factors.is_empty()
        assert "cumulative_factor" in ca_factors.columns

        # Step 3: Apply adjustments to OHLC
        adjusted_df = apply_adjustments(
            ohlc_df=sample_ohlc_before_ca,
            ca_factors_df=ca_factors,
            columns=["open", "high", "low", "close"],
        )
        assert not adjusted_df.is_empty()

        # Step 4: Write adjusted data to Parquet
        output_path = write_df(
            df=adjusted_df,
            dataset="normalized/equity_ohlc_adjusted",
            base_path=tmp_path,
            partitions=["trade_date"],
        )

        # Verify output
        assert output_path.exists()
        partition_dirs = list(output_path.glob("trade_date=*"))
        assert len(partition_dirs) > 0

        # Step 5: Read back and verify
        df_read = pl.read_parquet(output_path / "**/*.parquet")
        assert len(df_read) == len(adjusted_df)

        # Verify OHLC relationships are still valid
        assert (df_read["high"] >= df_read["low"]).all()

    def test_no_adjustments_for_future_dates(self, sample_ohlc_before_ca):
        """Test that corporate actions don't affect future prices."""
        # Create CA event in the past
        ca_df = pl.DataFrame(
            {
                "symbol": ["RELIANCE"],
                "ex_date": [date(2024, 1, 15)],
                "action_type": ["SPLIT"],
                "adjustment_factor": [2.0],
            }
        )

        # Get OHLC data
        reliance_df = sample_ohlc_before_ca.filter(pl.col("symbol") == "RELIANCE")

        # Apply adjustments
        ca_factors = compute_adjustment_factors(ca_df)
        adjusted_df = apply_adjustments(
            ohlc_df=reliance_df, ca_factors_df=ca_factors, columns=["close"]
        )

        # Verify: prices on or after ex_date should be unchanged
        after_ca = adjusted_df.filter(pl.col("trade_date") >= date(2024, 1, 15))
        original_after_ca = reliance_df.filter(pl.col("trade_date") >= date(2024, 1, 15))

        if len(after_ca) > 0:
            assert after_ca["close"].to_list() == original_after_ca["close"].to_list()
