"""Tests for corporate actions processor."""

from datetime import date

import polars as pl
import pytest

from champion.corporate_actions.ca_processor import (
    CorporateActionsProcessor,
    compute_adjustment_factors,
)


class TestCorporateActionsProcessor:
    """Test suite for corporate actions processor."""

    def setup_method(self):
        """Set up test fixtures."""
        self.processor = CorporateActionsProcessor()

    def test_compute_split_adjustment_1_to_5(self):
        """Test 1:5 split adjustment (1 share becomes 5)."""
        # 1:5 split means prices should be divided by 5
        factor = self.processor.compute_split_adjustment(old_shares=1, new_shares=5)
        assert factor == 5.0

    def test_compute_split_adjustment_2_to_1(self):
        """Test 2:1 reverse split (2 shares become 1)."""
        # 2:1 reverse split means prices should be multiplied by 2
        factor = self.processor.compute_split_adjustment(old_shares=2, new_shares=1)
        assert factor == 0.5

    def test_compute_split_adjustment_invalid(self):
        """Test invalid split ratio."""
        with pytest.raises(ValueError):
            self.processor.compute_split_adjustment(old_shares=0, new_shares=5)

        with pytest.raises(ValueError):
            self.processor.compute_split_adjustment(old_shares=1, new_shares=-5)

    def test_compute_bonus_adjustment_1_to_2(self):
        """Test 1:2 bonus (1 bonus share for every 2 existing)."""
        # 1:2 bonus means 2 shares become 3 total
        # Prices should be multiplied by 2/3
        factor = self.processor.compute_bonus_adjustment(new_shares=1, existing_shares=2)
        assert factor == 1.5  # (2+1)/2

    def test_compute_bonus_adjustment_1_to_1(self):
        """Test 1:1 bonus (1 bonus share for every 1 existing)."""
        # 1:1 bonus means shares double
        factor = self.processor.compute_bonus_adjustment(new_shares=1, existing_shares=1)
        assert factor == 2.0  # (1+1)/1

    def test_compute_bonus_adjustment_invalid(self):
        """Test invalid bonus ratio."""
        with pytest.raises(ValueError):
            self.processor.compute_bonus_adjustment(new_shares=0, existing_shares=2)

        with pytest.raises(ValueError):
            self.processor.compute_bonus_adjustment(new_shares=1, existing_shares=-2)

    def test_compute_dividend_adjustment(self):
        """Test dividend adjustment."""
        # Rs 10 dividend on Rs 100 stock
        # Adjustment factor = (100 - 10) / 100 = 0.9
        factor = self.processor.compute_dividend_adjustment(dividend_amount=10.0, close_price=100.0)
        assert factor == 0.9

    def test_compute_dividend_adjustment_small(self):
        """Test small dividend adjustment."""
        # Rs 2 dividend on Rs 100 stock
        factor = self.processor.compute_dividend_adjustment(dividend_amount=2.0, close_price=100.0)
        assert factor == 0.98

    def test_compute_dividend_adjustment_invalid(self):
        """Test invalid dividend parameters."""
        with pytest.raises(ValueError):
            self.processor.compute_dividend_adjustment(dividend_amount=-10.0, close_price=100.0)

        with pytest.raises(ValueError):
            self.processor.compute_dividend_adjustment(dividend_amount=10.0, close_price=0.0)


class TestComputeAdjustmentFactors:
    """Test suite for adjustment factor computation."""

    def test_compute_adjustment_factors_empty(self):
        """Test with empty DataFrame."""
        ca_df = pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "ex_date": pl.Date,
                "action_type": pl.Utf8,
                "adjustment_factor": pl.Float64,
            }
        )

        result = compute_adjustment_factors(ca_df)
        assert result.is_empty()
        assert "cumulative_factor" in result.columns

    def test_compute_adjustment_factors_single_event(self):
        """Test with single corporate action."""
        ca_df = pl.DataFrame(
            {
                "symbol": ["RELIANCE"],
                "ex_date": [date(2024, 1, 15)],
                "action_type": ["SPLIT"],
                "adjustment_factor": [5.0],
            }
        )

        result = compute_adjustment_factors(ca_df)
        assert len(result) == 1
        assert result["cumulative_factor"][0] == 5.0

    def test_compute_adjustment_factors_multiple_events_same_symbol(self):
        """Test with multiple events for same symbol."""
        # RELIANCE had 1:5 split on Jan 15, then 1:2 bonus on Feb 20
        ca_df = pl.DataFrame(
            {
                "symbol": ["RELIANCE", "RELIANCE"],
                "ex_date": [date(2024, 1, 15), date(2024, 2, 20)],
                "action_type": ["SPLIT", "BONUS"],
                "adjustment_factor": [5.0, 1.5],
            }
        )

        result = compute_adjustment_factors(ca_df)
        assert len(result) == 2

        # Sort by date descending (most recent first)
        result = result.sort("ex_date", descending=True)

        # Most recent event (Feb 20) has factor 1.5
        assert result["cumulative_factor"][0] == 1.5

        # Older event (Jan 15) has cumulative factor 1.5 * 5.0 = 7.5
        assert result["cumulative_factor"][1] == 7.5

    def test_compute_adjustment_factors_multiple_symbols(self):
        """Test with multiple symbols."""
        ca_df = pl.DataFrame(
            {
                "symbol": ["RELIANCE", "TCS", "INFY"],
                "ex_date": [
                    date(2024, 1, 15),
                    date(2024, 2, 20),
                    date(2024, 3, 10),
                ],
                "action_type": ["SPLIT", "BONUS", "DIVIDEND"],
                "adjustment_factor": [5.0, 1.5, 0.98],
            }
        )

        result = compute_adjustment_factors(ca_df)
        assert len(result) == 3

        # Each symbol has its own cumulative factor
        reliance = result.filter(pl.col("symbol") == "RELIANCE")
        assert reliance["cumulative_factor"][0] == 5.0

        tcs = result.filter(pl.col("symbol") == "TCS")
        assert tcs["cumulative_factor"][0] == 1.5

        infy = result.filter(pl.col("symbol") == "INFY")
        assert infy["cumulative_factor"][0] == pytest.approx(0.98)

    def test_compute_adjustment_factors_chronological_order(self):
        """Test that adjustments are applied in correct chronological order."""
        # Events out of order in input
        ca_df = pl.DataFrame(
            {
                "symbol": ["RELIANCE", "RELIANCE", "RELIANCE"],
                "ex_date": [
                    date(2024, 3, 10),  # Most recent
                    date(2024, 1, 15),  # Oldest
                    date(2024, 2, 20),  # Middle
                ],
                "action_type": ["BONUS", "SPLIT", "DIVIDEND"],
                "adjustment_factor": [1.5, 5.0, 0.98],
            }
        )

        result = compute_adjustment_factors(ca_df)
        result = result.sort("ex_date", descending=True)

        # Most recent (Mar 10): 1.5
        assert result["cumulative_factor"][0] == 1.5

        # Middle (Feb 20): 1.5 * 0.98 = 1.47
        assert result["cumulative_factor"][1] == pytest.approx(1.47)

        # Oldest (Jan 15): 1.5 * 0.98 * 5.0 = 7.35
        assert result["cumulative_factor"][2] == pytest.approx(7.35)


class TestCorporateActionIntegration:
    """Integration tests for corporate actions."""

    def test_realistic_split_scenario(self):
        """Test realistic split scenario with RELIANCE."""
        # RELIANCE 1:5 split on some date
        processor = CorporateActionsProcessor()

        # Pre-split price: Rs 2500
        # Post-split price should be: Rs 500
        split_factor = processor.compute_split_adjustment(old_shares=1, new_shares=5)

        pre_split_price = 2500.0
        adjusted_price = pre_split_price / split_factor

        assert adjusted_price == 500.0

    def test_realistic_bonus_scenario(self):
        """Test realistic bonus scenario."""
        # 1:2 bonus issue
        processor = CorporateActionsProcessor()

        # Pre-bonus price: Rs 150
        # After 1:2 bonus, theoretical price: Rs 100
        bonus_factor = processor.compute_bonus_adjustment(new_shares=1, existing_shares=2)

        pre_bonus_price = 150.0
        adjusted_price = pre_bonus_price / bonus_factor

        assert adjusted_price == 100.0

    def test_realistic_dividend_scenario(self):
        """Test realistic dividend scenario."""
        # Rs 15 dividend on Rs 300 stock
        processor = CorporateActionsProcessor()

        dividend_factor = processor.compute_dividend_adjustment(
            dividend_amount=15.0, close_price=300.0
        )

        # Adjustment factor should be 0.95
        assert dividend_factor == 0.95

        # Historical price of Rs 300 should be adjusted to Rs 285
        historical_price = 300.0
        adjusted_price = historical_price * dividend_factor

        assert adjusted_price == 285.0
