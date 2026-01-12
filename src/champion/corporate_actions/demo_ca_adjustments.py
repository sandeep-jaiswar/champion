"""End-to-end integration example for Corporate Actions processing.

This example demonstrates:
1. Loading corporate actions data
2. Computing adjustment factors
3. Applying adjustments to OHLC data
4. Verifying price continuity
"""

from datetime import date
from pathlib import Path

import polars as pl

from champion.corporate_actions.ca_processor import (
    CorporateActionsProcessor,
    compute_adjustment_factors,
)
from champion.corporate_actions.price_adjuster import apply_ca_adjustments


def create_sample_ca_data() -> pl.DataFrame:
    """Create sample corporate actions data for testing.

    Returns:
        DataFrame with sample CA events
    """
    return pl.DataFrame(
        {
            "symbol": [
                "RELIANCE",
                "TCS",
                "INFY",
                "RELIANCE",  # Multiple events for RELIANCE
            ],
            "ex_date": [
                date(2024, 1, 15),  # RELIANCE split
                date(2024, 2, 20),  # TCS bonus
                date(2024, 3, 10),  # INFY dividend
                date(2024, 2, 25),  # RELIANCE bonus
            ],
            "action_type": ["SPLIT", "BONUS", "DIVIDEND", "BONUS"],
            "adjustment_factor": [
                5.0,  # 1:5 split
                1.5,  # 1:2 bonus
                0.98,  # Rs 2 dividend on Rs 100 stock
                1.5,  # 1:2 bonus
            ],
            "split_ratio": [
                {"old_shares": 1, "new_shares": 5},
                None,
                None,
                None,
            ],
            "bonus_ratio": [
                None,
                {"new_shares": 1, "existing_shares": 2},
                None,
                {"new_shares": 1, "existing_shares": 2},
            ],
            "dividend_amount": [None, None, 2.0, None],
        }
    )


def create_sample_ohlc_data() -> pl.DataFrame:
    """Create sample OHLC data for testing.

    Returns:
        DataFrame with sample OHLC data
    """
    return pl.DataFrame(
        {
            "TckrSymb": [
                # RELIANCE - before split
                "RELIANCE",
                "RELIANCE",
                "RELIANCE",
                # RELIANCE - after split, before bonus
                "RELIANCE",
                "RELIANCE",
                # RELIANCE - after bonus
                "RELIANCE",
                # TCS
                "TCS",
                "TCS",
                # INFY
                "INFY",
                "INFY",
            ],
            "TradDt": [
                # RELIANCE dates
                date(2024, 1, 8),
                date(2024, 1, 9),
                date(2024, 1, 10),
                date(2024, 1, 16),
                date(2024, 2, 20),
                date(2024, 2, 26),
                # TCS dates
                date(2024, 2, 15),
                date(2024, 2, 21),
                # INFY dates
                date(2024, 3, 5),
                date(2024, 3, 11),
            ],
            "OpnPric": [
                # RELIANCE
                2450.0,
                2480.0,
                2500.0,
                490.0,
                480.0,
                320.0,
                # TCS
                3600.0,
                2400.0,
                # INFY
                1500.0,
                1470.0,
            ],
            "HghPric": [
                2490.0,
                2510.0,
                2530.0,
                505.0,
                495.0,
                335.0,
                3650.0,
                2420.0,
                1520.0,
                1490.0,
            ],
            "LwPric": [
                2440.0,
                2470.0,
                2490.0,
                485.0,
                475.0,
                315.0,
                3590.0,
                2390.0,
                1490.0,
                1460.0,
            ],
            "ClsPric": [
                2470.0,
                2500.0,
                2520.0,
                500.0,
                490.0,
                330.0,
                3620.0,
                2410.0,
                1510.0,
                1480.0,
            ],
        }
    )


def main():
    """Run end-to-end CA adjustment example."""
    print("=" * 80)
    print("Corporate Actions Adjustment - End-to-End Example")
    print("=" * 80)
    print()

    # Step 1: Create sample data
    print("Step 1: Create sample corporate actions and OHLC data")
    ca_df = create_sample_ca_data()
    ohlc_df = create_sample_ohlc_data()

    print(f"  - Corporate Actions: {len(ca_df)} events")
    print(f"  - OHLC Records: {len(ohlc_df)} records")
    print()

    # Show CA events
    print("Corporate Actions:")
    print(
        ca_df.select(
            ["symbol", "ex_date", "action_type", "adjustment_factor"]
        )
    )
    print()

    # Step 2: Compute cumulative adjustment factors
    print("Step 2: Compute cumulative adjustment factors")
    ca_factors = compute_adjustment_factors(ca_df)
    print(
        ca_factors.select(
            ["symbol", "ex_date", "adjustment_factor", "cumulative_factor"]
        )
    )
    print()

    # Step 3: Apply adjustments to OHLC
    print("Step 3: Apply CA adjustments to OHLC prices")
    adjusted_ohlc = apply_ca_adjustments(ohlc_df, ca_factors)
    print()

    # Step 4: Show results for RELIANCE
    print("=" * 80)
    print("RELIANCE - Before and After Adjustment")
    print("=" * 80)

    reliance_before = ohlc_df.filter(pl.col("TckrSymb") == "RELIANCE")
    reliance_after = adjusted_ohlc.filter(pl.col("TckrSymb") == "RELIANCE")

    print("\nBefore Adjustment (Raw Prices):")
    print(reliance_before.select(["TradDt", "OpnPric", "ClsPric"]))

    print("\nAfter Adjustment (CA-Adjusted Prices):")
    print(
        reliance_after.select(
            [
                "TradDt",
                "OpnPric",
                "ClsPric",
                "adjustment_factor",
                "adjustment_date",
            ]
        )
    )

    # Step 5: Verify price continuity
    print("\n" + "=" * 80)
    print("Price Continuity Check")
    print("=" * 80)

    # For RELIANCE, check if adjusted prices show continuity
    reliance_sorted = reliance_after.sort("TradDt")
    close_prices = reliance_sorted["ClsPric"].to_list()

    print("\nRELIANCE Adjusted Close Prices:")
    for i, (dt, close) in enumerate(
        zip(
            reliance_sorted["TradDt"].to_list(),
            close_prices,
        )
    ):
        adj_factor = reliance_sorted["adjustment_factor"][i]
        print(
            f"  {dt}: ₹{close:7.2f} (adj factor: {adj_factor:.2f})"
        )

    # Check if price changes are reasonable (< 20% day-over-day)
    print("\nDay-over-day price changes:")
    for i in range(1, len(close_prices)):
        change_pct = (
            (close_prices[i] - close_prices[i - 1]) / close_prices[i - 1]
        ) * 100
        print(
            f"  {reliance_sorted['TradDt'][i-1]} → {reliance_sorted['TradDt'][i]}: {change_pct:+.2f}%"
        )

    # Step 6: Show TCS and INFY
    print("\n" + "=" * 80)
    print("TCS and INFY - Adjusted Prices")
    print("=" * 80)

    for symbol in ["TCS", "INFY"]:
        print(f"\n{symbol}:")
        symbol_data = adjusted_ohlc.filter(pl.col("TckrSymb") == symbol)
        print(
            symbol_data.select(
                [
                    "TradDt",
                    "ClsPric",
                    "adjustment_factor",
                ]
            )
        )

    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"  - Total CA events: {len(ca_df)}")
    print(f"  - Symbols affected: {ca_df['symbol'].n_unique()}")
    print(f"  - OHLC records adjusted: {len(adjusted_ohlc)}")
    print(
        f"  - Average adjustment factor: {adjusted_ohlc['adjustment_factor'].mean():.2f}"
    )
    print()
    print("✅ End-to-end CA adjustment completed successfully!")


if __name__ == "__main__":
    main()
