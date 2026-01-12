"""Corporate actions processing module.

This module handles:
- Corporate action event parsing and ingestion
- Adjustment factor computation for splits, bonuses, dividends
- Price adjustment application to historical OHLC data
"""

from .ca_processor import CorporateActionsProcessor, compute_adjustment_factors
from .price_adjuster import apply_ca_adjustments

__all__ = [
    "CorporateActionsProcessor",
    "compute_adjustment_factors",
    "apply_ca_adjustments",
]
