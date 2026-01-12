"""
Technical indicators and feature engineering module.

This module provides functions for computing technical indicators
from normalized OHLC data using Polars for efficient computation.
"""

from .indicators import compute_sma, compute_ema, compute_rsi, compute_features

__all__ = [
    "compute_sma",
    "compute_ema",
    "compute_rsi",
    "compute_features",
]
