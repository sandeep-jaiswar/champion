"""Feature engineering and analytics layer.

Transforms raw market data into derived features and technical indicators.
Implements the champion.core.Transformer interface.

## Submodules

- `indicators.py`: Technical indicator calculations (SMA, EMA, RSI, etc)
- `portfolio.py`: Portfolio-level analytics
- `risk.py`: Risk metrics and measurements
"""

from .indicators import compute_ema, compute_features, compute_rsi, compute_sma

__all__ = [
    "compute_sma",
    "compute_ema",
    "compute_rsi",
    "compute_features",
]
