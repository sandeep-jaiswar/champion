"""Champion REST API Module.

This module provides REST API endpoints for accessing market data,
technical indicators, and corporate actions.
"""

from champion.api.main import create_app

__all__ = ["create_app"]
