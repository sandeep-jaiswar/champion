"""Configuration re-exports for backward compatibility.

The unified configuration is now in champion.core.config.
This module maintains backward compatibility with existing imports.

Deprecated: Use `from champion.core import AppConfig, get_config` instead.
"""

from champion.core import (
    AppConfig,
    get_config,
    reload_config,
    Environment,
)

__all__ = [
    "AppConfig",
    "get_config",
    "reload_config",
    "Environment",
]

# Alias for backward compatibility
config = get_config()
