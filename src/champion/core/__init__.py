"""Core infrastructure and interfaces for Champion platform.

This module provides foundational components for all domain packages:
- Configuration management
- Dependency injection
- Logging and observability
- Error handling
- Plugin system
- Abstract interfaces
"""

from .config import AppConfig, Environment, get_config, reload_config
from .di import Container, ServiceLocator, get_container
from .errors import (
    ChampionError,
    ConfigError,
    DataError,
    IntegrationError,
    ValidationError,
)
from .interfaces import (
    CacheBackend,
    DataContext,
    DataSink,
    DataSource,
    Observer,
    Repository,
    Scraper,
    Transformer,
    Validator,
)
from .logging import configure_logging, get_logger, get_request_id, set_request_id

__all__ = [
    # Config
    "AppConfig",
    "get_config",
    "reload_config",
    "Environment",
    # DI
    "Container",
    "ServiceLocator",
    "get_container",
    # Errors
    "ChampionError",
    "ValidationError",
    "DataError",
    "IntegrationError",
    "ConfigError",
    # Interfaces
    "DataSource",
    "DataSink",
    "Transformer",
    "Validator",
    "Scraper",
    "Observer",
    "Repository",
    "CacheBackend",
    "DataContext",
    # Logging
    "get_logger",
    "configure_logging",
    "get_request_id",
    "set_request_id",
]
