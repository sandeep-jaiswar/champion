"""Unified application configuration with environment support.

This module consolidates all configuration into a single source of truth,
supporting different environments (dev, staging, prod).

Configuration hierarchy:
    1. Environment variables (highest priority)
    2. .env file
    3. Environment-specific defaults (.env.dev, .env.prod)
    4. Built-in defaults
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    """Deployment environment."""

    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


# ============================================================================
# Data Source Configuration
# ============================================================================


class NSEConfig(BaseSettings):
    """NSE (National Stock Exchange) data source configuration."""

    bhavcopy_url: str = Field(
        default="https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip",
        description="NSE bhavcopy (OHLC) download URL",
    )
    equity_list_url: str = Field(
        default="https://archives.nseindia.com/content/equities/EQUITY_L.csv",
        description="NSE equity master list URL",
    )
    corporate_actions_url: str = Field(
        default="https://www.nseindia.com/api/corporates-corporateActions",
        description="NSE corporate actions API endpoint",
    )
    option_chain_url: str = Field(
        default="https://www.nseindia.com/api/option-chain-{instrument}",
        description="NSE option chain API template",
    )
    trading_calendar_url: str = Field(
        default="https://www.nseindia.com/api/holiday-master?type=trading",
        description="NSE trading calendar API",
    )
    holiday_calendar_url: str = Field(
        default="https://www.nseindia.com/api/holiday-master?type=trading",
        description="NSE holiday calendar API (alias)",
    )
    index_constituents_url: str = Field(
        default="https://www.nseindia.com/api/index-constituents?index={index_name}",
        description="NSE index constituents API",
    )
    ca_url: str = Field(
        default="https://www.nseindia.com/api/corporates-corporateActions",
        description="NSE corporate actions URL (alias)",
    )

    model_config = SettingsConfigDict(env_prefix="NSE_", extra="allow")


class BSEConfig(BaseSettings):
    """BSE (Bombay Stock Exchange) data source configuration."""

    bhavcopy_url: str = Field(
        default="https://www.bseindia.com/download/BhavCopy/Equity/EQ{date}_CSV.ZIP",
        description="BSE bhavcopy download URL",
    )
    equity_list_url: str = Field(
        default="https://www.bseindia.com/corporates/List_Scrips.html",
        description="BSE equity list URL",
    )

    model_config = SettingsConfigDict(env_prefix="BSE_", extra="allow")


# ============================================================================
# Messaging Configuration
# ============================================================================


class KafkaConfig(BaseSettings):
    """Apache Kafka configuration."""

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka broker addresses",
    )
    schema_registry_url: str = Field(
        default="http://localhost:8081",
        description="Kafka schema registry URL",
    )
    security_protocol: str = Field(
        default="PLAINTEXT",
        description="Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)",
    )
    sasl_mechanism: str | None = Field(
        default=None,
        description="SASL mechanism (PLAIN, SCRAM-SHA-256, etc)",
    )
    sasl_username: str | None = Field(default=None)
    sasl_password: str | None = Field(default=None)

    # Producer configs
    acks: str = Field(default="all", description="Producer acks setting")
    compression_type: str = Field(default="snappy")
    batch_size: int = Field(default=16384)
    linger_ms: int = Field(default=10)

    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="allow")


class TopicConfig(BaseSettings):
    """Kafka topic names."""

    raw_equity_ohlc: str = Field(default="raw.market.equity.ohlc")
    option_chain: str = Field(default="raw.market.option_chain")
    symbol_master: str = Field(default="reference.nse.symbol_master")
    corporate_actions: str = Field(default="reference.nse.corporate_actions")
    trading_calendar: str = Field(default="reference.nse.trading_calendar")
    index_constituents: str = Field(default="reference.nse.index_constituents")

    model_config = SettingsConfigDict(env_prefix="TOPIC_", extra="allow")


# ============================================================================
# Warehouse Configuration
# ============================================================================


class ClickHouseConfig(BaseSettings):
    """ClickHouse OLAP database configuration."""

    host: str = Field(default="localhost", description="ClickHouse server host")
    http_port: int = Field(default=8123, description="HTTP port")
    native_port: int = Field(default=9000, description="Native protocol port")
    user: str = Field(default="champion_user", description="Database user")
    password: str = Field(default="champion_pass", description="Database password")
    database: str = Field(default="champion_market", description="Database name")

    # Performance tuning
    connect_timeout: int = Field(default=10)
    send_timeout: int = Field(default=30)
    recv_timeout: int = Field(default=30)
    batch_size: int = Field(default=100000, description="Batch size for bulk operations")

    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_", extra="allow")


# ============================================================================
# Storage Configuration
# ============================================================================


class StorageConfig(BaseSettings):
    """Local storage configuration."""

    data_dir: Path = Field(
        default=Path("./data"),
        description="Root data directory for all data artifacts",
    )
    archive_retention_days: int = Field(
        default=30,
        ge=1,
        description="Days to retain archived data",
    )
    parquet_compression: str = Field(
        default="snappy",
        description="Parquet compression codec (snappy, gzip, lz4, zstd)",
    )

    @field_validator("data_dir", mode="before")
    @classmethod
    def ensure_data_dir_exists(cls, v: Path | str) -> Path:
        """Create data directory if it doesn't exist."""
        if isinstance(v, str):
            v = Path(v)
        v = v.resolve()
        v.mkdir(parents=True, exist_ok=True)
        return v

    model_config = SettingsConfigDict(env_prefix="", extra="allow")


# ============================================================================
# Scraper Configuration
# ============================================================================


class ScraperConfig(BaseSettings):
    """Data scraper behavior configuration."""

    retry_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Number of retry attempts for failed requests",
    )
    retry_delay_seconds: int = Field(
        default=60,
        ge=1,
        description="Delay between retries in seconds",
    )
    request_timeout_seconds: int = Field(
        default=300,
        ge=30,
        description="Request timeout in seconds",
    )
    timeout: int = Field(
        default=300,
        ge=30,
        description="HTTP request timeout in seconds (alias for request_timeout_seconds)",
    )
    user_agent: str = Field(
        default="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        description="HTTP User-Agent header",
    )
    rate_limit_requests_per_second: float = Field(
        default=10.0,
        gt=0,
        description="Request rate limiting",
    )

    model_config = SettingsConfigDict(env_prefix="SCRAPER_", extra="allow")


class CircuitBreakerConfig(BaseSettings):
    """Circuit breaker for fault tolerance."""

    failure_threshold: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Consecutive failures before opening circuit",
    )
    recovery_timeout_seconds: int = Field(
        default=300,
        ge=60,
        le=3600,
        description="Time to wait before attempting recovery",
    )
    success_threshold: int = Field(
        default=2,
        ge=1,
        description="Consecutive successes to close circuit",
    )

    model_config = SettingsConfigDict(env_prefix="CIRCUIT_BREAKER_", extra="allow")


# ============================================================================
# Observability Configuration
# ============================================================================


class LoggingConfig(BaseSettings):
    """Logging configuration."""

    level: str = Field(
        default="INFO",
        description="Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    format: str = Field(
        default="json",
        description="Log format (json, console)",
    )
    include_context: bool = Field(
        default=True,
        description="Include request context in logs",
    )

    @field_validator("level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v_upper

    model_config = SettingsConfigDict(env_prefix="LOG_", extra="allow")


class MetricsConfig(BaseSettings):
    """Prometheus metrics configuration."""

    enabled: bool = Field(default=True, description="Enable metrics collection")
    port: int = Field(default=9090, ge=1024, le=65535, description="Metrics server port")
    prefix: str = Field(default="champion_", description="Metrics name prefix")

    model_config = SettingsConfigDict(env_prefix="METRICS_", extra="allow")


class TracingConfig(BaseSettings):
    """Distributed tracing configuration."""

    enabled: bool = Field(default=False, description="Enable tracing")
    jaeger_agent_host: str = Field(default="localhost")
    jaeger_agent_port: int = Field(default=6831)
    sample_rate: float = Field(default=0.1, ge=0.0, le=1.0)

    model_config = SettingsConfigDict(env_prefix="TRACING_", extra="allow")


class ObservabilityConfig(BaseSettings):
    """Unified observability configuration."""

    environment: Environment = Field(default=Environment.DEV)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    tracing: TracingConfig = Field(default_factory=TracingConfig)

    model_config = SettingsConfigDict(env_prefix="OBS_", extra="allow")


# ============================================================================
# Unified Application Configuration
# ============================================================================


class AppConfig(BaseSettings):
    """Master configuration for Champion platform.

    All sub-configurations are included here for easy access:
        config.nse.bhavcopy_url
        config.clickhouse.host
        config.kafka.bootstrap_servers
        etc.
    """

    # Environment
    environment: Environment = Field(default=Environment.DEV)

    # Data Sources
    nse: NSEConfig = Field(default_factory=NSEConfig)
    bse: BSEConfig = Field(default_factory=BSEConfig)

    # Messaging
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    topics: TopicConfig = Field(default_factory=TopicConfig)

    # Warehouse
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)

    # Storage
    storage: StorageConfig = Field(default_factory=StorageConfig)

    # Behavior
    scraper: ScraperConfig = Field(default_factory=ScraperConfig)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)

    # Observability
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    def is_dev(self) -> bool:
        """Check if running in development environment."""
        return self.environment == Environment.DEV

    def is_prod(self) -> bool:
        """Check if running in production environment."""
        return self.environment == Environment.PROD


# Global configuration instance
config = AppConfig()


def get_config() -> AppConfig:
    """Get the global application configuration.

    Returns:
        AppConfig instance
    """
    return config


def reload_config() -> AppConfig:
    """Reload configuration from environment.

    Useful for testing or dynamic configuration changes.

    Returns:
        New AppConfig instance
    """
    global config
    config = AppConfig()
    return config
