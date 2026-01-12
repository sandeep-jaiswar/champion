"""Configuration management using Pydantic settings."""

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseSettings):
    """Kafka configuration."""

    bootstrap_servers: str = Field(default="localhost:9092")
    schema_registry_url: str = Field(default="http://localhost:8081")
    security_protocol: str = Field(default="PLAINTEXT")
    sasl_mechanism: str = Field(default="PLAIN")
    sasl_username: str | None = Field(default=None)
    sasl_password: str | None = Field(default=None)

    # Producer configs
    enable_idempotence: bool = Field(default=True)
    acks: str = Field(default="all")
    max_in_flight_requests: int = Field(default=5)
    compression_type: str = Field(default="snappy")
    batch_size: int = Field(default=16384)
    linger_ms: int = Field(default=10)

    model_config = SettingsConfigDict(env_prefix="KAFKA_")


class TopicConfig(BaseSettings):
    """Kafka topic names."""

    raw_ohlc: str = Field(default="raw.market.equity.ohlc")
    option_chain: str = Field(default="raw.market.option_chain")
    symbol_master: str = Field(default="reference.nse.symbol_master")
    corporate_actions: str = Field(default="reference.nse.corporate_actions")
    trading_calendar: str = Field(default="reference.nse.trading_calendar")

    model_config = SettingsConfigDict(env_prefix="TOPIC_")


class NSEConfig(BaseSettings):
    """NSE data source configuration."""

    bhavcopy_url: str = Field(
        default="https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
    )
    equity_list_url: str = Field(
        default="https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    )
    ca_url: str = Field(default="https://www.nseindia.com/api/corporates-corporateActions")
    option_chain_url: str = Field(default="https://www.nseindia.com/api/option-chain-{instrument}")
    holiday_calendar_url: str = Field(
        default="https://www.nseindia.com/api/holiday-master?type=trading"
    )

    model_config = SettingsConfigDict(env_prefix="NSE_")


class BSEConfig(BaseSettings):
    """BSE data source configuration."""

    bhavcopy_url: str = Field(
        default="https://www.bseindia.com/download/BhavCopy/Equity/EQ{date}_CSV.ZIP"
    )
    equity_list_url: str = Field(default="https://www.bseindia.com/corporates/List_Scrips.html")

    model_config = SettingsConfigDict(env_prefix="BSE_")


class ScraperConfig(BaseSettings):
    """Scraper behavior configuration."""

    retry_attempts: int = Field(default=3, ge=1, le=10)
    retry_delay: int = Field(default=60, ge=1)
    timeout: int = Field(default=300, ge=30)
    user_agent: str = Field(default="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")

    model_config = SettingsConfigDict(env_prefix="SCRAPER_")


class StorageConfig(BaseSettings):
    """Storage configuration."""

    data_dir: Path = Field(default=Path("./data"))
    archive_retention_days: int = Field(default=30, ge=1)

    @field_validator("data_dir")
    def ensure_data_dir_exists(cls, v: Path) -> Path:
        """Create data directory if it doesn't exist."""
        v = v.resolve()  # Convert to absolute path
        v.mkdir(parents=True, exist_ok=True)
        return v

    model_config = SettingsConfigDict(env_prefix="")


class ObservabilityConfig(BaseSettings):
    """Observability configuration."""

    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json")
    metrics_port: int = Field(default=9090, ge=1024, le=65535)
    tracing_enabled: bool = Field(default=False)
    jaeger_agent_host: str = Field(default="localhost")
    jaeger_agent_port: int = Field(default=6831)

    @field_validator("log_level")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is a valid logging level.

        Args:
            v: Log level string

        Returns:
            Normalized uppercase log level

        Raises:
            ValueError: If log level is invalid
        """
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v_upper

    model_config = SettingsConfigDict(env_prefix="")


class MonitoringConfig(BaseSettings):
    """Monitoring and alerting configuration."""

    alert_webhook_url: str | None = Field(default=None)
    slack_webhook_url: str | None = Field(default=None)

    model_config = SettingsConfigDict(env_prefix="")


class CircuitBreakerConfig(BaseSettings):
    """Circuit breaker configuration for data sources."""

    # NSE configuration
    nse_failure_threshold: int = Field(default=5, ge=1, le=20)
    nse_recovery_timeout: int = Field(default=300, ge=60, le=3600)

    # BSE configuration
    bse_failure_threshold: int = Field(default=5, ge=1, le=20)
    bse_recovery_timeout: int = Field(default=300, ge=60, le=3600)

    model_config = SettingsConfigDict(env_prefix="CIRCUIT_BREAKER_")


class Config(BaseSettings):
    """Main application configuration."""

    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    topics: TopicConfig = Field(default_factory=TopicConfig)
    nse: NSEConfig = Field(default_factory=NSEConfig)
    bse: BSEConfig = Field(default_factory=BSEConfig)
    scraper: ScraperConfig = Field(default_factory=ScraperConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",  # Ignore extra fields from .env that don't map to the schema
    )


# Global config instance
config = Config()
