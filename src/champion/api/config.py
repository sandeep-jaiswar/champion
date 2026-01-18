"""API Configuration."""

from pydantic import ConfigDict, Field
from pydantic_settings import BaseSettings


class APISettings(BaseSettings):
    """API configuration settings."""

    # API Settings
    api_title: str = Field(default="Champion Market Data API", description="API title")
    api_version: str = Field(default="1.0.0", description="API version")
    api_prefix: str = Field(default="/api/v1", description="API prefix")

    # Server Settings
    host: str = Field(default="0.0.0.0", description="API host")
    port: int = Field(default=8000, description="API port")

    # CORS Settings
    cors_origins: list[str] = Field(
        default=["http://localhost:3000", "http://localhost:8000"],
        description="Allowed CORS origins",
    )

    # Redis Cache Settings
    redis_host: str = Field(default="localhost", description="Redis host")
    redis_port: int = Field(default=6379, description="Redis port")
    redis_db: int = Field(default=0, description="Redis database")
    cache_ttl: int = Field(default=300, description="Cache TTL in seconds (5 minutes)")

    # Rate Limiting
    rate_limit_per_minute: int = Field(default=60, description="Rate limit per minute")

    # JWT Settings
    jwt_secret_key: str = Field(
        default="your-secret-key-change-this-in-production", description="JWT secret key"
    )
    jwt_algorithm: str = Field(default="HS256", description="JWT algorithm")
    jwt_expiration_minutes: int = Field(default=30, description="JWT token expiration in minutes")

    # Database Settings
    clickhouse_host: str = Field(default="localhost", description="ClickHouse host")
    clickhouse_port: int = Field(
        default=8123, description="ClickHouse HTTP port (8123) for API access"
    )
    clickhouse_user: str = Field(default="default", description="ClickHouse user")
    clickhouse_password: str = Field(default="", description="ClickHouse password")
    clickhouse_database: str = Field(default="champion", description="ClickHouse database")

    # Pagination
    default_page_size: int = Field(default=100, description="Default page size")
    max_page_size: int = Field(default=1000, description="Maximum page size")

    model_config = ConfigDict(
        env_file=".env",
        env_prefix="CHAMPION_",
        case_sensitive=False,
        extra="ignore"  # Ignore extra environment variables
    )


def get_api_settings() -> APISettings:
    """Get API settings instance."""
    return APISettings()
