# ClickHouse Environment Configuration

## Overview

All ClickHouse configuration is now centralized in the `.env` file using the `CHAMPION_CLICKHOUSE_*` prefix. This ensures consistent configuration across all components of the application.

## Configuration Variables

The following environment variables are used throughout the application:

```env
# ClickHouse Configuration
# Note: Port 8123 is the HTTP interface (used by clickhouse_connect library)
# Port 9000 is the native protocol (used by native clients)
CHAMPION_CLICKHOUSE_HOST=localhost
CHAMPION_CLICKHOUSE_PORT=8123
CHAMPION_CLICKHOUSE_USER=default
CHAMPION_CLICKHOUSE_PASSWORD=
CHAMPION_CLICKHOUSE_DATABASE=champion
```

### Port Configuration

- **Port 8123**: HTTP interface - Used by the `clickhouse_connect` Python library and most API interactions
- **Port 9000**: Native protocol - Used for high-performance batch operations and native client connections

**Important**: The application primarily uses port 8123 (HTTP interface) by default.

## Components Updated

### 1. API Configuration (`src/champion/api/config.py`)

```python
class APISettings(BaseSettings):
    clickhouse_host: str = Field(default="localhost", description="ClickHouse host")
    clickhouse_port: int = Field(default=8123, description="ClickHouse HTTP port")
    clickhouse_user: str = Field(default="default", description="ClickHouse user")
    clickhouse_password: str = Field(default="", description="ClickHouse password")
    clickhouse_database: str = Field(default="champion", description="ClickHouse database")
    
    model_config = ConfigDict(
        env_prefix="CHAMPION_",
        extra="ignore"
    )
```

### 2. ClickHouse Sink Adapter (`src/champion/warehouse/adapters.py`)

The `ClickHouseSink` class now reads from environment variables:

```python
def __init__(
    self,
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    **kwargs,
):
    self.host = host or os.getenv("CHAMPION_CLICKHOUSE_HOST", "localhost")
    self.port = port or int(os.getenv("CHAMPION_CLICKHOUSE_PORT", "8123"))
    self.user = user or os.getenv("CHAMPION_CLICKHOUSE_USER", "default")
    self.password = password or os.getenv("CHAMPION_CLICKHOUSE_PASSWORD", "")
    self.database = database or os.getenv("CHAMPION_CLICKHOUSE_DATABASE", "champion")
```

### 3. ClickHouse Batch Loader (`src/champion/warehouse/clickhouse/batch_loader.py`)

Similar updates to read from environment variables with proper defaults.

### 4. Core Configuration (`src/champion/core/config.py`)

```python
class ClickHouseConfig(BaseSettings):
    host: str = Field(default="localhost")
    http_port: int = Field(default=8123)
    native_port: int = Field(default=9000)
    user: str = Field(default="default")
    password: str = Field(default="")
    database: str = Field(default="champion")
    
    model_config = SettingsConfigDict(env_prefix="CHAMPION_CLICKHOUSE_", extra="allow")
```

### 5. Orchestration Flows

Updated all Prefect flows to use `CHAMPION_CLICKHOUSE_*` variables:

- `src/champion/orchestration/flows/flows.py`
- `src/champion/orchestration/flows/normalization_job.py`

### 6. Scripts

- `scripts/preflight_check.py` - Health check script
- `scripts/init_demo_user.py` - Already using correct variables

## Usage

### Starting ClickHouse with Docker

```bash
# Start ClickHouse container with both HTTP and native ports
docker run -d \
  -p 8123:8123 \
  -p 9000:9000 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD= \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  --name champion-clickhouse \
  clickhouse/clickhouse-server

# Create the champion database
echo "CREATE DATABASE IF NOT EXISTS champion" | curl -s -X POST http://localhost:8123 --data-binary @-
```

### Testing Configuration

```python
# Test ClickHouseSink with environment variables
from champion.warehouse.adapters import ClickHouseSink

sink = ClickHouseSink()  # Automatically reads from .env
sink.connect()
print(f"Connected to {sink.host}:{sink.port}/{sink.database}")
```

### Testing API Configuration

```python
from champion.api.config import get_api_settings

settings = get_api_settings()
print(f"API ClickHouse Config:")
print(f"  Host: {settings.clickhouse_host}")
print(f"  Port: {settings.clickhouse_port}")
print(f"  Database: {settings.clickhouse_database}")
```

## Verification

All components now correctly:

1. ✅ Read from `.env` file using `CHAMPION_CLICKHOUSE_*` prefix
2. ✅ Have sensible defaults matching ClickHouse standard configuration
3. ✅ Allow parameter overrides when needed
4. ✅ Use port 8123 (HTTP interface) by default
5. ✅ Support both empty and explicit passwords

## Benefits

1. **Consistency**: Single source of truth for all ClickHouse configuration
2. **Flexibility**: Easy to change configuration for different environments
3. **Security**: Passwords and sensitive info stay in `.env` (not committed to git)
4. **Defaults**: Sensible defaults work out-of-the-box
5. **Override**: Can still pass explicit parameters when needed

## Testing Results

```bash
# API Health Check
$ curl http://localhost:8000/health
{"status":"healthy","service":"champion-api","version":"1.0.0"}

# ClickHouse Version Check
$ poetry run python -c "from champion.warehouse.adapters import ClickHouseSink; s=ClickHouseSink(); s.connect(); print(s.client.query('SELECT version()').result_rows[0][0])"
25.12.3.21
```

## Migration Notes

If you have existing code using the old `CLICKHOUSE_*` prefix (without `CHAMPION_`), update them to use `CHAMPION_CLICKHOUSE_*` for consistency.

**Old (deprecated):**

```bash
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
```

**New (current):**

```bash
CHAMPION_CLICKHOUSE_HOST=localhost
CHAMPION_CLICKHOUSE_PORT=8123
```
