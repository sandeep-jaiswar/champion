# Champion REST API

REST API for accessing Champion platform data including OHLC data, technical indicators, corporate actions, and index information.

## Features

- **OHLC Data**: Historical and latest price data
- **Corporate Actions**: Stock splits, dividends, and other corporate events
- **Technical Indicators**: SMA, RSI, EMA with dynamic calculation
- **Index Data**: Index constituents and historical changes
- **Authentication**: JWT-based authentication
- **Rate Limiting**: 60 requests per minute per IP
- **Caching**: Redis-based caching with 5-minute TTL
- **API Documentation**: Auto-generated OpenAPI/Swagger docs

## Quick Start

### Installation

The API dependencies are included in the project's `pyproject.toml`. Install them with:

```bash
poetry install
```

### Starting the Server

Using the CLI:

```bash
champion api serve
```

With custom options:

```bash
champion api serve --port 8080
champion api serve --reload  # For development with auto-reload
champion api serve --workers 4  # For production with multiple workers
```

### Accessing the API

- API Base URL: `http://localhost:8000`
- Interactive Docs: `http://localhost:8000/docs`
- Alternative Docs: `http://localhost:8000/redoc`
- OpenAPI Schema: `http://localhost:8000/openapi.json`

## Authentication

Most endpoints require authentication. Get a JWT token:

```bash
curl -X POST "http://localhost:8000/api/v1/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=demo&password=demo123"
```

Use the token in subsequent requests:

```bash
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" \
  "http://localhost:8000/api/v1/ohlc?symbol=INFY"
```

Demo credentials:

- Username: `demo`
- Password: `demo123`

## API Endpoints

### OHLC Data

```bash
# Get OHLC data with date range
GET /api/v1/ohlc?symbol=INFY&from=2024-01-01&to=2024-12-31

# Get latest OHLC data
GET /api/v1/ohlc/INFY/latest

# Get candle data for charting
GET /api/v1/ohlc/INFY/candles?interval=1d
```

### Corporate Actions

```bash
# Get all corporate actions
GET /api/v1/corporate-actions?symbol=INFY

# Get stock splits
GET /api/v1/corporate-actions/INFY/splits

# Get dividends
GET /api/v1/corporate-actions/INFY/dividends
```

### Technical Indicators

```bash
# Get Simple Moving Average
GET /api/v1/indicators/INFY/sma?period=20

# Get Relative Strength Index
GET /api/v1/indicators/INFY/rsi?period=14

# Get Exponential Moving Average
GET /api/v1/indicators/INFY/ema?period=12
```

### Index Data

```bash
# List all indices
GET /api/v1/indices

# Get index constituents
GET /api/v1/indices/NIFTY50/constituents

# Get index changes
GET /api/v1/indices/NIFTY50/changes
```

## Configuration

Configure the API using environment variables with the `CHAMPION_API_` prefix:

```bash
# Server
export CHAMPION_API_HOST=0.0.0.0
export CHAMPION_API_PORT=8000

# Database
export CHAMPION_API_CLICKHOUSE_HOST=localhost
export CHAMPION_API_CLICKHOUSE_PORT=8123

# Redis
export CHAMPION_API_REDIS_HOST=localhost
export CHAMPION_API_REDIS_PORT=6379
export CHAMPION_API_CACHE_TTL=300

# Security
export CHAMPION_API_JWT_SECRET_KEY=your-secret-key
export CHAMPION_API_RATE_LIMIT_PER_MINUTE=60

# CORS
export CHAMPION_API_CORS_ORIGINS='["http://localhost:3000"]'
```

## Pagination

All list endpoints support pagination:

```bash
GET /api/v1/ohlc?symbol=INFY&page=1&page_size=100
```

Parameters:

- `page`: Page number (default: 1)
- `page_size`: Items per page (default: 100, max: 1000)

## Rate Limiting

The API implements rate limiting using Redis:

- Default: 60 requests per minute per IP address
- Returns HTTP 429 (Too Many Requests) when exceeded

## Caching

GET requests are cached in Redis for 5 minutes by default:

- Reduces database load
- Improves response times
- Automatically invalidates after TTL

## Error Handling

The API returns standard HTTP status codes:

- `200 OK`: Success
- `400 Bad Request`: Invalid parameters
- `401 Unauthorized`: Missing or invalid authentication
- `404 Not Found`: Resource not found
- `429 Too Many Requests`: Rate limit exceeded
- `500 Internal Server Error`: Server error

Error response format:

```json
{
  "error": "Error message",
  "detail": "Detailed error information",
  "timestamp": "2024-01-01T00:00:00"
}
```

## Development

### Running Tests

```bash
pytest tests/unit/test_api.py -v
```

### API Documentation

The API documentation is auto-generated from the code using FastAPI's built-in OpenAPI support. Access it at:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Adding New Endpoints

1. Create a new router in `src/champion/api/routers/`
2. Define schemas in `src/champion/api/schemas/`
3. Add the router to `src/champion/api/main.py`
4. Add tests in `tests/unit/test_api.py`

## Performance

The API is designed for high performance:

- **Target Latency**: <100ms p99
- **Target Throughput**: 1000 req/sec
- **Caching**: Redis-based response caching
- **Database**: ClickHouse for fast analytical queries
- **Connection Pooling**: Reuses database connections
- **Async Support**: Built on Starlette/ASGI

## Production Deployment

### Using Uvicorn

```bash
uvicorn champion.api.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 4 \
  --log-level info
```

### Using Docker

```bash
docker build -t champion-api .
docker run -p 8000:8000 champion-api
```

### Requirements

- Python 3.11+
- ClickHouse (for data storage)
- Redis (for caching and rate limiting)

## Security Considerations

1. **Change the JWT secret** in production
2. **Use HTTPS** in production
3. **Configure CORS** appropriately
4. **Implement proper authentication** (replace demo user)
5. **Use environment variables** for sensitive configuration
6. **Enable rate limiting** to prevent abuse

## Troubleshooting

### Redis Connection Errors

If Redis is not available, the API will:

- Disable caching (fail open)
- Continue to function without rate limiting

### ClickHouse Connection Errors

If ClickHouse is not available, endpoints will return:

- HTTP 500 errors with descriptive messages
- Placeholder data for some endpoints (indices, corporate actions)

### Missing Data

Some endpoints (corporate actions, indices) return empty results if the underlying tables are not yet populated. This is expected behavior and will resolve once the ETL pipelines populate the data.
