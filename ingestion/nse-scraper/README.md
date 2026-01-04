# NSE Data Scraper

Production-grade NSE data ingestion service with Kafka integration, schema registry, and comprehensive error handling.

## Architecture

```text
NSE Website/FTP
      ↓
[Scrapers] → [Parsers] → [Kafka Producer] → Kafka Topics
                ↓
         [Schema Registry]
                ↓
         [Observability]
```

## Features

- **Modular Scrapers**: Separate scrapers for OHLC, Symbol Master, Corporate Actions, Trading Calendar
- **Schema-First**: Avro schema validation via Confluent Schema Registry
- **Idempotent**: Prevents duplicate ingestion with event_id tracking
- **Resilient**: Retry logic, circuit breakers, dead-letter queues
- **Observable**: Prometheus metrics, structured logging, distributed tracing
- **Configurable**: Environment-based configuration with validation

## Project Structure

```text
nse-scraper/
├── README.md
├── pyproject.toml              # Poetry dependency management
├── .env.example                # Environment variables template
├── Dockerfile                  # Production container image
├── docker-compose.yml          # Local development setup
├── src/
│   ├── __init__.py
│   ├── config.py               # Configuration management
│   ├── main.py                 # Application entrypoint
│   ├── scrapers/               # Data source scrapers
│   │   ├── __init__.py
│   │   ├── base.py             # Base scraper class
│   │   ├── bhavcopy.py         # NSE CM bhavcopy scraper
│   │   ├── symbol_master.py    # EQUITY_L scraper
│   │   ├── corporate_actions.py # CA scraper
│   │   └── trading_calendar.py # Calendar scraper
│   ├── parsers/                # Data parsers
│   │   ├── __init__.py
│   │   ├── base.py             # Base parser
│   │   ├── bhavcopy_parser.py  # Parse CM bhavcopy CSV
│   │   ├── symbol_parser.py    # Parse EQUITY_L CSV
│   │   └── ca_parser.py        # Parse CA CSV with PURPOSE extraction
│   ├── producers/              # Kafka producers
│   │   ├── __init__.py
│   │   ├── base.py             # Base Kafka producer
│   │   ├── avro_producer.py    # Avro-serializing producer
│   │   └── batch_producer.py   # Batch producer for bulk loads
│   ├── models/                 # Data models
│   │   ├── __init__.py
│   │   ├── events.py           # Event envelope models
│   │   └── schemas.py          # Avro schema loaders
│   └── utils/                  # Utilities
│       ├── __init__.py
│       ├── logger.py           # Structured logging
│       ├── metrics.py          # Prometheus metrics
│       ├── retry.py            # Retry decorators
│       └── validators.py       # Data validation
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # Pytest fixtures
│   ├── unit/                   # Unit tests
│   │   ├── test_parsers.py
│   │   └── test_validators.py
│   └── integration/            # Integration tests
│       └── test_kafka.py
└── scripts/
    ├── download_sample_data.sh # Download NSE sample files
    └── validate_schemas.py     # Validate Avro schemas
```

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Access to NSE data sources
- Kafka cluster with Schema Registry

### Installation

```bash
# Clone repository
cd ingestion/nse-scraper

# Install dependencies
poetry install

# Copy environment template
cp .env.example .env
# Edit .env with your configuration

# Run locally
poetry run python -m src.main
```

### Development with Docker

```bash
# Start local Kafka + Schema Registry + Zookeeper
docker-compose up -d

# Run scraper
poetry run python -m src.main --scraper bhavcopy --date 2026-01-02

# View logs
docker-compose logs -f nse-scraper

# Stop services
docker-compose down
```

## Configuration

### Environment Variables

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=
KAFKA_SASL_PASSWORD=

# Kafka Topics
TOPIC_RAW_OHLC=raw.market.equity.ohlc
TOPIC_SYMBOL_MASTER=reference.nse.symbol_master
TOPIC_CORPORATE_ACTIONS=reference.nse.corporate_actions
TOPIC_TRADING_CALENDAR=reference.nse.trading_calendar

# NSE Data Sources
NSE_BHAVCOPY_URL=https://archives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv
NSE_EQUITY_LIST_URL=https://archives.nseindia.com/content/equities/EQUITY_L.csv
NSE_CA_URL=https://www.nseindia.com/api/corporates-corporateActions

# Scraper Configuration
SCRAPER_SCHEDULE_BHAVCOPY=0 18 * * 1-5  # 6 PM on weekdays
SCRAPER_SCHEDULE_SYMBOL_MASTER=0 1 * * *  # 1 AM daily
SCRAPER_SCHEDULE_CA=0 2 * * *  # 2 AM daily
SCRAPER_RETRY_ATTEMPTS=3
SCRAPER_RETRY_DELAY=60
SCRAPER_TIMEOUT=300

# Storage (for downloaded files)
DATA_DIR=/data/nse
ARCHIVE_RETENTION_DAYS=30

# Observability
LOG_LEVEL=INFO
LOG_FORMAT=json
METRICS_PORT=9090
TRACING_ENABLED=false
JAEGER_AGENT_HOST=localhost
JAEGER_AGENT_PORT=6831

# Monitoring
ALERT_WEBHOOK_URL=
SLACK_WEBHOOK_URL=
```

## Usage

### CLI Commands

```bash
# Scrape specific date
poetry run python -m src.main scrape bhavcopy --date 2026-01-02

# Backfill date range
poetry run python -m src.main backfill bhavcopy --start 2025-01-01 --end 2026-01-01

# Scrape all reference data
poetry run python -m src.main scrape reference-data

# Validate downloaded file
poetry run python -m src.main validate --file /data/nse/BhavCopy_NSE_CM_0_0_0_20260102_F_0000.csv

# Dry run (parse without producing to Kafka)
poetry run python -m src.main scrape bhavcopy --date 2026-01-02 --dry-run
```

### Scheduled Execution (Airflow DAG)

Airflow DAG orchestration is planned for future implementation.

```python
# DAG file will be added in a future release
# See ../airflow/dags/nse_ingestion_dag.py (coming soon)
```

## Data Quality

### Validation Rules

**Bhavcopy**:

- File must have 34 columns
- TradDt must match requested date
- TckrSymb must be non-empty
- Numeric fields must parse correctly
- Reject rows with all-null payload

**Symbol Master**:

- ISIN format: 12-character alphanumeric
- MARKET LOT must be positive integer
- DATE OF LISTING must be valid date

**Corporate Actions**:

- EX-DATE must be valid date
- PURPOSE must be non-empty
- Parse PURPOSE to extract action type

### Error Handling

1. **Network Errors**: Exponential backoff retry (3 attempts)
2. **Parse Errors**: Log + quarantine to DLQ topic
3. **Schema Validation Errors**: Log + alert + skip row
4. **Kafka Produce Errors**: Retry with backoff + alert

### Idempotency

- Generate deterministic `event_id` from (source, date, symbol)
- Kafka producer with `enable.idempotence=true`
- Downstream consumers deduplicate by `event_id`

## Monitoring

### Metrics (Prometheus)

```text
nse_scraper_files_downloaded_total{scraper="bhavcopy"}
nse_scraper_rows_parsed_total{scraper="bhavcopy", status="success"}
nse_scraper_rows_parsed_total{scraper="bhavcopy", status="failed"}
nse_scraper_kafka_produce_success_total{topic="raw.market.equity.ohlc"}
nse_scraper_kafka_produce_failed_total{topic="raw.market.equity.ohlc"}
nse_scraper_scrape_duration_seconds{scraper="bhavcopy"}
nse_scraper_last_successful_scrape_timestamp{scraper="bhavcopy"}
```

### Alerts

- Scrape failure > 3 consecutive times
- Parse error rate > 5%
- Kafka produce failure rate > 1%
- Scraper lag > 24 hours

## Development

### Running Tests

```bash
# Unit tests
poetry run pytest tests/unit -v

# Integration tests (requires Docker)
docker-compose up -d kafka schema-registry
poetry run pytest tests/integration -v

# Coverage report
poetry run pytest --cov=src --cov-report=html
open htmlcov/index.html
```

### Code Quality

```bash
# Format code
poetry run black src/ tests/

# Lint
poetry run ruff check src/ tests/

# Type checking
poetry run mypy src/

# Pre-commit hooks
poetry run pre-commit install
poetry run pre-commit run --all-files
```

## Deployment

### Kubernetes (Production)

```bash
# Build image
docker build -t champion/nse-scraper:latest .

# Push to registry
docker push champion/nse-scraper:latest

# Deploy CronJob (Kubernetes manifests to be added)
# kubectl apply -f k8s/nse-scraper-cronjob.yaml

# Monitor logs
kubectl logs -f job/nse-scraper-bhavcopy-20260102
```

### Docker Compose (Staging)

```bash
# Production compose file (to be added in future release)
# docker-compose -f docker-compose.prod.yml up -d
```

## Troubleshooting

### Common Issues

**NSE Website Blocking**: Use rotating proxies or rate limiting
**ISIN Mismatch**: Cross-validate with symbol_master before producing
**Date Format Variations**: NSE uses DD-MMM-YYYY, normalize to YYYY-MM-DD
**Missing Files**: NSE archives may have delays, implement retry with backoff

### Debug Mode

```bash
export LOG_LEVEL=DEBUG
poetry run python -m src.main scrape bhavcopy --date 2026-01-02
```

## Next Steps

1. Implement rate limiting for NSE API
2. Add proxy rotation for scraping
3. Implement change data capture for symbol master updates
4. Add data quality dashboard (Great Expectations)
5. Integrate with Airflow for orchestration
6. Add schema evolution handling
