# Getting Started with Champion

Welcome to Champion - a production-grade stock market intelligence platform. This section helps you get up and running quickly.

## Quick Navigation

| Document | Time | For Whom |
|----------|------|----------|
| **[Installation & Setup](installation.md)** | 15 min | Everyone starting out |
| **[Quick Start Guide](quick_start.md)** | 20 min | First-time users |
| **[Architecture Overview](architecture_overview.md)** | 30 min | Understanding the system |
| **[Troubleshooting Guide](troubleshooting.md)** | 10 min | When things go wrong |

## What is Champion?

Champion is a comprehensive stock market analytics platform that:

- **Ingests** real-time and historical market data from NSE/BSE
- **Processes** data through Polars (50-100x faster than Pandas)
- **Stores** efficiently in Parquet + ClickHouse
- **Orchestrates** pipelines with Prefect
- **Analyzes** with feature engineering and ML
- **Monitors** performance with MLflow + Prometheus

## Architecture at a Glance

```
NSE/BSE APIs
    ↓
[Prefect Orchestration] → Automatic scheduling
    ↓
[Ingestion] → httpx with retry logic
    ↓
[Processing] → Polars (50-100x faster)
    ↓
[Storage] → Parquet Lake + ClickHouse
    ↓
[Analytics] → MLflow + Feature Store
```

## Technology Stack

- **Language**: Python 3.12
- **Data Processing**: Polars
- **Orchestration**: Prefect 2.14+
- **Warehouse**: ClickHouse
- **Storage**: Parquet + Partitioned
- **Streaming**: Kafka
- **Metrics**: MLflow + Prometheus
- **Infrastructure**: Docker + Compose

## System Requirements

- Python 3.12+
- Docker & Docker Compose
- 4GB+ RAM
- 20GB+ disk space

## Installation (Quick Version)

```bash
# 1. Clone repository
git clone <repo>
cd champion

# 2. Install dependencies
poetry install

# 3. Start infrastructure
docker-compose up -d

# 4. Verify setup
poetry run python -c "import champion; print('✅ Ready')"
```

**Full instructions:** See [Installation Guide](installation.md)

## Your First ETL Run

```bash
# 1. Start Prefect Server
prefect server start &

# 2. Trigger a flow
prefect deployment run 'nse-bhavcopy-etl/nse-bhavcopy-daily'

# 3. Monitor in UI
# Open http://localhost:4200
```

## Project Layout

```
champion/
├── src/champion/           # Production code
├── docs/                   # This documentation
├── tests/                  # Test suite
├── scripts/                # Operational scripts
├── schemas/                # Data schemas
└── config/                 # Configuration
```

## Common Tasks

### Run Data Pipeline

→ See [User Guide: Running ETL](../03_user_guides/running_etl_pipelines.md)

### Query Data Warehouse

→ See [User Guide: Querying Warehouse](../03_user_guides/querying_warehouse.md)

### Add New Data Source

→ See [Development: Adding Components](../04_development/adding_new_components.md)

### Understand Architecture

→ See [Architecture Overview](architecture_overview.md)

## Key Concepts

- **Bronze Layer**: Raw data as received
- **Silver Layer**: Cleaned and validated
- **Gold Layer**: Ready for analytics
- **Prefect Flows**: Automated pipelines
- **Feature Store**: Computed analytics features
- **Event Streaming**: Real-time data via Kafka

## Support & Resources

- **Issues**: See [Troubleshooting](troubleshooting.md)
- **Architecture**: See [Architecture Guide](../01_architecture/)
- **Development**: See [Developer Guide](../04_development/)
- **API Reference**: See [API Docs](../05_api_reference/)

## Next Steps

1. **Follow** the [Installation Guide](installation.md) (15 min)
2. **Run** the [Quick Start](quick_start.md) (20 min)
3. **Explore** [Architecture](architecture_overview.md) (30 min)
4. **Join** development - see [Contributing](../../CONTRIBUTING.md)

---

**Ready?** Start with [Installation](installation.md) →
