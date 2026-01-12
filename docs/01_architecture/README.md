# Architecture Documentation

This section documents the design principles, architecture decisions, and system structure of the Champion platform.

## Documentation Map

| Document | Focus | Audience |
|----------|-------|----------|
| [Vision & Principles](vision_and_principles.md) | Why we built this way | Everyone |
| [Data Flow](data_flow.md) | How data moves through system | Architects |
| [Domain Boundaries](domain_boundaries.md) | Service decomposition | Architects |
| [Polyglot Architecture](polyglot_architecture.md) | Technology choices | Tech leads |
| [Storage Strategy](storage_strategy.md) | Lake vs Warehouse | Data engineers |
| [Compute Strategy](compute_strategy.md) | Processing pipeline | Data engineers |
| [Feature Store](feature_store.md) | Feature engineering | Data scientists |
| [Security & Governance](security_and_governance.md) | Data protection | Security team |
| [Observability](observability.md) | Monitoring & logging | DevOps/SRE |
| [Data Platform (Detailed)](data_platform_detailed.md) | Complete platform overview | Technical leads |

## Quick Reference

### Architecture Patterns

```
Data Ingestion (NSE/BSE APIs)
    ↓
Scraping Layer (httpx + retry logic)
    ↓
Parsing Layer (Polars - high performance)
    ↓
Normalization Layer (Validation + enrichment)
    ↓
Storage Layer (Parquet Data Lake)
    ↓
Warehouse Layer (ClickHouse OLAP)
    ↓
Analytics Layer (MLflow + Feature Store)
```

### Key Architectural Decisions

| Decision | Rationale | Document |
|----------|-----------|----------|
| Polars over Pandas | 50-100x performance, Arrow-backed | [ADR-001](../07_decisions/001_polars_over_pandas.md) |
| Parquet for storage | Columnar, efficient, partitionable | [ADR-002](../07_decisions/002_parquet_storage.md) |
| ClickHouse warehouse | OLAP, fast aggregations, compression | [ADR-003](../07_decisions/003_clickhouse_warehouse.md) |
| Prefect orchestration | Flexible, observable, production-ready | [ADR-004](../07_decisions/004_prefect_orchestration.md) |
| Schema evolution | Versioning, backward compatibility | [ADR-005](../07_decisions/005_schema_versioning.md) |

### Core Principles

1. **High Performance** - Polars, Parquet, ClickHouse
2. **Observability** - Every task metrics, structured logging
3. **Reliability** - Retry logic, data validation, audit trails
4. **Scalability** - Partitioned storage, horizontal scale
5. **Maintainability** - Clear domains, documented decisions

## Domains

- **Ingestion**: Data collection from external sources
- **Storage**: Efficient data persistence (Parquet + ClickHouse)
- **Features**: Computed analytics and indicators
- **Orchestration**: Workflow scheduling with Prefect
- **ML**: Experiment tracking with MLflow

## Technology Stack

- **Data Processing**: Polars (Python)
- **Storage**: Parquet files + ClickHouse
- **Streaming**: Kafka with Avro schemas
- **Orchestration**: Prefect 2.14+
- **Monitoring**: MLflow + Prometheus
- **Infrastructure**: Docker Compose

## Navigation

- **New to Champion?** → Start with [Vision & Principles](vision_and_principles.md)
- **Designing a feature?** → See [Domain Boundaries](domain_boundaries.md)
- **Choosing tech?** → See [Polyglot Architecture](polyglot_architecture.md)
- **Debugging issues?** → See [Observability](observability.md)

---

**Want to understand system design in detail?** Read [Data Flow](data_flow.md)
