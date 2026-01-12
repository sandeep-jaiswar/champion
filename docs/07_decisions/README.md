# Architecture Decision Records (ADRs)

Decisions that shaped Champion's design. Read these to understand the "why" behind the architecture.

## Process

When making significant technical decisions:

1. Create ADR following template in [ADR Template](template.md)
2. Get team review and consensus
3. Archive in this directory
4. Reference in relevant documentation

## Key Decisions

- **[ADR-001: ClickHouse for Warehouse](adr-001-clickhouse-warehouse.md)** - Why OLAP instead of OLTP
- **[ADR-002: Polars for Parsing](adr-002-polars-parsing.md)** - Why not Pandas/DuckDB
- **[ADR-003: Prefect for Orchestration](adr-003-prefect-orchestration.md)** - Flow orchestration choice
- **[ADR-004: Parquet for Lake](adr-004-parquet-lake.md)** - Why Parquet over Delta/Iceberg
- **[ADR-005: Domain-Driven Design](adr-005-domain-driven-design.md)** - Code organization principle

## By Category

### Data Storage

- [ADR-001: ClickHouse](adr-001-clickhouse-warehouse.md)
- [ADR-004: Parquet Lake](adr-004-parquet-lake.md)

### Data Processing

- [ADR-002: Polars](adr-002-polars-parsing.md)

### Orchestration

- [ADR-003: Prefect](adr-003-prefect-orchestration.md)

### Architecture

- [ADR-005: DDD](adr-005-domain-driven-design.md)

---

**New ADRs?** Use the [template](template.md) and follow the process above.
