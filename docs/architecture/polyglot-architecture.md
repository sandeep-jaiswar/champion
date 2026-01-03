# Polyglot Architecture Blueprint

## Goals

- Preserve raw truth, enable full replay, and guarantee financial correctness.
- Support both low-latency analytics and heavy batch research without coupling.
- Let each domain choose the right store and compute model while enforcing contracts.

## Core Principles

- Log-centric: Kafka is the system of movement; storage systems are sinks.
- Schema-first: Avro contracts define topics; code aligns to schemas.
- Immutability: Corrections are new events; no in-place mutation downstream.
- Determinism: Streaming and batch produce the same outputs for identical inputs.
- Clear ownership: Each domain owns its topics, schemas, and SLAs.

## Technology Stack (Uber-inspired)

- **Event backbone:** Kafka with `raw.*`, `normalized.*`, `analytics.*` topics; Avro schemas in repo.
- **Lakehouse:** Hudi Bronze/Silver/Gold on object storage (replayable, vacuumed via compaction).
- **OLAP serving:** ClickHouse for API/reporting; Pinot optional for live dashboards.
- **Cache:** Redis or CDN for hot responses; API-layer caching encouraged.
- **Compute:** Flink (or Kafka Streams) for streaming; Spark for batch/backfills; Airflow for orchestration.
- **Feature serving:** Offline (Hudi/ClickHouse) and optional online (Redis/Pinot) with versioned definitions.
- **Artifacts:** Object storage for models, TDRs, and replay snapshots.

## Domain-to-Store Mapping

- Ingestion → Kafka `raw.*`; Bronze Hudi tables (immutable, source-aligned).
- Normalization → Kafka `normalized.*`; Silver Hudi tables (standardized, CA-adjusted).
- Analytics → Kafka `analytics.*`; Gold Hudi tables (aggregates, features); ClickHouse materializations.
- Intelligence → Model outputs to `analytics.*` or serving stores; artifacts to object storage.
- Serving → ClickHouse + cache; never mutates upstream.

## Partitioning and Keys

- Kafka key: `entity_id` (symbol + exchange or instrument_id) to preserve per-entity ordering.
- Hudi primary key: `event_id` (or deterministic composite) with `event_time`/trade_date partitions.
- ClickHouse order by: `entity_id`, `event_date`, and scenario-specific sorting keys.

## SLA and Latency Tiers

- Real-time: sub-second to few seconds E2E for ingest → normalize → serve (Pinot/ClickHouse RT).
- Near-real-time: tens of seconds to minutes for feature updates.
- Batch: hourly/daily backfills and heavyweight research pipelines.

## Cost and Correctness Controls

- Compaction and clustering policies on Hudi to balance cost and query latency.
- TTL/retention per topic class; avoid topic deletion—use versions.
- DQ gates at domain boundaries; fail fast on contract violations.
- Backfill playbooks and deterministic replay from Bronze.

## Observability Hooks

- Envelope carries ingest vs event time; used for lag SLOs and out-of-order detection.
- Metrics per domain: throughput, lag, DQ failures, replay success, SLA compliance.
- Traces for critical paths (ingest → normalize → serve) and backfill jobs.

## Compliance and Access

- Domain-scoped ACLs on topics and tables; producers/consumers limited to owned domains.
- Secrets via vault/managed KMS; encryption in transit and at rest.
- Audit logs for access to Kafka, Hudi, ClickHouse, and artifacts.
