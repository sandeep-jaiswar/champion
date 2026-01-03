# Data Flow (End-to-End)

## Overview

This doc details the movement of data across domains: ingestion → Kafka → lakehouse → OLAP/serving. It complements the system overview with latency-specific swimlanes.

## Real-Time Path (Low Latency)

```text
Exchange/API → Ingestion (stateless) → Kafka raw.* → Stream Normalize → Kafka normalized.*
    → Stream Derive → Kafka analytics.* → ClickHouse/Pinot RT → API/Cache → Clients
```

- Ingestion: idempotent writers to `raw.*`, envelope stamped with `event_time` and `ingest_time`.
- Stream Normalize: symbol mapping, CA adjustments, calendar alignment; deterministic, schema-validated.
- Stream Derive: lightweight features/signals; must be replayable.
- Serving: ClickHouse/Pinot RT nodes power low-latency queries; cache fronts APIs.

## Batch Path (Throughput, Backfills, Research)

```text
Kafka raw.* → Hudi Bronze (append) → Spark Normalize → Hudi Silver
    → Spark Derive → Hudi Gold → ClickHouse batch ingest / Feature Store → API/BI/Research
```

- Bronze: exact source truth; no corrections.
- Silver: standardized, CA-adjusted, late-arrival tolerant via upserts with deterministic keys.
- Gold: aggregates, factor datasets, feature tables; fully recomputable from Bronze.
- Research/Backtest: query Hudi Gold or snapshot exports; never mutate downstream.

## Replay and Backfill Playbook

- Trigger from earliest affected partition in Bronze.
- Rebuild Silver with deterministic transforms; verify counts and checksums vs expected.
- Regenerate Gold and refresh ClickHouse/feature stores.
- Record backfill metadata (window, reason, producer hash, schema version).

## Event Contracts and Validation

- Envelope required: `event_id`, `event_time`, `ingest_time`, `source`, `schema_version`, `entity_id`, `payload`.
- Validation: schema registry + DQ checks per domain (nullability, ranges, monotonic time where applicable).
- Rejections: routed to quarantine topics/tables with reason codes.

## Latency and Ordering

- Ordering: per-`entity_id` ordering guaranteed by Kafka key; processors must preserve key affinity.
- Watermarks: set per topic to manage late data; backfills must respect watermark windows or disable temporarily.
- SLAs: RT path seconds; batch path minutes-hours depending on window size and compaction.

## Failure Isolation

- Domain boundaries absorb faults: ingestion issues do not break serving; normalization lag does not halt analytics.
- Kafka buffers bursts; Hudi enables recompute; ClickHouse/Pinot isolated for query spikes with autoscale or replicas.
