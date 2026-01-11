# Observability and Data Quality

## Goals

- Provide end-to-end visibility across ingest, normalize, derive, store, and serve.
- Detect regressions early, isolate faults by domain, and enable confident replay/backfill.

This document complements [compute-strategy.md](./compute-strategy.md) (deterministic outputs and watermark policies), [security.md](./security.md) (audit logging), and [data-flow.md](./data-flow.md) (end-to-end visibility).

## Pillars

- **Metrics:** Throughput, lag, error rates, DQ failures, watermark delay (the lag between the watermark and current wall-clock time), checkpoint age (time since last successful streaming checkpoint), replay/backfill outcomes.
- **Traces:** Critical request paths (ingest → normalize → serve) and batch/stream jobs with version tags.
- **Logs:** Structured, entity-keyed (`entity_id`, `event_id`), with reason codes for drops/quarantine.

For more details on streaming concepts, see [compute-strategy.md](./compute-strategy.md).

## Data Quality Checks

- Schema conformance: enforced via registry; rejects to quarantine with reasons.
- Null/validity: per-field null/regex/range checks; symbol/ISIN validity.
- Time sanity: event_time <= now + skew; ingest_time present; watermark drift alerts.
- Duplicate detection: `event_id` uniqueness per topic/table; high-duplication alerts.
- Distribution drift: percentile/mean/std monitoring on key metrics and features.

## Quarantine and Replay

- Dedicated quarantine topics/tables per domain with reason codes and payload.
- Replay jobs consume quarantine after fixes; all replays recorded with window and job version.
- **Workflow:** Fixes validated in staging; replay jobs are idempotent (using deterministic job IDs) and track replay attempts per window.
- **SLA:** Quarantine items addressed within defined domain-specific timelines; escalation if backlog exceeds threshold.
- **Idempotency:** Replay jobs use deterministic job IDs and skip already-processed windows to ensure safe reruns.

See [compute-strategy.md](./compute-strategy.md) for deterministic compute patterns and [security.md](./security.md) for replay audit requirements.

## SLOs and Alerting

- RT path: ingest-to-serve latency, freshness, and error budgets per domain.
- Batch path: completion time, backfill success rate, row-count/parity checks.
- DQ SLOs: null-rate, schema violation rate, drift thresholds; page teams on sustained breaches.

## Dashboards (suggested)

- Kafka: lag, partition skew, DLQ volume.
- Hudi: write latency, compaction backlog, small-file counts, upsert conflicts.
- Compute: checkpoint age, job lag, failure counts, replay duration.
- Serving: ClickHouse/Pinot p50/p95/p99, QPS, cache hit rate, error rate.
- Features: freshness, RT vs batch parity, drift metrics.

## Audit and Lineage

- Envelope fields plus job metadata emitted to a lineage sink.
- Access logs for Kafka/Hudi/ClickHouse/Pinot; periodic reviews per domain.
- Backfill manifests stored with checksums and row counts for reproducibility.
