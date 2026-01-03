# Observability and Data Quality

## Goals

- Provide end-to-end visibility across ingest, normalize, derive, store, and serve.
- Detect regressions early, isolate faults by domain, and enable confident replay/backfill.

## Pillars

- **Metrics:** Throughput, lag, error rates, DQ failures, watermark delay, replay/backfill outcomes.
- **Traces:** Critical request paths (ingest → normalize → serve) and batch/stream jobs with version tags.
- **Logs:** Structured, entity-keyed (`entity_id`, `event_id`), with reason codes for drops/quarantine.

## Data Quality Checks

- Schema conformance: enforced via registry; rejects to quarantine with reasons.
- Null/validity: per-field null/regex/range checks; symbol/ISIN validity.
- Time sanity: event_time <= now + skew; ingest_time present; watermark drift alerts.
- Duplicate detection: `event_id` uniqueness per topic/table; high-duplication alerts.
- Distribution drift: percentile/mean/std monitoring on key metrics and features.

## Quarantine and Replay

- Dedicated quarantine topics/tables per domain with reason codes and payload.
- Replay jobs consume quarantine after fixes; all replays recorded with window and job version.

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
