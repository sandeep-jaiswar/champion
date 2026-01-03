# Compute Strategy (Streaming + Batch)

## Goals

- Use the right engine per latency/cost profile while keeping outputs deterministic and replayable.
- Ensure streaming and batch yield identical results given the same inputs.

## Streaming (Flink or Kafka Streams)

- **Use cases:** normalization, lightweight enrichments, near-RT features/signals, RT serving feeds.
- **Contracts:** consume from `raw.*`/`normalized.*`, produce to `normalized.*`/`analytics.*`.
- **Idempotency:** key by `entity_id`; use `event_id` for dedupe; avoid stateful joins without time bounds.
- **Watermarks:** set per topic; define lateness policy; emit metrics on out-of-order drops.
- **State:** checkpointed to durable store; version state schemas; include job version in metrics.

## Batch (Spark)

- **Use cases:** backfills, heavy transforms, feature generation, lake compaction, research exports.
- **Inputs:** Hudi Bronze/Silver; **Outputs:** Silver/Gold, ClickHouse ingest, feature exports.
- **Determinism:** pure functions over inputs; no time.now; parameterize with replay window and schema version.
- **Backfills:** rerun from earliest affected partition; write audit manifest (window, job hash, counts).

## Streaming vs Batch Decision Guide

- Latency < ~10s → streaming; latency minutes+ or full history → batch.
- If transformation requires full window recomputation (e.g., CA restatement) → batch backfill.
- If output must be exactly reproducible historically → ensure streaming logic mirrors batch and is replay-tested.

## Feature Computation Patterns

- Rolling windows: prefer batch for large windows; streaming for short/near-now with periodic reconciliation.
- Point-in-time correctness: store event_time and processing_time; avoid lookahead by enforcing watermarking.
- Versioned feature definitions: include `feature_version` in outputs; never overwrite prior definitions.

## Orchestration

- Use Airflow (or equivalent) to coordinate Spark jobs, backfills, and ClickHouse reloads.
- Promote jobs via TDRs; embed schema version and job commit hash in run metadata.

## Performance and Cost

- Autoscale streaming based on lag and CPU; autoscale batch clusters per job size.
- Optimize Spark shuffle (partition sizing) and Hudi write configs for compaction windows.
- Cache hot dimensions in memory-side stores if needed, but keep canonical values in the lake.

## Observability for Compute

- Metrics: throughput, lag, checkpoint age, failed records, DQ failures, watermark delay.
- Logs: structured with `event_id` and `entity_id` for joinability.
- Traces: critical paths from ingest through serve; include job version in trace attributes.
