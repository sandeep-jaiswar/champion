# Storage Strategy (Polyglot)

## Objectives

- Preserve raw truth, enable replay, and serve low-latency analytics without compromising financial correctness.
- Apply the right store per access pattern: lake for durability/replay, OLAP for serve, cache for hot paths.

## Hudi Lakehouse

- **Layers:**
  - Bronze: append-only, source-aligned, no corrections.
  - Silver: standardized/CA-adjusted, deterministic upserts, late-data tolerant.
  - Gold: aggregates, feature tables, curated research datasets.
- **Partitioning:** by trade/market date (and optionally exchange) to balance pruning vs small-file risk.
- **Primary keys:** `event_id` (or deterministic composite) to dedupe/idempotently upsert.
- **Compaction/Clustering:** scheduled based on volume; align with SLAs to avoid query stalls.
- **Retention:** Bronze long-lived; Silver/Gold shorter with ability to rebuild; snapshots for audits.
- **DQ and Quarantine:** failed records land in side tables with reason codes; replayable once fixed.

## ClickHouse (Serving OLAP)

- **Use:** API/query serving for dashboards and client requests.
- **Schema:** columnar, minimize memory/storage bloat from low-cardinality strings by using dictionary encoding and appropriate codecs.
- **Order/Partition keys:** `entity_id`, `event_date` (or bucketed by date), plus scenario-specific sort keys.
- **Materialized views:** for rollups (e.g., OHLC aggregates, factor windows); refresh from Hudi or Kafka.
- **TTL:** set per table; consider moving cold segments back to lake.
- **Concurrency:** size replicas for bursty read; isolate from heavy backfills via dedicated clusters or queues.

## Pinot (Optional RT Serve)

- **When:** sub-second freshness for live dashboards/alerts.
- **Ingest:** directly from Kafka `analytics.*` or normalized topics; batch ingest from Hudi for historical fill.
- **Schema discipline:** same Avro contracts; ensure upsert keys match Kafka keys.

## Cache Layer

- **When:** hot keys, tight p99, or expensive OLAP queries.
- **Store:** Redis/KeyDB or CDN for static assets.
- **Contract:** cache entries tagged with schema version and TTL; purged on backfill or schema bump.

## Artifacts and Lineage

- Model artifacts, Technical Design Reviews (TDRs), and replay manifests stored in object storage with versioned paths.
- Lineage tracked via envelope fields plus job metadata; emit lineage events to governance tooling.

## Access Control and Compliance

- Domain-scoped ACLs on Hudi tables, ClickHouse, Pinot, and caches.
- Encryption at rest (KMS) and in transit (TLS); secrets via managed vault.
- Audit logs for read/write paths; periodic reviews tied to domains.
