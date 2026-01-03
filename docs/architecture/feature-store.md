# Feature Store Strategy

## Objectives

- Deliver deterministic, explainable features with parity between offline (batch) and online (RT) paths.
- Make every feature versioned, reproducible, and auditable.

## Architecture

- **Offline store:** Hudi Gold tables hold historical feature values; rebuilt from Bronze/Silver.
- **Online store (optional):** Redis/Pinot/ClickHouse RT for low-latency serving; populated from `analytics.*`.
- **Registry:** Feature definitions (name, owner, version, transformation, source topics/tables, freshness SLA).
- **Contracts:** Feature outputs include `feature_name`, `feature_version`, `entity_id`, `event_time`, `value`, `source_version`.

## Parity and Determinism

- Batch recomputation from Bronze is the source of truth; streaming outputs must match batch on overlap windows.
- Periodic reconciliation jobs compare RT vs batch for drift and null-rate changes.

## Versioning

- Every semantic change bumps `feature_version`; never overwrite previous versions.
- Store metadata: schema version, job hash, transformation config, and backfill window.

## Serving Patterns

- Point lookups (online): key by `entity_id`; fetch latest by `event_time` <= request_time.
- Time-travel (offline): query Hudi Gold with `event_time` predicates; ensure PIT correctness for backtests.
- Aggregated features: publish both raw components and aggregates for auditability.

## Freshness and SLAs

- Define freshness targets per feature; alert on lag (event_time vs now) and staleness in caches.
- Backfill strategy documented per feature family; rerun from earliest affected partition on schema/model change.

## Data Quality and Governance

- DQ checks per feature: null rates, distribution shifts, monotonicity where expected.
- Lineage: capture upstream topics/tables and transformation IDs; emit lineage events.
- Access: domain-scoped ACLs; sensitive features flagged and access-controlled.
