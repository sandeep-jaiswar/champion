# Domain Model & Bounded Contexts

This document defines the **core domains**, their **responsibilities**, **owned data**, and **interaction rules** for the Stock Market Analysis Platform.

The goal is to enforce **clear ownership**, enable **polyglot persistence**, and prevent tight coupling as the system scales.

---

## 1. Why Domain Boundaries Matter

In high-volume financial systems:

- Ambiguous ownership leads to data corruption
- Shared databases destroy scalability
- “Convenience joins” become systemic bottlenecks

Therefore, **each domain owns its data, schemas, pipelines, and SLAs**.

---

## 2. Top-Level Domain Map

```text
┌──────────────────────────┐
│      Ingestion Domain     │
└────────────┬─────────────┘
             │
┌────────────▼─────────────┐
│  Normalization Domain     │
└────────────┬─────────────┘
             │
┌────────────▼─────────────┐
│      Storage Domain       │
└────────────┬─────────────┘
             │
┌────────────▼─────────────┐
│     Analytics Domain      │
└────────────┬─────────────┘
             │
┌────────────▼─────────────┐
│    Intelligence Domain    │
└────────────┬─────────────┘
             │
┌────────────▼─────────────┐
│      Serving Domain       │
└──────────────────────────┘

        (Cross-cutting: Governance & Infra)
```

### Flow Guarantees

- Data flows **strictly downstream**
- Upstream domains are **immutable**
- No domain can require synchronous access to an upstream domain

---

## 3. Ingestion Domain

### Ingestion Purpose

Acquire external data **exactly as provided by the source**, without interpretation.

### Ingestion Responsibilities

- NSE scraping (prices, volumes, derivatives)
- Corporate announcements and PDFs
- API-based ingestion
- Capture of source metadata and delivery timestamps
- Source-level idempotency

### Ingestion Owns

- Raw schemas
- Kafka raw topics
- Source adapters

### Ingestion Explicitly Does NOT

- Normalize symbols
- Adjust prices
- Drop, correct, or infer data
- Join with reference data

### Ingestion Output

Immutable raw events (`raw.*`)

---

## 4. Normalization & Enrichment Domain

### Normalization Purpose

Convert raw events into **financially consistent, standardized representations**.

### Normalization Responsibilities

- Symbol / ISIN normalization
- Corporate action application
- Trading calendar alignment
- FX normalization (future)

### Normalization Owns

- Normalized schemas
- Enrichment and transformation logic
- Reference data caches

### Normalization Explicitly Does NOT

- Mutate raw data
- Act as a system of record
- Serve data directly to users

### Normalization Output

Deterministic, reproducible normalized events

---

## 5. Storage Domain

### Storage Purpose

Persist events into **durable, replayable, and query-optimized projections**.

### Storage Responsibilities

- Bronze / Silver / Gold data layers
- Partitioning, compaction, and retention
- Backfills and historical reprocessing

### Storage Owns

- Hudi table layouts
- ClickHouse schemas
- OLAP-optimized materializations

### Storage Explicitly Does NOT

- Interpret business meaning
- Compute analytics
- Act as a source of truth

> Storage is a **projection of events**, never a domain of meaning.

---

## 6. Analytics Domain

### Analytics Purpose

Transform stored data into **features, aggregates, and research-grade datasets**.

### Analytics Responsibilities

- Batch analytics
- Feature computation
- Research datasets
- Feature Store construction

### Analytics Owns

- Feature definitions
- Analytical pipelines
- Statistical correctness

### Analytics Explicitly Does NOT

- Execute trading logic
- Serve real-time APIs
- Mutate storage

---

## 7. Intelligence Domain

### Intelligence Purpose

Generate **signals, models, and anomaly detections** from analytics outputs.

### Intelligence Responsibilities

- Rule-based signals
- Statistical detection
- Machine learning models
- Model evaluation

### Intelligence Owns

- Signal definitions
- Model artifacts
- Evaluation metrics

### Intelligence Explicitly Does NOT

- Persist raw or normalized data
- Provide dashboards or APIs

---

## 8. Serving Domain

### Serving Purpose

Expose data and insights **safely, efficiently, and predictably**.

### Serving Responsibilities

- Read-optimized APIs
- Caching strategies
- Query orchestration
- Response SLAs

### Serving Owns

- API contracts
- Serving-layer schemas
- Performance guarantees

### Serving Explicitly Does NOT

- Compute analytics
- Train models
- Mutate upstream data

---

## 9. Governance (Cross-Cutting)

### Governance Responsibilities

- Data lineage
- Quality checks
- Reconciliation
- Auditability
- Compliance visibility

### Governance Authority

- Can **observe all domains**
- Can **block releases**
- Can **own no data**

---

## 10. Infrastructure (Cross-Cutting)

### Infrastructure Responsibilities

- Kafka
- Kubernetes
- CI/CD
- Secrets, configs, observability

Infrastructure provides **platform capabilities**. Domains provide **business value**.

---

## 11. Explicit Interaction Rules (Non-Negotiable)

1. Domains communicate **only via events or published contracts**.
2. No domain may read another domain’s database directly.
3. Storage is not a business-logic layer.
4. Analytics never mutate stored data.
5. Serving never recomputes analytics.

Violations require **explicit architectural approval** and documentation.

---

## 12. Evolution Strategy

- Domains may split internally over time.
- Cross-domain coupling must strictly decrease.
- Schema evolution occurs via versioning only.
- Replayability must be preserved indefinitely.

---

## Final Note

This domain model is **foundational**. All future schemas, pipelines, services, and storage layouts must align with these boundaries.

> **When in doubt: preserve contracts, isolate domains, and defer interpretation downstream.**
