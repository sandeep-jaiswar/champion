# Domain Model & Bounded Contexts

This document defines the **core domains**, their **responsibilities**, **owned data**, and **interaction rules** for the Stock Market Analysis Platform.

The goal is to enforce **clear ownership**, enable **polyglot persistence**, and prevent tight coupling as the system scales.

---

## 1. Why Domain Boundaries Matter

In high-volume financial systems:

* Ambiguous ownership leads to data corruption
* Shared databases destroy scalability
* “Convenience joins” become systemic bottlenecks

Therefore, **each domain owns its data, schemas, pipelines, and SLAs**.

---

## 2. Top-Level Domain Map

```
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

Data flows **downstream only**. Upstream domains are immutable.

---

## 3. Ingestion Domain

### Purpose

Acquire data from external sources **as-is**, without interpretation.

### Responsibilities

* NSE scraping (prices, volumes, derivatives)
* Corporate announcements & PDFs
* API-based ingestion
* Source metadata capture

### Owns

* Raw schemas
* Kafka raw topics
* Source-specific idempotency

### Does NOT

* Normalize symbols
* Adjust prices
* Drop or correct data

### Output

* Immutable raw events

---

## 4. Normalization & Enrichment Domain

### Purpose

Convert raw data into **financially meaningful, consistent records**.

### Responsibilities

* Symbol / ISIN normalization
* Corporate action adjustments
* FX normalization (future)
* Trading calendar alignment

### Owns

* Normalized schemas
* Enrichment logic
* Reference data caches

### Does NOT

* Persist final truth
* Serve data to users

### Output

* Clean, enriched events

---

## 5. Storage Domain

### Purpose

Persist data durably in **query-optimized and replayable forms**.

### Responsibilities

* Bronze / Silver / Gold layers
* Partitioning strategy
* Retention & compaction

### Owns

* Hudi tables
* ClickHouse schemas
* OLAP materializations

### Does NOT

* Compute business metrics
* Interpret analytics

---

## 6. Analytics Domain

### Purpose

Transform stored data into **features, aggregates, and insights**.

### Responsibilities

* Batch analytics
* Feature computation
* Research datasets
* Feature Store

### Owns

* Feature definitions
* Batch pipelines
* Analytical correctness

### Does NOT

* Execute trading logic
* Serve real-time APIs

---

## 7. Intelligence Domain

### Purpose

Generate **signals, anomalies, and models** from analytics outputs.

### Responsibilities

* Rule-based signals
* Statistical detection
* ML models

### Owns

* Signal definitions
* Model artifacts
* Evaluation metrics

### Does NOT

* Persist raw data
* Provide dashboards

---

## 8. Serving Domain

### Purpose

Expose data and insights **efficiently and safely**.

### Responsibilities

* Read-optimized APIs
* Caching
* Dashboard backends

### Owns

* API contracts
* Query orchestration
* Response SLAs

### Does NOT

* Compute analytics
* Mutate upstream data

---

## 9. Governance (Cross-Cutting)

### Responsibilities

* Data lineage
* Quality checks
* Reconciliation
* Auditability

Governance can **observe all domains** but **own none**.

---

## 10. Infra (Cross-Cutting)

### Responsibilities

* Kafka
* Kubernetes
* CI/CD
* Secrets & configs

Infra provides platforms; domains consume them.

---

## 11. Explicit Interaction Rules

1. Domains communicate **only via events or contracts**
2. No domain reads another domain’s database directly
3. Storage is not a business logic layer
4. Analytics never mutate storage
5. Serving never recomputes analytics

Violations require explicit architectural approval.

---

## 12. Evolution Strategy

* Domains may split internally over time
* Cross-domain coupling must always decrease
* Schemas evolve via versioning, never breaking changes

---

This domain model is **foundational**. All future pipelines, schemas, and services must align with it.
