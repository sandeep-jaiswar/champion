# System Overview & Data Flow

This document describes the **end-to-end architecture** and **data flow** of the Stock Market Analysis Platform, showing how data moves across domains from ingestion to serving.

It operationalizes the **Vision** and **Domain Model** into a concrete, executable system design.

---

## 1. Architectural North Star

The platform follows an **Uber-inspired log-centric architecture**:

> **Kafka is the system of movement. Storage systems are sinks. Compute systems are consumers.**

All market data enters the platform as **immutable events** and flows downstream through clearly defined domains.

---

## 2. High-Level Flow

```
External Sources (NSE, APIs, PDFs)
            │
            ▼
┌──────────────────────────┐
│      Ingestion Domain     │
│  (Scrapers / Pullers)     │
└────────────┬─────────────┘
             │  Raw Events
             ▼
┌──────────────────────────┐
│          Kafka            │  ← Event Backbone
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│  Normalization Domain     │
│ (Stream / Batch Compute)  │
└────────────┬─────────────┘
             │  Enriched Events
             ▼
┌──────────────────────────┐
│      Storage Domain       │
│ (Hudi / ClickHouse)       │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│     Analytics Domain      │
│ (Batch / Feature Store)   │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│    Intelligence Domain    │
│ (Signals / Models)        │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│      Serving Domain       │
│ (APIs / Dashboards)       │
└──────────────────────────┘
```

---

## 3. Ingestion → Kafka (Raw Truth)

### Key Characteristics

* Stateless ingestion services
* Source-specific idempotency
* No transformations

### Output

* Kafka **raw topics**
* Schemas reflect source payloads

### Guarantees

* At-least-once delivery
* No data loss
* Ordering preserved per symbol

---

## 4. Kafka as the Central Backbone

Kafka is the **only** system allowed to:

* Decouple producers and consumers
* Buffer bursts
* Enable replay

### Topic Categories

* `raw.*`
* `normalized.*`
* `analytics.*` (derived events)

Kafka topics are **append-only** and **versioned by schema**, never by topic deletion.

---

## 5. Normalization & Enrichment Flow

### Modes

* **Streaming**: near–real-time normalization
* **Batch**: historical backfills and corrections

### Responsibilities

* Symbol / ISIN resolution
* Corporate action application
* Data standardization

### Output

* Clean events to Kafka
* Optional write-through to Silver storage

---

## 6. Storage Layering Strategy

### Bronze (Raw)

* Immutable
* Source-aligned
* Full replay capability

### Silver (Normalized)

* Cleaned & enriched
* Queryable
* Stable schemas

### Gold (Curated)

* Aggregates
* Analytical views
* Feature-aligned

Each layer can be **fully recomputed from upstream data**.

---

## 7. Batch vs Real-Time Split

### Real-Time Path

* Kafka → Stream Processor → ClickHouse / Pinot
* Used for:

  * Monitoring
  * Dashboards
  * Alerts

### Batch Path

* Hudi → Spark
* Used for:

  * Research
  * Backtesting
  * Feature generation

Both paths share schemas but differ in latency and guarantees.

---

## 8. Analytics & Feature Flow

```
Gold Data
   │
   ├─ Batch Feature Jobs
   ├─ Rolling Aggregations
   └─ Derived Metrics
   │
   ▼
Feature Store
```

Features are:

* Deterministic
* Versioned
* Recomputable

---

## 9. Intelligence Flow

Signals and models consume **features**, not raw market data.

```
Features → Signals / Models → Scores / Alerts
```

This separation ensures:

* Explainability
* Testability
* Independent evolution

---

## 10. Serving Flow

### Principles

* Read-only
* No heavy computation
* Aggressively cached

### Query Pattern

```
Client → API → Cache → OLAP Store
```

Serving systems never reach upstream domains directly.

---

## 11. Reprocessing & Backfills (Critical)

Any correction follows this rule:

> **Recompute from the earliest affected layer; never patch downstream.**

### Supported Operations

* Replay Kafka topics
* Rebuild Hudi tables
* Recompute analytics

---

## 12. Failure Isolation

Failures are contained within domains:

* Ingestion failure ≠ analytics failure
* Analytics delay ≠ serving outage

Kafka acts as the shock absorber.

---

## 13. What This Enables

* Polyglot persistence without chaos
* Independent scaling of compute and storage
* Strong auditability
* Safe experimentation

---

This system overview is the **blueprint**. All Kafka topics, schemas, pipelines, and services must align with this flow.
