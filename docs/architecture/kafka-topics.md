# Kafka Topic Taxonomy & Event Contracts

This document defines the **Kafka topic hierarchy**, **naming conventions**, and **event contract standards** used across the Stock Market Analysis Platform.

Kafka topics are treated as **public, versioned APIs** between domains.

---

## 1. Design Principles

1. **Topics reflect domains, not implementations**
2. **Schemas evolve; topics do not**
3. **One event type per topic**
4. **Immutability is non-negotiable**
5. **Reprocessing must always be possible**

---

## 2. Topic Naming Convention

```text
<layer>.<domain>.<entity>.<event_type>
```text

### Components

- **layer**: `raw`, `normalized`, `analytics`
- **domain**: `market`, `corporate`, `reference`, `system`
- **entity**: logical business entity
- **event_type**: semantic action

### Examples

```text
raw.market.equity.trade
raw.market.index.ohlc
raw.corporate.announcement.pdf

normalized.market.equity.ohlc
normalized.market.derivative.ohlc

analytics.market.equity.features
analytics.market.signals.anomaly
```

---

## 3. Topic Categories

### 3.1 Raw Topics (`raw.*`)

**Owned by:** Ingestion Domain  
**Purpose:** Preserve source truth exactly as received

Characteristics:

- Source-aligned schemas
- No enrichment
- No correction

Examples:

- `raw.market.equity.trade`
- `raw.market.equity.ohlc`
- `raw.corporate.announcement.pdf`

---

### 3.2 Normalized Topics (`normalized.*`)

**Owned by:** Normalization Domain  
**Purpose:** Financially consistent, cleaned events

Characteristics:

- Canonical symbols
- Adjusted prices
- Stable schemas

Examples:

- `normalized.market.equity.ohlc`
- `normalized.market.index.ohlc`

---

### 3.3 Analytics Topics (`analytics.*`)

**Owned by:** Analytics Domain  
**Purpose:** Derived metrics and features

Characteristics:

- Deterministic outputs
- Versioned feature definitions

Examples:

- `analytics.market.equity.features`
- `analytics.market.equity.returns`

---

## 4. Event Envelope Standard

All events must conform to a **standard envelope** to support lineage, replay, and debugging.

### Required Fields

```json
{
  "event_id": "uuid",
  "event_time": "timestamp",
  "ingest_time": "timestamp",
  "source": "nse|api|file",
  "schema_version": "v1",
  "entity_id": "string",
  "payload": {}
}
```

### Notes

- `event_time` = market time
- `ingest_time` = system time
- `entity_id` used as Kafka key

---

## 5. Schema Evolution Rules

1. Backward-compatible changes only
2. New fields must be optional
3. No field removal
4. Breaking changes require new topic

Schema changes are recorded as **Technology Decision Records (TDRs)**.

---

## 6. Partitioning Strategy

### Kafka Key

- Primary key: `entity_id` (e.g., symbol + exchange)

### Guarantees

- Ordering preserved per symbol
- Horizontal scalability

---

## 7. Retention Policy Guidelines

| Topic Type | Retention           |
| ---------- | ------------------- |
| Raw        | Long-term (months+) |
| Normalized | Medium-term         |
| Analytics  | Short to medium     |

Actual retention depends on cost and compliance requirements.

---

## 8. Anti-Patterns (Explicitly Forbidden)

- Multiple event types in one topic
- Schema-less JSON
- Downstream mutation of events
- Ad-hoc topic creation

---

## 9. What This Enables

- Clean domain boundaries
- Safe replay & backfills
- Independent consumer evolution
- Polyglot storage integration

---

This document defines the **event contracts** of the platform. Any service producing or consuming Kafka topics must comply strictly with these rules.
