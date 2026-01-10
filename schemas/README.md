# Schemas – Source of Truth

## Purpose

This directory contains the **canonical data schemas** for the Stock Market Intelligence Platform. Schemas defined here are **contracts**, not implementation details. All ingestion services, streaming pipelines, storage layers (Hudi, ClickHouse), and analytics systems **must conform** to these schemas. If code and schema disagree, **the schema is correct**.

---

## Core Philosophy

### 1. Schemas are APIs

- Schemas define **what data means**
- Code defines **how data moves**
- Breaking schema changes are forbidden
- Evolution happens only through **versioning**

> Treat schemas with the same rigor as public APIs.

### 2. Schema-first, everywhere

Before writing producers, consumers, Spark jobs, Flink jobs, or storage mappings, the schema **must already exist**. No schema → no code.

### 3. Raw ≠ Normalized

This directory will contain **multiple schema families**. Each family has strict rules.

---

## Schema Families

### 1. Raw Market Data (`raw.*`)

#### Raw Purpose

- Preserve exchange source truth
- Enable deterministic replay
- Support auditability

#### Raw Rules

- Immutable
- No enrichment
- No normalization
- No derived fields
- Mirrors exchange payloads exactly

#### Raw Examples

- `raw.market.equity.ohlc`
- `raw.market.equity.trade`
- `raw.market.index.ohlc`

If NSE does not provide a field, **it must not appear here**.

### 2. Normalized Market Data (`normalized.*`) *(future)*

#### Normalized Purpose

- Standardize symbols
- Align timestamps
- Apply corporate actions
- Enable cross-asset analytics

#### Normalized Rules

- Derived from raw data only
- Fully reproducible from raw streams
- Explicit transformation logic

### 3. Reference & Corporate Actions *(future)*

#### Reference Purpose

- Capture slow-moving, authoritative data
- Enable financial correctness

Examples:

- Symbol mappings
- ISIN changes
- Splits, bonuses, dividends

---

## Event Envelope (Mandatory)

All event schemas **must embed** the standard platform envelope.

Indicative structure:

```json
{
  "event_id": "uuid",
  "event_time": "timestamp",
  "ingest_time": "timestamp",
  "source": "nse",
  "schema_version": "v1",
  "entity_id": "string",
  "payload": {}
}
```

### Semantics

- `event_id` → globally unique
- `event_time` → exchange / market timestamp
- `ingest_time` → platform ingestion time
- `entity_id` → Kafka partition key
- `schema_version` → immutable once published

---

## File & Naming Conventions

- Format: Avro (`.avsc`)
- One schema per file
- One Kafka topic per schema
- One event type per topic

### Directory layout (example)

```text
schemas/
└── market-data/
    ├── raw_equity_ohlc.avsc
    ├── raw_equity_trade.avsc
    └── raw_index_ohlc.avsc
```

---

## Schema Evolution Rules

### Allowed

- Adding optional fields
- Adding new schema versions
- Creating new topics

### Forbidden

- Removing fields
- Changing field meanings
- Changing field types
- Reinterpreting existing data

---

## Parquet Schema Contracts

In addition to Avro schemas for streaming (Kafka), the platform uses **Parquet** for columnar storage in the data lake. Parquet schemas are defined as JSON Schema documents and enable:

- **Data Validation**: Type checking, nullability, and range constraints
- **Quality Gates**: Automated validation in Prefect flows
- **Quarantine**: Failed records are isolated for inspection

### Avro → Parquet Mapping

See `parquet/README.md` for detailed documentation on:

- Type and logical type mappings
- Schema flattening (envelope + payload → flat structure)
- Validation rules and constraints
- Quarantine strategy for failed records
- Integration with Prefect flows

### Key Differences

| Aspect | Avro (Streaming) | Parquet (Storage) |
|--------|------------------|-------------------|
| Structure | Nested (payload envelope) | Flattened columns |
| Schema Format | `.avsc` (Avro JSON) | `.json` (JSON Schema) |
| Validation | At producer/consumer | At write time (Prefect) |
| Evolution | Forward/backward compatible | Backward compatible |
| Storage | Kafka topics | Data lake partitions |

---

## Governance

- All schema changes require an architecture ticket
- Schema reviews prioritize correctness over convenience
- Temporary fields do not exist
- Experimental schemas must be explicitly labeled
