# Stock Market Intelligence Platform

## Overview

This repository contains the foundations of a **production-grade, event-driven stock market analysis platform**, inspired by large-scale systems at companies like Uber.

The platform is designed to:

- Ingest raw exchange data (starting with NSE)
- Preserve immutable source truth
- Support real-time and batch analytics
- Enable financial correctness at scale
- Power research, modeling, and trading intelligence

This is **not** a monolithic application. It is a **polyglot data platform** built around **schemas, events, and contracts**.

---

## Core Principles (Read This First)

### 1. Schema-first, code-second

Schemas are **APIs**. All systems (Kafka, Hudi, ClickHouse, Spark, Flink, services) derive from schemas.

- Breaking schema changes are forbidden
- Evolution happens via versioning
- Code must conform to schemas, never the reverse

### 2. Raw data is sacred

Raw market data:

- Is immutable
- Is replayable
- Mirrors the exchange exactly
- Is never enriched or corrected

Any transformation, normalization, or adjustment happens **downstream**.

### 3. Event-driven by default

The system is built around:

- Kafka as the event backbone
- Explicit topic ownership
- Deterministic replay
- Idempotent consumers

No service talks to another service directly for market data.

### 4. Financial correctness > convenience

This platform optimizes for:

- Auditability
- Reproducibility
- Traceability
- Correctness under reprocessing

Low latency is important — but never at the cost of correctness.

---

## High-level Architecture

```text
[ Exchange / NSE ]
      |
      v
[ Ingestion Services ]
      |
      v
[ Kafka (Raw Topics) ]
      |
      +--> [ Hudi Bronze (Immutable) ]
      |
      +--> [ Normalization Pipelines ]
      |
      v
[ Hudi Silver / Gold ]
      |
      +--> ClickHouse
      +--> Analytics / Models
```

---

## Repository Structure

```text
├── README.md
├── docs/
│   ├── architecture/   # System design & contracts
│   ├── decisions/      # Technology Decision Records (TDRs)
│   └── issues/         # Canonical issue definitions
├── schemas/            # Avro schemas (source of truth)
└── .github/
    └── ISSUE_TEMPLATE/ # Enforced issue discipline
```

---

## Domain Boundaries (Important)

This repository enforces **strict domain separation**:

- **Ingestion** — fetches and emits raw exchange events
- **Market Data (Raw)** — immutable, replayable event streams
- **Normalization** — symbol mapping, corporate actions, alignment
- **Storage** — Hudi (lakehouse) and ClickHouse (serving)
- **Analytics & Intelligence** — indicators, signals, models (out of scope initially)

Cross-domain shortcuts are explicitly forbidden.

---

## What This Repo Is NOT

- ❌ A trading bot
- ❌ A UI/dashboard project
- ❌ A one-off data scraper
- ❌ A monolithic application

Those may exist later — **outside** this core platform.

---

## Development Philosophy

- Architecture before implementation
- Contracts before code
- Small, composable services
- Clear ownership and boundaries
- Tickets as executable intent

If something is unclear, it belongs in `docs/architecture/`, not in code comments.

---

## Contribution Rules (Non-Negotiable)

- No feature work without an architecture ticket
- No schema changes without versioning
- No enrichment in raw domains
- No hidden coupling between services
- No “temporary” hacks

When in doubt:

> **Preserve data. Defer decisions. Document intent.**

---

## Status

🚧 **Active planning & foundation phase**

The current focus is on:

- Architecture
- Schemas
- Event contracts
- Storage design

Implementation will follow only after contracts are locked.
