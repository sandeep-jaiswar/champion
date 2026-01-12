# Champion Documentation Hub

Complete documentation for the Champion data platform.

## Quick Navigation

| Purpose | Start Here |
|---------|-----------|
| **New to Champion?** | [Getting Started](00_getting_started/) - Installation, quick setup, first ETL run |
| **Understanding the design?** | [Architecture](01_architecture/) - System design, principles, tech stack |
| **How does X work?** | [Implementation](02_implementation/) - Component deep dives, how-to guides |
| **Using Champion?** | [User Guides](03_user_guides/) - Run pipelines, query warehouse, monitor |
| **Contributing code?** | [Development](04_development/) - Standards, testing, git workflow |
| **API details?** | [API Reference](05_api_reference/) - Function signatures, modules |
| **What's the schema?** | [Data Dictionaries](06_data_dictionaries/) - Field definitions, types |
| **Why this design?** | [Decisions](07_decisions/) - Architecture Decision Records (ADRs) |

## By Role

### 👤 New Developer

1. [Getting Started](00_getting_started/#getting-started) - Setup and first run
2. [Architecture Overview](01_architecture/) - Understand how things fit together
3. [Development Guide](04_development/) - How to write code for Champion

### 📊 Data Analyst

1. [User Guides](03_user_guides/) - How to use Champion
2. [Data Dictionaries](06_data_dictionaries/) - Schema and field reference
3. [Implementation: Querying](02_implementation/queries.md) - Query examples

### 🏗️ Architect

1. [Architecture](01_architecture/) - Complete system design
2. [Decisions](07_decisions/) - Design rationale
3. [Implementation](02_implementation/) - Technical details

### 👨‍💼 Manager/Lead

1. [Architecture Overview](00_getting_started/architecture_overview.md) - High-level view
2. [Decisions](07_decisions/) - Key design choices
3. [Development Guide](04_development/) - Development process

## Documentation Map

```
docs/
├── 00_getting_started/      👉 START HERE if new
│   ├── README.md            Quick navigation for this section
│   ├── installation.md       Setup Champion locally
│   ├── quick_start.md        Run your first ETL
│   ├── architecture_overview.md  System design at 10,000ft
│   └── troubleshooting.md    Common issues and fixes
│
├── 01_architecture/         Design and principles
│   ├── README.md            Architecture concepts
│   ├── vision.md            What is Champion?
│   ├── data-flow.md         Data journey through system
│   ├── domain-model.md      Core business domains
│   ├── polyglot-architecture.md  Multi-language design
│   ├── data-platform.md     Data platform architecture
│   ├── storage-strategy.md  Parquet lake + ClickHouse warehouse
│   ├── compute-strategy.md  Prefect orchestration
│   ├── feature-store.md     ML feature engineering
│   ├── security.md          Authentication & authorization
│   ├── observability.md     Monitoring and alerting
│   └── system-overview.md   Complete system diagram
│
├── 02_implementation/       How everything works
│   ├── README.md            Component guide
│   ├── fundamentals-ingestion.md     Scrape and store company data
│   ├── index-constituents.md         Track index membership
│   ├── polars-parser.md     Parse CSV to Parquet
│   ├── prefect-orchestration.md      Run and schedule flows
│   ├── prometheus-metrics.md         Monitor with Prometheus
│   ├── clickhouse.md        Query warehouse
│   ├── features.md          Engineer features
│   ├── bulk-block-deals.md           Bulk trading data
│   ├── symbol-master-enrichment.md   Enrich stock metadata
│   ├── schema-fix.md        Fix data schemas
│   └── overview.md          All implementations
│
├── 03_user_guides/          Using Champion
│   ├── README.md            User guide index
│   ├── running-etl-pipelines.md      Execute data collection
│   ├── querying-warehouse.md         Get data from ClickHouse
│   ├── feature-engineering.md        Create ML features
│   ├── monitoring-flows.md           Track pipeline health
│   ├── troubleshooting.md            Fix common problems
│   ├── bse-data-verification.md      Validate BSE data
│   ├── fundamentals-verification-queries.md  Verify company data
│   └── symbol-master-enrichment-verification.md  Verify metadata
│
├── 04_development/          Contributing
│   ├── README.md            Developer guide
│   ├── setup.md             Dev environment setup
│   ├── code-standards.md    Code style and conventions
│   ├── testing.md           Writing and running tests
│   ├── git-workflow.md      Git branching strategy
│   └── adding-components.md How to add new scrapers/features
│
├── 05_api_reference/        API Documentation
│   ├── README.md            API overview
│   ├── scrapers.md          Data collection APIs
│   ├── parsers.md           Data parsing APIs
│   ├── storage.md           Parquet I/O APIs
│   ├── warehouse.md         ClickHouse APIs
│   ├── features.md          Feature engineering APIs
│   └── orchestration.md     Prefect flow APIs
│
├── 06_data_dictionaries/    Schemas & Fields
│   ├── README.md            Data dictionary index
│   ├── nse_market_data.md   NSE stock data schema
│   ├── bse_market_data.md   BSE stock data schema
│   ├── symbol_master.md     Stock information schema
│   ├── index_constituents.md  Index member schema
│   ├── corporate_actions.md   Splits/dividends schema
│   ├── technical_indicators.md  Calculated features
│   ├── fundamentals_features.md Earnings/valuations
│   ├── macro_features.md    Economic indicators
│   ├── validation_rules.md  Data quality checks
│   └── anomaly_detection.md Outlier detection
│
└── 07_decisions/            Architecture Decisions
    ├── README.md            ADR process and index
    ├── template.md          ADR template
    ├── adr-001-clickhouse-warehouse.md
    ├── adr-002-polars-parsing.md
    ├── adr-003-prefect-orchestration.md
    ├── adr-004-parquet-lake.md
    └── adr-005-domain-driven-design.md
```

## Finding Information

**By search term:**

- `ctrl+p` then search for topic (works in VS Code)
- Use table of contents above
- Navigate by role (see "By Role" section)

**Documentation sections:**

- High-level? Start with Getting Started or Architecture
- How does X work? Check Implementation
- Need specific fields? See Data Dictionaries
- Why designed this way? Read Decisions

## Contributing

Found an issue in the docs?

1. See [Development Guide](04_development/) for git workflow
2. Make the fix
3. Submit a PR

---

**Need help?** Check [Troubleshooting](03_user_guides/troubleshooting.md) or ask the team.

Last updated: $(date +%Y-%m-%d)
