#!/usr/bin/env bash
set -euo pipefail

# Requires: gh CLI authenticated to GitHub and run inside repo root
# Usage: ./scripts/create_issues.sh

check_gh() {
  if ! command -v gh >/dev/null 2>&1; then
    echo "gh CLI not found. Install via: sudo apt install gh" >&2
    exit 1
  fi
  gh auth status >/dev/null || {
    echo "gh CLI not authenticated. Run: gh auth login" >&2
    exit 1
  }
}

create_issue() {
  local title="$1"
  local body="$2"
  local labels="$3"
  gh issue create --title "$title" --body "$body" --label "$labels" --assignee @me || true
}

# 1. Architecture Doc
read -r -d '' ISSUE1 << 'EOF'
**Summary:** Author data platform architecture integrating Polars, Parquet, ClickHouse, Prefect, and MLflow.

**Details:**
- Document storage layout and partitioning for Parquet datasets (raw, normalized, features).
- Define ClickHouse table schemas, engines, sort keys, and ingestion methods.
- Map Avro → Parquet types and schema evolution rules.
- Orchestration plan with Prefect flows, schedules, retries, logging.
- MLflow usage patterns for tracking parameters, metrics, and artifacts.

**Deliverables:**
- docs/architecture/data-platform.md updated with the above sections.

**Acceptance Criteria:**
- Architecture doc present with clear diagrams/text.
- Parquet partitioning and file sizing guidance included.
- ClickHouse DDL examples provided for all tables.
- Prefect flow graph and schedules defined.
- MLflow tracking plan documented.
EOF
create_issue "Architecture: Polars/Parquet/ClickHouse/Prefect/MLflow" "$ISSUE1" "architecture,planning"

# 2. Polars Parser Refactor
read -r -d '' ISSUE2 << 'EOF'
**Summary:** Refactor bhavcopy parsing from pandas/CSV to Polars for performance and typing.

**Scope:**
- Implement `PolarsBhavcopyParser` reading CSV with explicit schemas and robust type casts.
- Normalize column names to canonical schema and enforce nullability.
- Benchmarks: compare parse speed and memory versus current parser.
- Write outputs to Parquet in partitioned layout `normalized/ohlc/year=YYYY/month=MM/day=DD/`.

**Acceptance Criteria:**
- New parser class under `src/parsers/polars_bhavcopy_parser.py` with unit tests.
- Parsing 2,500+ rows completes < 1s on dev machine.
- Output Parquet files are type-consistent with schema and readable by ClickHouse.
- Prefect task stub calls parser successfully.
EOF
create_issue "Parser: Refactor bhavcopy to Polars + Parquet" "$ISSUE2" "polars,parquet,ingestion"

# 3. Parquet Data Lake Setup
read -r -d '' ISSUE3 << 'EOF'
**Summary:** Establish data lake directories and write utilities for Parquet IO and partitioning.

**Scope:**
- Create `src/storage/parquet_io.py` helpers: write_df(df, dataset, partitions), coalesce small files.
- Generate `_metadata` and `_common_metadata` for datasets.
- Add retention policy utilities and a cleanup script for old partitions.

**Acceptance Criteria:**
- Directory structure created under `data/lake/{raw,normalized,features}`.
- Utilities tested against sample datasets.
- Retention CLI removes partitions older than N days.
EOF
create_issue "Storage: Parquet lake structure + IO utilities" "$ISSUE3" "storage,parquet"

# 4. ClickHouse Schema & Load
read -r -d '' ISSUE4 << 'EOF'
**Summary:** Define ClickHouse schemas and implement batch load from Parquet.

**Scope:**
- Add docker-compose service for ClickHouse (ports 8123, 9000).
- Create DDL for `raw_equity_ohlc`, `normalized_equity_ohlc`, `features_equity_indicators`.
- Implement loader: local file load via `clickhouse-client` or HTTP insert.
- Add minimal role/user and auth.

**Acceptance Criteria:**
- ClickHouse service starts locally via compose.
- Tables created and sample Parquet loaded.
- Queries for symbol/date ranges return correct row counts and aggregates.
EOF
create_issue "Warehouse: ClickHouse schemas + batch loader" "$ISSUE4" "clickhouse,warehouse"

# 5. Prefect Orchestration
read -r -d '' ISSUE5 << 'EOF'
**Summary:** Implement Prefect flows for scrape → parse → write → load.

**Scope:**
- Define tasks: scrape_bhavcopy, parse_polars_raw, normalize_polars, write_parquet, load_clickhouse.
- Configure schedules (weekday 6pm IST), retries, logging, parameters.
- Integrate MLflow logging within tasks for metrics (rows, durations).

**Acceptance Criteria:**
- Prefect flows defined in `src/orchestration/flows.py`.
- Local agent runs scheduled flow successfully for a given date.
- MLflow captures run metadata and metrics.
EOF
create_issue "Orchestration: Prefect flows + scheduling" "$ISSUE5" "prefect,orchestration"

# 6. MLflow Server & Tracking
read -r -d '' ISSUE6 << 'EOF'
**Summary:** Stand up MLflow tracking server and integrate logging calls.

**Scope:**
- Add docker-compose MLflow service (port 5000) with local artifact store.
- Provide `src/ml/tracking.py` abstraction for logging params/metrics/artifacts.
- Hook into Prefect tasks to log row counts, durations, partition ranges.

**Acceptance Criteria:**
- MLflow UI reachable at http://localhost:5000.
- Runs created per flow execution with metrics and params populated.
EOF
create_issue "ML: MLflow server + tracking integration" "$ISSUE6" "mlflow,ml"

# 7. Feature Engineering (Polars)
read -r -d '' ISSUE7 << 'EOF'
**Summary:** Implement basic technical indicators using Polars and persist to Parquet.

**Scope:**
- Indicators: SMA(5,20), EMA(12,26), RSI(14).
- Windowed operations on normalized OHLC to produce features dataset.
- Write partitioned Parquet and load into ClickHouse.

**Acceptance Criteria:**
- Feature functions under `src/features/indicators.py` with tests.
- Parquet files written to `features/equity/` and loaded.
- ClickHouse queries show expected columns/values.
EOF
create_issue "Features: Polars indicators (SMA/EMA/RSI)" "$ISSUE7" "features,polars"

# 8. Data Quality & Contracts
read -r -d '' ISSUE8 << 'EOF'
**Summary:** Formalize schema contracts and data validation for Parquet datasets.

**Scope:**
- Define JSON schema for normalized OHLC and features datasets.
- Validation utilities: types, nullability, ranges (e.g., price >= 0).
- Integrate into Prefect flow with failure alerts.

**Acceptance Criteria:**
- Schema docs added to `schemas/README.md` mapping Avro→Parquet.
- Validation passes for sample data; failing rows quarantined.
EOF
create_issue "Contracts: Parquet schema + validation" "$ISSUE8" "contracts,data-quality"

# 9. Observability & Metrics
read -r -d '' ISSUE9 << 'EOF'
**Summary:** Extend Prometheus metrics to cover new pipeline stages.

**Scope:**
- Counters/gauges: parquet_write_success, parquet_write_failed; clickhouse_load_success/failed; flow_duration.
- Expose metrics server during flows.

**Acceptance Criteria:**
- Metrics exported at runtime and scraped locally.
- Grafana dashboard JSON added (optional).
EOF
create_issue "Observability: Prometheus metrics for pipelines" "$ISSUE9" "observability,metrics"

# 10. CI/CD
read -r -d '' ISSUE10 << 'EOF'
**Summary:** Add CI pipeline: lint, type-check, unit tests, and build of orchestration components.

**Scope:**
- GitHub Actions: ruff, black, mypy, pytest; optional docker build for ClickHouse/MLflow services.

**Acceptance Criteria:**
- Pipeline green on PRs; status checks enforced.
EOF
create_issue "CI/CD: Lint, type-check, tests, builds" "$ISSUE10" "ci-cd,quality"

check_gh

echo "✔ Issues creation attempted via gh. Review on GitHub."