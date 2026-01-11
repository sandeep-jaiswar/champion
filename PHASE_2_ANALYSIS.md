# Phase 2: Source Code Reorganization - Analysis & Plan

## Current State Analysis

### Code Distribution
```
ingestion/nse-scraper/          ← MAIN SCRAPER LOGIC (production code)
  - Data collection, Kafka integration, Prefect flows
  - Has its own pyproject.toml with full dependencies
  
warehouse/loader/               ← WAREHOUSE LOADING
  - ClickHouse batch loader
  - Only 4 files + tests
  
validation/                     ← DATA VALIDATION
  - Separate package structure
  - validation/src/validation/ pattern
  - Has pyproject.toml
  
src/                            ← CORE LIBRARIES
  ├── corporate_actions/        Feature: Price adjustments for splits/dividends
  ├── features/                 Feature: Technical indicators
  ├── storage/                  Library: Parquet I/O abstractions
  └── ml/                       Library: MLflow tracking utilities
  
Root Level Scripts             ← NEED ORGANIZATION
  - run_fundamentals_etl.py
  - run_index_etl.py
  - run_macro_etl.py
  - test_index_constituent.py
  
examples/                       ← EXAMPLES
  - macro_correlation_analysis.py
```

### Issues with Current Structure
1. **Fragmented Codebase**
   - Main scraper logic in `ingestion/nse-scraper/` (production code shouldn't be in "ingestion" folder)
   - Core libraries in `src/` but missing key components
   - Warehouse code in separate `warehouse/` folder
   - Validation in separate `validation/` folder
   - No clear entry point or package structure

2. **Missing Components**
   - Scrapers: Not packaged/importable, buried in `ingestion/nse-scraper/src/`
   - Parsers: Not found as standalone module
   - Warehouse client: Only has loader, not query client
   - Orchestration: Not organized as package
   - Missing proper `__init__.py` files

3. **Multiple pyproject.toml Files**
   - `ingestion/nse-scraper/pyproject.toml` (main scraper)
   - `validation/pyproject.toml` (validation package)
   - `src/storage/pyproject.toml` (storage package)
   - No root `pyproject.toml` tying everything together

4. **Test Locations**
   - Tests scattered: `src/*/tests/`, `warehouse/loader/tests/`, `validation/tests/`
   - Root level test: `test_index_constituent.py`
   - No unified test runner

---

## Target Architecture

### New Package Structure

```
src/champion/                          ← UNIFIED MAIN PACKAGE
├── __init__.py
├── 
├── scrapers/                          Domain: Data Collection
│   ├── __init__.py
│   ├── nse/
│   │   ├── __init__.py
│   │   ├── scraper.py                 NSE scraper implementation
│   │   ├── constants.py               NSE URLs, symbols
│   │   └── validators.py              NSE data validators
│   ├── bse/
│   │   ├── __init__.py
│   │   └── scraper.py                 BSE scraper implementation
│   └── base.py                        Base scraper class
│
├── parsers/                           Domain: Data Parsing
│   ├── __init__.py
│   ├── polars_parser.py               Polars-based parsing
│   ├── schemas.py                     Parquet schemas
│   └── validators.py                  CSV validation
│
├── storage/                           Domain: Data Storage (existing)
│   ├── __init__.py
│   ├── parquet_io.py                  Parquet read/write
│   ├── retention.py                   Retention policies
│   └── tests/
│       └── (tests move to root tests/)
│
├── warehouse/                         Domain: Data Warehouse
│   ├── __init__.py
│   ├── clickhouse/
│   │   ├── __init__.py
│   │   ├── client.py                  ClickHouse connection
│   │   ├── batch_loader.py            Batch loading
│   │   └── queries.py                 Common queries
│   └── models/                        Data models
│       ├── __init__.py
│       ├── market_data.py
│       ├── reference_data.py
│       └── corporate_actions.py
│
├── features/                          Domain: Feature Engineering (existing)
│   ├── __init__.py
│   ├── indicators.py                  Technical indicators
│   ├── fundamentals.py                Fundamental indicators
│   └── tests/
│       └── (tests move to root tests/)
│
├── corporate_actions/                 Domain: Corporate Actions (existing)
│   ├── __init__.py
│   ├── price_adjuster.py              Price adjustment logic
│   ├── ca_processor.py                Corporate action processor
│   └── tests/
│       └── (tests move to root tests/)
│
├── orchestration/                     Domain: Flow Orchestration (NEW)
│   ├── __init__.py
│   ├── flows/
│   │   ├── __init__.py
│   │   ├── market_data_flow.py        Market data ETL
│   │   ├── fundamentals_flow.py       Fundamentals ETL
│   │   ├── index_constituents_flow.py Index ETL
│   │   └── macro_flow.py              Macro data ETL
│   ├── tasks/
│   │   ├── __init__.py
│   │   ├── scraping_tasks.py          Common scraping tasks
│   │   ├── parsing_tasks.py           Common parsing tasks
│   │   ├── warehouse_tasks.py         Loading tasks
│   │   └── feature_tasks.py           Feature computation
│   └── config.py                      Flow configuration
│
├── ml/                                Domain: ML Utilities (existing)
│   ├── __init__.py
│   └── tracking.py                    MLflow tracking
│
└── validation/                        Domain: Data Validation
    ├── __init__.py
    ├── validator.py                   Validation logic
    ├── flows.py                       Validation flows
    └── schemas.py                     Validation schemas

tests/                                 ← UNIFIED TEST SUITE
├── conftest.py                        Shared fixtures
├── unit/
│   ├── test_scrapers.py
│   ├── test_parsers.py
│   ├── test_storage.py
│   ├── test_warehouse.py
│   ├── test_features.py
│   ├── test_corporate_actions.py
│   └── test_ml.py
├── integration/
│   ├── test_etl_pipeline.py
│   └── test_warehouse_integration.py
└── fixtures/
    ├── sample_data.py
    └── mock_clients.py

scripts/                               ← PRODUCTION SCRIPTS
├── data/
│   ├── run_fundamentals_etl.py        (moved from root)
│   ├── run_index_etl.py               (moved from root)
│   └── run_macro_etl.py               (moved from root)
└── utils/
    └── (utility scripts)

examples/                              ← EXAMPLES (unchanged)
├── macro_correlation_analysis.py
└── feature_engineering_example.py
```

### Package Dependencies (pyproject.toml)

Will have ONE root `pyproject.toml` with:
- All dependencies consolidated
- `packages = [{include = "champion", from = "src"}]`
- CLI entry points for main flows
- Scripts pointing to `scripts/data/`
- All tools (pytest, black, ruff, mypy)

---

## Migration Steps

### Step 1: Create New Package Structure
- [ ] Create `src/champion/` with `__init__.py`
- [ ] Create all domain subdirectories
- [ ] Create `tests/` directory structure
- [ ] Create `scripts/` directory structure

### Step 2: Move and Reorganize Code
- [ ] Move `src/storage/` → `src/champion/storage/`
- [ ] Move `src/features/` → `src/champion/features/`
- [ ] Move `src/corporate_actions/` → `src/champion/corporate_actions/`
- [ ] Move `src/ml/` → `src/champion/ml/`
- [ ] Extract `ingestion/nse-scraper/src/` → `src/champion/scrapers/nse/`
- [ ] Extract `warehouse/loader/` → `src/champion/warehouse/clickhouse/`
- [ ] Extract `validation/src/validation/` → `src/champion/validation/`
- [ ] Create `src/champion/parsers/` from common code
- [ ] Create `src/champion/orchestration/` with flows from `ingestion/nse-scraper/`

### Step 3: Move Tests
- [ ] Consolidate all tests to `tests/` root
- [ ] Move `src/*/tests/` → `tests/unit/`
- [ ] Move `warehouse/loader/tests/` → `tests/unit/`
- [ ] Move `validation/tests/` → `tests/unit/`
- [ ] Update test imports

### Step 4: Move Scripts
- [ ] Move `run_*.py` scripts → `scripts/data/`
- [ ] Move `test_index_constituent.py` → `tests/unit/`
- [ ] Create entry points in pyproject.toml

### Step 5: Create Root pyproject.toml
- [ ] Consolidate all dependencies
- [ ] Set package root to `src/champion`
- [ ] Configure pytest, black, ruff, mypy at root level
- [ ] Define CLI entry points

### Step 6: Fix All Imports
- [ ] Find all imports from old locations
- [ ] Update to new `champion.*` imports
- [ ] Test import resolution
- [ ] Verify no circular imports

### Step 7: Remove Old Structures
- [ ] Remove `src/storage/pyproject.toml`
- [ ] Remove `src/ml/__init__.py` (if no special handling)
- [ ] Delete `validation/pyproject.toml`
- [ ] Delete old `ingestion/` structure (keep docs only)
- [ ] Delete `warehouse/loader/` (move to `champion/warehouse/`)

### Step 8: Testing & Verification
- [ ] Run full test suite
- [ ] Verify all imports work
- [ ] Test CLI entry points
- [ ] Check package installation
- [ ] Verify Prefect flows still work

---

## Import Refactoring Examples

### Before
```python
from src.storage.parquet_io import read_parquet
from src.features.indicators import calculate_ma
from src.corporate_actions.price_adjuster import adjust_price
# From ingestion/nse-scraper/src/
from src.main import NSEScraper
from src.kafka_producer import KafkaProducer
```

### After
```python
from champion.storage import read_parquet
from champion.features import calculate_ma
from champion.corporate_actions import adjust_price
from champion.scrapers.nse import NSEScraper
from champion.orchestration import KafkaProducer
```

---

## Risk Mitigation

1. **Backward Compatibility**
   - Maintain old imports with deprecation warnings initially
   - Create compatibility shims if needed
   - Gradual migration for external dependencies

2. **Testing Strategy**
   - Run tests after each major module move
   - Create smoke tests for each domain
   - Integration tests before/after refactoring

3. **Documentation**
   - Update all imports in docs
   - Create migration guide for dependent code
   - Document new module structure in architecture docs

4. **Git Strategy**
   - Commit by domain (scrapers, warehouse, features, etc.)
   - Tag major milestones
   - Easy to revert if issues arise

---

## Timeline Estimate

- **Step 1-2**: Create structure + move code (2-3 hours)
- **Step 3-4**: Move tests + scripts (30 mins)
- **Step 5**: Create root config (30 mins)
- **Step 6**: Fix imports (1-2 hours, depends on count)
- **Step 7**: Cleanup (30 mins)
- **Step 8**: Testing + verification (1 hour)

**Total**: ~6-8 hours of focused work

---

## Success Criteria

✅ All code organized under `src/champion/` with clear domains
✅ All tests in `tests/` directory with unified runner
✅ All scripts in `scripts/` directory
✅ Single root `pyproject.toml` defining all dependencies
✅ All imports updated and verified
✅ Full test suite passes
✅ Package installs correctly: `pip install -e .`
✅ All Prefect flows still work
✅ Documentation updated with new structure

---

Ready for Phase 2 execution!
