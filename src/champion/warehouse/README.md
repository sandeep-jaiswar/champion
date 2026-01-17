# Warehouse

Domain scope: ClickHouse integration (clients, loaders, queries, models).

## Structure

```
warehouse/
├── clickhouse/         # ClickHouse data warehouse integration
│   ├── batch_loader.py        # Batch data loader for ClickHouse
│   ├── generate_sample_data.py # Sample data generator
│   └── __init__.py
├── models/             # Data models for warehouse operations
│   └── __init__.py
├── adapters.py         # Adapters for different warehouse systems
└── __init__.py
```

## Current Status

- ✅ ClickHouse batch loader and sample data generator integrated
- ✅ All files consolidated under src/champion/warehouse/clickhouse/
- ✅ Tests moved to root tests/ directory

## Usage

```python
from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

loader = ClickHouseLoader()
loader.load_table("raw_equity_ohlc", "data/lake/raw/equity_ohlc/")
```

For more details, see the main `/warehouse/README.md` documentation.
