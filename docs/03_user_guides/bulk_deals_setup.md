# Bulk & Block Deals Data Platform - Setup & Operations Guide

## ✅ Backfill Status

All historical data successfully ingested:

| Period | Bulk Deals | Block Deals | Total Events |
|--------|-----------|-----------|--------------|
| 2024 Full Year | 26,685 | 1,556 | **28,241** |
| 2025 Full Year | 19,407 | 2,026 | **21,433** |
| 2026 Jan 1-11 | 797 | 5 | **802** |
| **TOTAL** | **46,889** | **3,587** | **50,476** |

## 📊 Data Quality

✅ **100% Data Integrity:**

- Symbol: Complete (0 nulls)
- Client Name: Complete (0 nulls)
- Quantity: Complete (0 nulls)
- Price: Complete (0 nulls)

✅ **20 Columns Preserved:**

```
event_id, event_time, ingest_time, source, schema_version,
entity_id, deal_date, symbol, client_name, quantity, avg_price,
deal_type, transaction_type, exchange, security_name, remarks,
raw_buy_sell, year, month, day
```

## 🏗️ Storage Architecture

```
data/lake/bulk_block_deals/
├── deal_type=BULK/
│   ├── year=2024/month=01-12/day=01-31/
│   ├── year=2025/month=01-12/day=01-31/
│   └── year=2026/month=01/day=01-11/
└── deal_type=BLOCK/
    ├── year=2024/month=01-12/day=01-31/
    ├── year=2025/month=01-12/day=01-31/
    └── year=2026/month=01/day=01-11/
```

**Format:** Apache Parquet with Hive partitioning
**Total Files:** 760 Parquet files
**Total Size:** ~300 MB compressed

## 📈 Key Insights

### Top Active Symbols

1. **MOBIKWIK** - 892 events, 809.7M quantity
2. **QUADFUTURE** - 535 events, 222.5M quantity
3. **MTNL** - 473 events, 1.17B quantity

### Deal Type Split

- **BULK:** 92.89% (46,889 events)
- **BLOCK:** 7.11% (3,587 events)

### Transaction Distribution

- **BUY:** 51.24% (25,866 events)
- **SELL:** 48.76% (24,610 events)

### Price Range

- Min: ₹0.03
- Max: ₹152,350
- Mean: ₹433.05
- Median: ₹153.96

### Volume

- Total Quantity: 111.8B units
- Mean per Event: 2.2M units
- Median per Event: 400K units

## 🔄 Daily Ingestion Setup

### Option 1: Manual Daily Run

```bash
# Run for today
poetry run python daily_ingestion.py

# Run for specific date
poetry run python daily_ingestion.py 2026-01-12
```

### Option 2: Automated via Cron (Linux/Mac)

```bash
# Add to crontab (crontab -e)
# Run daily at 22:30 IST (5:00 PM UTC) after market close
30 22 * * 1-5 cd /media/sandeep-jaiswar/DataDrive/champion && poetry run python daily_ingestion.py >> logs/daily_ingestion.log 2>&1
```

### Option 3: Automated via CLI

```bash
# Test daily ETL for tomorrow
poetry run champion etl-bulk-deals --start-date $(date -d +1day +%Y-%m-%d) --end-date $(date -d +1day +%Y-%m-%d)
```

## 📊 Analytics & Queries

### Run Full Analytics Suite

```bash
poetry run python analytics.py
```

Includes:

- Top 10 most active symbols
- Deal type distribution
- Transaction type split
- Events by year-month
- Top 10 clients
- Price statistics
- Quantity statistics
- Data quality check

### Custom Polars Queries

**Example: Query specific symbol trades**

```python
import polars as pl
from pathlib import Path

lake = Path("data/lake/bulk_block_deals")
files = list(lake.rglob("*.parquet"))
data = pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])

# Filter for MOBIKWIK
mobikwik = data.filter(pl.col("symbol") == "MOBIKWIK")
print(f"MOBIKWIK: {len(mobikwik)} events, {mobikwik['quantity'].sum():,} total quantity")
```

## 🔐 MLflow Tracking

All flows are tracked in MLflow:

```bash
# View all runs
mlflow ui --backend-store-uri file://./mlruns
```

Open browser: `http://localhost:5000`

**Available Metrics:**

- `dates_processed` - Number of dates successfully ingested
- `failed_dates` - Number of dates with errors
- `total_events` - Total events ingested
- `deal_types` - Deal types processed (BULK/BLOCK)

## 🔧 Configuration

**Environment Variables:**

```bash
export MLFLOW_TRACKING_URI=file:///path/to/mlruns
export PYTHONPATH=/media/sandeep-jaiswar/DataDrive/champion/src:$PYTHONPATH
```

**Prefect Settings** (in code):

- Flow execution: Sequential
- Task retries: 1 (per-date basis)
- Timeout: None (no timeout)

## 📋 Checklist

- [x] Backfill 2024, 2025, 2026
- [x] Validate data integrity (50,476 events)
- [x] Verify all NSE columns preserved
- [x] Create validation script
- [x] Create analytics suite
- [x] Create daily ingestion script
- [ ] Set up cron/scheduler for daily runs
- [ ] Load data to ClickHouse (optional)
- [ ] Create dashboard (optional)

## 🚀 Next Steps

1. **Set up daily ingestion** (cron or scheduler)
2. **Monitor MLflow** for daily run health
3. **Run analytics monthly** for insights
4. **Archive old data** when storage becomes a concern
5. **Integrate with ClickHouse** for OLAP queries

## 📞 Support Commands

```bash
# Validate backfill
poetry run python validate_backfill.py

# Run analytics
poetry run python analytics.py

# Manual daily ingestion
poetry run python daily_ingestion.py

# View specific date
poetry run champion etl-bulk-deals --start-date 2026-01-12 --end-date 2026-01-12

# Check MLflow
mlflow ui --backend-store-uri file://./mlruns
```

---

**Created:** 2026-01-11  
**Platform:** Champion v0.1.0  
**Status:** Production Ready ✅
