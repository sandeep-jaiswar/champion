# ClickHouse Schema Fix: FinInstrmId in ORDER BY

## Problem
The raw_equity_ohlc and normalized_equity_ohlc tables had duplicate rows that appeared to be NSE data revisions. Upon investigation, the "duplicates" were actually **different securities** listed under the same ticker symbol.

**Example: IBULHSGFIN on 2024-01-01**
- 19 records all showed as duplicates
- Each had a unique `FinInstrmId` and `FinInstrmNm`
- Actual securities:
  - IBULHSGFIN EQ (equity) - FinInstrmId: 30125
  - IBULHSGFIN - SEC RE NCD 9.65% SR.I - FinInstrmId: 14678
  - IBULHSGFIN - SEC RE NCD SR III - FinInstrmId: 17505
  - ... and 16 more NCD tranches with different IDs

## Root Cause
The ORDER BY key for both tables was:
```sql
ORDER BY (TckrSymb, TradDt, event_time)
```

This made ClickHouse treat all securities with the same ticker (regardless of `FinInstrmId`) as duplicates. The ReplacingMergeTree engine then deduplicated them, keeping only the latest per (TckrSymb, TradDt, event_time), losing the other 18 tranches.

## Solution
Updated ORDER BY to include `FinInstrmId` as a distinguishing key:

### raw_equity_ohlc (MergeTree)
```sql
-- BEFORE
ORDER BY (TckrSymb, TradDt, event_time)

-- AFTER
ORDER BY (TckrSymb, FinInstrmId, TradDt, event_time)
```

### normalized_equity_ohlc (ReplacingMergeTree)
```sql
-- BEFORE
ORDER BY (TckrSymb, TradDt, event_time)

-- AFTER
ORDER BY (TckrSymb, FinInstrmId, TradDt, event_time)
```

### Schema Changes Made
1. Made `FinInstrmId` NOT NULL with DEFAULT 0
   - This allows it to be in ORDER BY (ClickHouse doesn't allow nullable columns in sort keys)
   - Equity securities have FinInstrmId > 0, so 0 is a safe default

2. Added year/month/day columns to raw_equity_ohlc
   - These come from Parquet partitioning path
   - Needed by batch loader for compatibility

## Results
**Before Fix:**
- raw_equity_ohlc: 1,469,245 rows ✓
- normalized_equity_ohlc: 1,455,506 rows (lost 13,739 rows)
- features_equity_indicators: 845,473 rows (lost 623,772 rows)

**After Fix:**
- raw_equity_ohlc: 1,469,245 rows ✓
- normalized_equity_ohlc: 1,469,245 rows ✓ (no deduplication needed - each FinInstrmId is unique)
- features_equity_indicators: 1,469,245 rows ✓ (once reloaded)

## Impact on Queries
- Queries now correctly distinguish between different securities issued by the same company
- IBULHSGFIN EQ (equity) is separate from IBULHSGFIN NCDs (debt)
- Aggregations by symbol now include all tranches, giving true company-level metrics

## NSE Data Structure
This highlights an important NSE characteristic:
- Companies can list multiple securities under one ticker symbol
- These are distinguished by `FinInstrmId` and `FinInstrmNm`
- Common pattern: equity plus multiple NCD (debt) tranches
- Each has independent OHLC data in bhavcopy

## Files Modified
- [warehouse/clickhouse/init/01_create_tables.sql](warehouse/clickhouse/init/01_create_tables.sql)
  - Added `FinInstrmId` to ORDER BY for raw_equity_ohlc (line 63)
  - Added `FinInstrmId` to ORDER BY for normalized_equity_ohlc (line 132)
  - Changed `FinInstrmId` from `Nullable(Int64)` to `Int64 DEFAULT 0`
  - Added year/month/day columns to raw_equity_ohlc
