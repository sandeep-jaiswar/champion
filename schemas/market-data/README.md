# NSE Equity OHLC Schemas

## Purpose

These schemas define the contract for equity OHLC (Open-High-Low-Close) data flowing through the platform, from raw ingestion to normalized serving.

## Schema Files

### 1. `raw_equity_ohlc.avsc`

**Topic**: `raw.market.equity.ohlc`  
**Owner**: Ingestion Domain  
**Source**: NSE bhavcopy (equity segment daily files)

#### Raw Key Characteristics

- Mirrors NSE bhavcopy format exactly
- All payload fields nullable to handle malformed/incomplete data
- No transformations applied
- Immutable once published

#### Envelope Fields

- `event_id`: UUID for global uniqueness
- `event_time`: Market timestamp from NSE
- `ingest_time`: Platform capture time
- `source`: `"nse_cm_bhavcopy"` (CM segment file), `"nse_fo_bhavcopy"` (F&O segment), or `"nse_api"` (real-time)
- `schema_version`: `"v1"`
- `entity_id`: Kafka partition key (`"SYMBOL:NSE"`)

#### Raw Payload Fields

Based on NSE BhavCopy_NSE_CM format (covers equity + derivatives + government bonds):

- `TradDt`, `BizDt`: Trade and business dates (YYYY-MM-DD)
- `Sgmt`: Segment (CM=Capital Market)
- `FinInstrmTp`: Instrument type (STK=Stock, FUT=Future, OPT=Option, IDX=Index)
- `FinInstrmId`: NSE internal instrument ID
- `ISIN`: International Securities Identification Number
- `TckrSymb`: Ticker symbol (e.g., `"RELIANCE"`)
- `SctySrs`: Security series (`"EQ"`, `"BE"`, `"GB"`, etc.)
- OHLC prices: `OpnPric`, `HghPric`, `LwPric`, `ClsPric`, `LastPric`, `PrvsClsgPric`
- `SttlmPric`: Settlement price
- Volume data: `TtlTradgVol`, `TtlTrfVal` (turnover in INR), `TtlNbOfTxsExctd`
- Derivatives: `XpryDt`, `StrkPric`, `OptnTp`, `OpnIntrst`, `ChngInOpnIntrst`
- Reserved: `Rsvd01`, `Rsvd02`, `Rsvd03`, `Rsvd04`

### 2. `normalized_equity_ohlc.avsc`

**Topic**: `normalized.market.equity.ohlc`  
**Owner**: Normalization Domain  
**Derived From**: `raw.market.equity.ohlc` + reference data + CA calendar

#### Normalized Key Characteristics

- Standardized symbol format
- Corporate action adjusted prices
- Strongly typed (non-nullable for core fields)
- Calendar-aligned with trading days
- Deterministically reproducible from raw + reference data

#### Transformations Applied

1. **Symbol Normalization**: `TckrSymb` "RELIANCE" → `"RELIANCE:NSE"`
2. **Date Standardization**: `TradDt` string → Avro date type (days since epoch)
3. **CA Adjustment**: All prices adjusted for splits, bonuses, dividends
4. **Field Mapping**: `TtlTradgVol` → `volume`, `TtlTrfVal` → `turnover`
5. **Instrument Classification**: `FinInstrmTp` + `SctySrs` → standardized type
6. **Metadata**: `adjustment_factor`, `adjustment_date`, `is_trading_day`

#### Normalized Payload Fields

- `instrument_id`: Canonical ID (`"SYMBOL:EXCHANGE"`)
- `symbol`, `exchange`, `isin`, `instrument_type`, `series`
- `trade_date`: Avro date type (days since epoch)
- `prev_close`: Previous close (CA-adjusted)
- OHLC: `open`, `high`, `low`, `close` (all CA-adjusted, non-nullable)
- Prices: `last_price`, `settlement_price` (CA-adjusted)
- Volume: `volume`, `turnover`, `trades`
- Adjustments: `adjustment_factor`, `adjustment_date`
- Validation: `is_trading_day`

## Usage Examples

### Raw Event (from NSE bhavcopy)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_time": 1735776000000,
  "ingest_time": 1735776060000,
  "source": "nse_cm_bhavcopy",
  "schema_version": "v1",
  "entity_id": "RELIANCE:NSE",
  "payload": {
    "TradDt": "2026-01-02",
    "BizDt": "2026-01-02",
    "Sgmt": "CM",
    "Src": "NSE",
    "FinInstrmTp": "STK",
    "FinInstrmId": 2885,
    "ISIN": "INE002A01018",
    "TckrSymb": "RELIANCE",
    "SctySrs": "EQ",
    "XpryDt": null,
    "FininstrmActlXpryDt": null,
    "StrkPric": null,
    "OptnTp": null,
    "FinInstrmNm": "Reliance Industries Limited",
    "OpnPric": 2450.50,
    "HghPric": 2478.90,
    "LwPric": 2445.00,
    "ClsPric": 2470.35,
    "LastPric": 2470.00,
    "PrvsClsgPric": 2455.20,
    "UndrlygPric": null,
    "SttlmPric": 2470.35,
    "OpnIntrst": null,
    "ChngInOpnIntrst": null,
    "TtlTradgVol": 12500000,
    "TtlTrfVal": 3080050000.0,
    "TtlNbOfTxsExctd": 125000,
    "SsnId": "F1",
    "NewBrdLotQty": 1,
    "Rmks": null,
    "Rsvd01": null,
    "Rsvd02": null,
    "Rsvd03": null,
    "Rsvd04": null
  }
}
```

### Normalized Event (after processing)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_time": 1735776000000,
  "ingest_time": 1735776060000,
  "source": "nse_cm_bhavcopy",
  "schema_version": "v1",
  "entity_id": "RELIANCE:NSE",
  "payload": {
    "instrument_id": "RELIANCE:NSE",
    "symbol": "RELIANCE",
    "exchange": "NSE",
    "isin": "INE002A01018",
    "instrument_type": "STK",
    "series": "EQ",
    "trade_date": 20454,
    "prev_close": 2455.20,
    "open": 2450.50,
    "high": 2478.90,
    "low": 2445.00,
    "close": 2470.35,
    "last_price": 2470.00,
    "settlement_price": 2470.35,
    "volume": 12500000,
    "turnover": 3080050000.0,
    "trades": 125000,
    "adjustment_factor": 1.0,
    "adjustment_date": null,
    "is_trading_day": true
  }
}
```

## Data Quality Rules

### Raw Schema

- `event_id` must be unique
- `event_time` <= `ingest_time` (skew tolerance: 5 minutes for end-of-day files, 30 seconds for real-time)
- `TckrSymb` must be non-empty when present
- `TradDt` must be valid date format (YYYY-MM-DD)
- All payload fields nullable for graceful handling of malformed data

### Normalized Schema

- `open`, `high`, `low`, `close` must be non-null
- `high` >= `low`
- `volume` >= 0
- `adjustment_factor` > 0
- `trade_date` must align with NSE trading calendar (ref: `reference.nse.trading_calendar`)

## Evolution Strategy

### Backward-Compatible Changes (Allowed)

- Add optional fields to payload
- Increase precision of numeric types
- Add new enum values

### Breaking Changes (Require New Version)

- Remove fields
- Change field types
- Make nullable fields non-nullable
- Change field semantics

## Corporate Actions Adjustments

### Overview

Corporate actions (splits, bonuses, dividends, rights issues) create discontinuities in historical price data. To ensure price continuity and accurate financial analysis, all OHLC prices are adjusted retrospectively when corporate actions occur.

### Adjustment Process

1. **CA Event Ingestion**: Corporate action events are scraped from NSE and stored in `corporate_actions` table
2. **Adjustment Factor Computation**: For each CA event, an adjustment factor is computed:
   - **Stock Split** (e.g., 1:5): `factor = new_shares / old_shares = 5.0`
   - **Bonus Issue** (e.g., 1:2): `factor = (existing + new) / existing = 1.5`
   - **Dividend**: `factor = 1.0 - (dividend / close_price)`
3. **Cumulative Factors**: Multiple CA events are combined using cumulative products
4. **Price Adjustment**: Historical prices before ex-date are divided by adjustment factor

### Example: Stock Split

**Scenario**: RELIANCE announces 1:5 split on 2024-01-15

| Date       | Raw Close | Adjustment Factor | Adjusted Close | Notes                    |
|------------|-----------|-------------------|----------------|--------------------------|
| 2024-01-10 | 2500      | 5.0               | 500            | Before split, adjusted   |
| 2024-01-11 | 2520      | 5.0               | 504            | Before split, adjusted   |
| 2024-01-15 | 500       | 1.0               | 500            | Ex-date, no adjustment   |
| 2024-01-16 | 505       | 1.0               | 505            | After split              |

**Result**: Price continuity maintained at ~500 level

### Example: Bonus Issue

**Scenario**: TCS announces 1:2 bonus on 2024-02-20

| Date       | Raw Close | Adjustment Factor | Adjusted Close | Notes                    |
|------------|-----------|-------------------|----------------|--------------------------|
| 2024-02-15 | 3600      | 1.5               | 2400           | Before bonus, adjusted   |
| 2024-02-19 | 3650      | 1.5               | 2433           | Before bonus, adjusted   |
| 2024-02-20 | 2400      | 1.0               | 2400           | Ex-date, no adjustment   |
| 2024-02-21 | 2420      | 1.0               | 2420           | After bonus              |

**Result**: Price continuity maintained at ~2400 level

### Implementation Details

**Module**: `src/corporate_actions/`

- `ca_processor.py`: Computes adjustment factors from CA events
- `price_adjuster.py`: Applies adjustments to OHLC data
- Tests: `tests/test_ca_processor.py`, `tests/test_price_adjuster.py`

**ClickHouse Tables**:

- `corporate_actions`: Stores CA events with computed adjustment factors
- `normalized_equity_ohlc`: Contains adjusted OHLC prices with metadata columns:
  - `adjustment_factor`: Cumulative CA adjustment applied (1.0 = no adjustment)
  - `adjustment_date`: Most recent CA ex-date affecting this record

### Query Examples

**View corporate actions for a symbol**:

```sql
SELECT 
    symbol, 
    ex_date, 
    action_type, 
    adjustment_factor,
    purpose
FROM champion_market.corporate_actions
WHERE symbol = 'RELIANCE'
ORDER BY ex_date DESC;
```

**Compare raw vs adjusted prices**:

```sql
SELECT 
    r.TradDt as trade_date,
    r.ClsPric as raw_close,
    n.ClsPric as adjusted_close,
    n.adjustment_factor,
    (r.ClsPric - n.ClsPric) as adjustment_amount
FROM champion_market.raw_equity_ohlc r
JOIN champion_market.normalized_equity_ohlc n
    ON r.TckrSymb = n.TckrSymb AND r.TradDt = n.TradDt
WHERE r.TckrSymb = 'RELIANCE'
    AND r.TradDt BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY r.TradDt;
```

**Find all symbols with recent splits**:

```sql
SELECT 
    symbol,
    ex_date,
    split_old_shares,
    split_new_shares,
    adjustment_factor
FROM champion_market.corporate_actions
WHERE action_type = 'SPLIT'
    AND ex_date >= today() - INTERVAL 90 DAY
ORDER BY ex_date DESC;
```

### Verification

To verify CA adjustments are working correctly:

1. **Continuity Check**: Prices before and after CA should maintain relative trends
2. **Volume Check**: Trading volume should NOT be adjusted (it reflects actual shares traded)
3. **Idempotency**: Re-running adjustment pipeline should produce identical results

## Next Steps

1. **Reference Schemas**: Create symbol master and CA calendar schemas
2. **Trade Schema**: Add `raw_equity_trade.avsc` for tick-by-tick data
3. **Index Schema**: Add OHLC for NIFTY50, BANKNIFTY indices
4. **Derivatives**: Add futures/options schemas
