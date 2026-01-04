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
- `source`: `"nse_bhavcopy"` or `"nse_api"`
- `schema_version`: `"v1"`
- `entity_id`: Kafka partition key (`"SYMBOL:NSE"`)

#### Raw Payload Fields

Based on NSE sec_bhavdata_full format (fields match CSV exactly):

- `SYMBOL`: Trading symbol (e.g., `"RELIANCE"`)
- `SERIES`: Security series (typically `"EQ"`, `"BE"`, `"GS"`)
- `DATE1`: Date string in NSE format (`"DD-MMM-YYYY"`)
- OHLC prices: `OPEN_PRICE`, `HIGH_PRICE`, `LOW_PRICE`, `CLOSE_PRICE`, `LAST_PRICE`, `PREV_CLOSE`
- Volume data: `TTL_TRD_QNTY`, `TURNOVER_LACS`, `NO_OF_TRADES`
- Deliverables: `DELIV_QTY`, `DELIV_PER`
- `AVG_PRICE`: Volume-weighted average price provided by NSE

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

1. **Symbol Normalization**: `"RELIANCE"` → `"RELIANCE:NSE"`
2. **Date Standardization**: `DATE1` string → Avro date type (days since epoch)
3. **CA Adjustment**: All prices adjusted for splits, bonuses, dividends
4. **Field Mapping**: `TTL_TRD_QNTY` → `volume`, `TURNOVER_LACS` → `turnover` (lakhs to INR)
5. **Unit Conversion**: `TURNOVER_LACS` × 100,000 → turnover in INR
6. **Metadata**: `adjustment_factor`, `adjustment_date`, `is_trading_day`

#### Normalized Payload Fields

- `instrument_id`: Canonical ID (`"SYMBOL:EXCHANGE"`)
- `symbol`, `exchange`, `series`
- `trade_date`: Avro date type (days since epoch)
- `prev_close`: Previous close (CA-adjusted)
- OHLC: `open`, `high`, `low`, `close` (all CA-adjusted, non-nullable)
- Prices: `last_price`, `avg_price` (NSE VWAP, CA-adjusted)
- Volume: `volume`, `turnover`, `trades`, `deliverable_volume`, `deliverable_percentage`
- Adjustments: `adjustment_factor`, `adjustment_date`
- Validation: `is_trading_day`

## Usage Examples

### Raw Event (from NSE bhavcopy)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_time": 1704297600000,
  "ingest_time": 1704297660000,
  "source": "nse_bhavcopy",
  "schema_version": "v1",
  "entity_id": "RELIANCE:NSE",
  "payload": {
    "SYMBOL": "RELIANCE",
    "SERIES": "EQ",
    "DATE1": "02-Jan-2026",
    "PREV_CLOSE": 2455.20,
    "OPEN_PRICE": 2450.50,
    "HIGH_PRICE": 2478.90,
    "LOW_PRICE": 2445.00,
    "LAST_PRICE": 2470.00,
    "CLOSE_PRICE": 2470.35,
    "AVG_PRICE": 2464.04,
    "TTL_TRD_QNTY": 12500000,
    "TURNOVER_LACS": 30800.50,
    "NO_OF_TRADES": 125000,
    "DELIV_QTY": 6250000,
    "DELIV_PER": 50.0
  }
}
```

### Normalized Event (after processing)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_time": 1704297600000,
  "ingest_time": 1704297660000,
  "source": "nse_bhavcopy",
  "schema_version": "v1",
  "entity_id": "RELIANCE:NSE",
  "payload": {
    "instrument_id": "RELIANCE:NSE",
    "symbol": "RELIANCE",
    "exchange": "NSE",
    "series": "EQ",
    "trade_date": 20476,
    "prev_close": 2455.20,
    "open": 2450.50,
    "high": 2478.90,
    "low": 2445.00,
    "close": 2470.35,
    "last_price": 2470.00,
    "avg_price": 2464.04,
    "volume": 12500000,
    "turnover": 3080050000.0,
    "trades": 125000,
    "deliverable_volume": 6250000,
    "deliverable_percentage": 50.0,
    "adjustment_factor": 1.0,
    "adjustment_date": null,
    "is_trading_day": true
  }
}
```

## Data Quality Rules

### Raw Schema

- `event_id` must be unique
- `event_time` <= `ingest_time` (within skew tolerance)
- `SYMBOL` must be non-empty when present
- `DATE1` must be valid NSE date format (`DD-MMM-YYYY`)
- All payload fields nullable for graceful handling of malformed data

### Normalized Schema

- `open`, `high`, `low`, `close` must be non-null
- `high` >= `low`
- `volume` >= 0
- `adjustment_factor` > 0
- `trade_date` must align with trading calendar

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

## Next Steps

1. **Reference Schemas**: Create symbol master and CA calendar schemas
2. **Trade Schema**: Add `raw_equity_trade.avsc` for tick-by-tick data
3. **Index Schema**: Add OHLC for NIFTY50, BANKNIFTY indices
4. **Derivatives**: Add futures/options schemas
5. **Corporate Actions**: Define split/bonus/dividend event schemas
