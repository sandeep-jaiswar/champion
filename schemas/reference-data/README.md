# Reference Data Schemas

## Purpose

Reference data schemas define the contracts for slowly-changing dimensional data that enriches and validates market data. These schemas support normalization, validation, and corporate action adjustments.

## Schema Files

### 1. `symbol_master.avsc`

**Topic**: `reference.nse.symbol_master`, `reference.bse.symbol_master`  
**Owner**: Reference Data Domain  
**Update Pattern**: SCD Type 2 (temporal validity with valid_from/valid_to)

#### Purpose (Symbol Master)

- Canonical mapping: exchange symbol → instrument_id
- Direct source: NSE EQUITY_L.csv (2223 listed instruments)
- Fields: SYMBOL, NAME OF COMPANY, SERIES, DATE OF LISTING, PAID UP VALUE, MARKET LOT, ISIN, FACE VALUE
- Enrichment: sector, industry, market cap, tick size, index membership
- Trading lot size and face value validation
- Validation source for normalization pipeline

#### Key Fields (Symbol Master)

- `instrument_id`: Canonical identifier (SYMBOL:EXCHANGE)
- `symbol`: From EQUITY_L SYMBOL column
- `company_name`: From EQUITY_L NAME OF COMPANY
- `isin`: From EQUITY_L ISIN NUMBER (unique per symbol)
- `series`: From EQUITY_L SERIES (EQ, BE, etc.)
- `listing_date`: From EQUITY_L DATE OF LISTING
- `face_value`: From EQUITY_L FACE VALUE (₹ per share)
- `paid_up_value`: From EQUITY_L PAID UP VALUE (₹ per share)
- `lot_size`: From EQUITY_L MARKET LOT (typically 1 for NSE)
- `status`: ACTIVE, SUSPENDED, DELISTED
- `valid_from`, `valid_to`: Temporal validity window (SCD Type 2)

#### Usage (Symbol Master)

```python
# Normalization pipeline lookup
symbol_master = lookup_latest(instrument_id="RELIANCE:NSE")
if symbol_master.status != "ACTIVE":
    quarantine_event(reason="INACTIVE_SYMBOL")

# Validate lot size
if order_quantity % symbol_master.lot_size != 0:
    reject_order(reason="INVALID_LOT_SIZE")

# Validate face value for splits/bonus
if event_type == "CORPORATE_ACTION":
    pre_ca_price = symbol_master.paid_up_value
```

### 2. `corporate_action.avsc`

**Topic**: `reference.nse.corporate_actions`, `reference.bse.corporate_actions`  
**Owner**: Reference Data Domain  
**Update Pattern**: Event-sourced (immutable, append-only)

#### Purpose (Corporate Actions)

- Historical record of all corporate actions from NSE CA file (1923 events, Jan 2025 - Jan 2026)
- Fields: SYMBOL, COMPANY NAME, SERIES, PURPOSE, FACE VALUE, EX-DATE, RECORD DATE, BOOK CLOSURE START DATE, BOOK CLOSURE END DATE
- Parse PURPOSE field to extract splits, bonuses, dividends, rights, interest payments, EGM meetings
- Compute `adjustment_factor` for normalized OHLC price adjustments
- Backfill and replay-safe design (event-sourced, immutable, append-only)
- Audit trail for all price adjustments

#### Corporate Action Types (Parsed PURPOSE)

- **SPLIT**: Face Value Split (Sub-Division) - e.g., Rs 10 → Re 1 per share
- **BONUS**: Bonus issue - e.g., Bonus 2:1 (2 new shares for 1 existing)
- **DIVIDEND**: Interim/Final/Special dividend - e.g., Rs 5.50 Per Share
- **RIGHTS**: Rights issue - e.g., Rights 5:41 @ Premium Rs 109
- **INTEREST_PAYMENT**: Interest payment on government bonds - e.g., Government Securities
- **EGMMEETING**: Extraordinary General Meeting (no price adjustment)
- **MERGER**, **DEMERGER**, **BUYBACK**: Other corporate events

#### Key Fields (Corporate Actions)

- `ca_id`: Composite key (symbol:exchange:ex_date:purpose)
- `symbol`: From NSE CA SYMBOL
- `company_name`: From NSE CA COMPANY NAME
- `series`: From NSE CA SERIES (EQ, GS, BE, etc.)
- `purpose`: Raw text from NSE CA PURPOSE (parsed for action type)
- `action_type`: Enum parsed from PURPOSE (SPLIT, BONUS, DIVIDEND, RIGHTS, INTEREST_PAYMENT, EGMMEETING, etc.)
- `face_value`: From NSE CA FACE VALUE (Rs per share)
- `ex_date`: From NSE CA EX-DATE
- `record_date`: From NSE CA RECORD DATE
- `book_closure_start_date`, `book_closure_end_date`: From NSE CA columns
- `split_ratio`, `bonus_ratio`, `dividend_amount`, `rights_ratio`, `rights_price`: Parsed from PURPOSE
- `adjustment_factor`: Computed multiplicative factor (default 1.0)
- `raw_purpose`: Full PURPOSE text for audit and debugging

#### Adjustment Factor Computation (Corporate Actions)

**Split (10:1 to 1:1 consolidation)**: `adjustment_factor = 10` (old prices × 10)
**Split (1:10 sub-division)**: `adjustment_factor = 0.1` (old prices × 0.1)  
**Bonus (2:1 bonus)**: `adjustment_factor = 1/3 = 0.333` (1 existing + 2 bonus = 3 total)
**Dividend**: Typically no adjustment (ex-dividend drop is market-driven, but can be modeled as 0 for precise backtesting)
**Rights Issue**: Computed based on subscription price and rights ratio
**Interest Payments**: No price adjustment (GS instruments only)

```python
# Compute cumulative adjustment factor for RELIANCE from Jan 1, 2025 to present
ca_events = filter(ca_stream, instrument_id="RELIANCE:NSE", ex_date >= "2025-01-01")
cumulative_factor = reduce(lambda acc, ca: acc * ca.adjustment_factor, ca_events, 1.0)
adjusted_price = raw_price * cumulative_factor
```

### 3. `trading_calendar.avsc`

**Topic**: `reference.nse.trading_calendar`, `reference.bse.trading_calendar`  
**Owner**: Reference Data Domain  
**Update Pattern**: SCD Type 1 (upsert by exchange + trade_date)

#### Purpose (Trading Calendar)

- Identify trading days vs. holidays
- Validate `is_trading_day` flag in normalized OHLC
- Segment-specific trading hours (CM, FO, CD)
- Handle special sessions (muhurat trading, half-day)

#### Day Types

- **NORMAL_TRADING**: Regular trading day
- **WEEKEND**: Saturday/Sunday
- **PUBLIC_HOLIDAY**: National/state holiday
- **MARKET_HOLIDAY**: Exchange-specific holiday
- **SPECIAL_TRADING**: Special session (e.g., budget day extended hours)
- **HALF_DAY**: Shortened trading session
- **MUHURAT_TRADING**: Diwali special session

#### Key Fields (Trading Calendar)

- `exchange`: NSE, BSE
- `trade_date`: Date in question
- `is_trading_day`: Boolean flag
- `day_type`: Enum of day types
- `segments`: Array of segment-specific session timings
- `clearing_date`, `settlement_date`: T+1, T+2 dates

#### Usage (Trading Calendar)

```python
# Validate normalized OHLC against calendar
calendar_entry = lookup(exchange="NSE", trade_date="2026-01-26")
if not calendar_entry.is_trading_day:
    assert ohlc.payload.is_trading_day == False
else:
    # Validate OHLC exists
    assert ohlc is not None
```

### 4. `index_constituent.avsc`

**Topic**: `reference.nse.index_constituents`, `reference.bse.index_constituents`  
**Owner**: Reference Data Domain  
**Update Pattern**: Event-sourced (append-only for historical tracking)

#### Purpose (Index Constituents)

- Track index membership for NIFTY50, BANKNIFTY, and other major indices
- Record constituent additions, removals, and rebalances with effective dates
- Capture constituent weights (percentage) in index when available
- Support historical backtesting with accurate index composition at any date
- Enable index replication and tracking strategies

#### Key Fields (Index Constituents)

- `index_name`: Index identifier (e.g., 'NIFTY50', 'BANKNIFTY')
- `symbol`: Trading symbol (e.g., 'RELIANCE', 'HDFCBANK')
- `isin`: International Securities Identification Number
- `company_name`: Company name
- `effective_date`: Date when constituent change takes effect (days since epoch)
- `action`: Action type (ADD, REMOVE, REBALANCE)
- `weight`: Constituent weight in index (percentage, 0-100), if available
- `free_float_market_cap`: Free float market capitalization in INR crores
- `shares_for_index`: Number of shares used for index calculation
- `announcement_date`: Date when change was announced
- `index_category`: Category (e.g., 'Broad Market', 'Sectoral', 'Market Cap')
- `sector`: Sector classification
- `industry`: Industry classification

#### Usage (Index Constituents)

```python
# Get current NIFTY50 constituents
current_nifty50 = filter(
    index_constituent_stream,
    index_name="NIFTY50",
    action="ADD",
    effective_date=latest
)

# Track symbol across indices
reliance_indices = filter(
    index_constituent_stream,
    symbol="RELIANCE",
    action="ADD"
)

# Detect rebalance changes in a quarter
q1_changes = filter(
    index_constituent_stream,
    effective_date >= "2026-01-01",
    effective_date <= "2026-03-31",
    action in ["ADD", "REMOVE"]
)
```

#### ClickHouse Queries (Index Constituents)

```sql
-- Current NIFTY50 membership
SELECT symbol, company_name, weight, sector
FROM champion_market.index_constituent
WHERE index_name = 'NIFTY50'
  AND action = 'ADD'
ORDER BY weight DESC NULLS LAST;

-- Quarterly changes (adds/removes)
SELECT index_name, action, symbol, effective_date
FROM champion_market.index_constituent
WHERE effective_date >= today() - INTERVAL 90 DAY
  AND action IN ('ADD', 'REMOVE')
ORDER BY index_name, effective_date DESC;

-- Compare NIFTY50 vs BANKNIFTY
SELECT 
    n.symbol,
    n.weight as nifty50_weight,
    b.weight as banknifty_weight
FROM (
    SELECT symbol, weight
    FROM champion_market.index_constituent
    WHERE index_name = 'NIFTY50' AND action = 'ADD'
) n
INNER JOIN (
    SELECT symbol, weight
    FROM champion_market.index_constituent
    WHERE index_name = 'BANKNIFTY' AND action = 'ADD'
) b ON n.symbol = b.symbol;
```

## Kafka Topics

### Symbol Master (Kafka Topic)

- **Topic**: `reference.nse.symbol_master`
- **Key**: `instrument_id` (e.g., "RELIANCE:NSE")
- **Compaction**: Log compacted (retain latest per key)
- **Partitions**: 4 (low volume, ref data)

### Corporate Actions (Kafka Topic)

- **Topic**: `reference.nse.corporate_actions`
- **Key**: `instrument_id` (for ordering)
- **Compaction**: None (event-sourced, retain all)
- **Partitions**: 8 (partition by instrument_id for ordering guarantee)

### Trading Calendar (Kafka Topic)

- **Topic**: `reference.nse.trading_calendar`
- **Key**: `exchange:trade_date` (e.g., "NSE:2026-01-26")
- **Compaction**: Log compacted (one entry per exchange-date)
- **Partitions**: 2 (very low volume)

### Index Constituents (Kafka Topic)

- **Topic**: `reference.nse.index_constituents`
- **Key**: `index_name:symbol` (e.g., "NIFTY50:RELIANCE")
- **Compaction**: None (event-sourced, retain all historical changes)
- **Partitions**: 8 (partition by index_name for ordering guarantee)
- **Notes**: Historical tracking requires all events; use effective_date for time-travel queries

## Data Flow

```text
Exchange Website/API
        ↓
[Scraper/Ingestion Service]
        ↓
    Kafka Topics
        ↓
[Hudi Bronze Layer] (raw reference data)
        ↓
[Normalization Pipeline] (validate, deduplicate)
        ↓
[Hudi Gold Layer] (queryable reference tables)
        ↓
[ClickHouse/Pinot] (low-latency lookup)
```

## Data Quality Rules

### Symbol Master

- `instrument_id` must be unique per valid_from date
- `status` must be one of: ACTIVE, SUSPENDED, DELISTED
- `valid_from` <= `valid_to` (when valid_to is not null)
- `isin` format validation (12-character alphanumeric)
- `lot_size` > 0 when present

### Corporate Actions

- `ca_id` must be unique (symbol + exchange + ex_date + purpose)
- `ex_date` >= `announcement_date` (if both present)
- `record_date` >= `ex_date` (if both present)
- `book_closure_end_date` >= `book_closure_start_date` (if both present)
- `adjustment_factor` > 0 (except dividend where it may be 0 for modeling)
- `purpose` must be non-empty and parseable
- PURPOSE parsing rules:
  - Contains "Split" or "Sub-Division" → SPLIT (extract old:new ratio)
  - Contains "Bonus" → BONUS (extract ratio)
  - Contains "Dividend" or "Dividend Per Share" → DIVIDEND (extract amount, type)
  - Contains "Rights" → RIGHTS (extract ratio and price)
  - Contains "Interest Payment" → INTEREST_PAYMENT
  - Contains "General Meeting" or "EGM" → EGMMEETING
- `status` default: ANNOUNCED (updated to EXECUTED post ex-date)

### Trading Calendar

- `trade_date` must be unique per exchange
- If `is_trading_day` = true, at least one segment must have `is_open` = true
- Session times must be chronologically ordered: pre_open_start < pre_open_end < normal_open < normal_close
- `clearing_date` >= `trade_date`
- `settlement_date` >= `clearing_date`

## Storage Strategy

### Hudi Bronze (Raw)

- Write raw events as-is
- Partitioned by ingest_date
- Retention: 2 years

### Hudi Gold (Curated)

- **Symbol Master**: Current + historical versions (SCD Type 2)
- **Corporate Actions**: All events (event-sourced)
- **Trading Calendar**: Current + 5 years of history

### ClickHouse (OLAP Serving)

- **Symbol Master**: Materialized view of current (valid_to = null) entries
- **Corporate Actions**: Aggregated by instrument_id for quick adjustment factor computation
- **Trading Calendar**: Lookup table for fast is_trading_day checks

### Redis (Cache)

- **Symbol Master**: Hot cache of top 500 symbols
- **Trading Calendar**: Cache next 30 days
- TTL: 24 hours, refresh daily

## Next Steps

1. **Scraper Services**: Build NSE/BSE scrapers for reference data
2. **Validation Pipeline**: Flink job to validate reference data quality
3. **Lookup Service**: REST API for normalized pipeline to query reference data
4. **Backfill**: Historical symbol master and CA events for backtesting
5. **Monitoring**: Alerts for missing trading calendar entries, stale symbol master
