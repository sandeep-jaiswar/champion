-- ClickHouse DDL Schema for Champion Market Data Platform
-- This script creates database and tables for raw, normalized, and features layers

-- Create database
CREATE DATABASE IF NOT EXISTS champion_market;

-- ==============================================================================
-- 1. RAW EQUITY OHLC TABLE
-- ==============================================================================
-- Purpose: Store raw NSE bhavcopy data for audit trail and replay
-- Retention: 5 years
-- Partitioning: Monthly (YYYYMM)
-- Sort Key: Symbol, TradDt, event_time for efficient queries

CREATE TABLE IF NOT EXISTS champion_market.raw_equity_ohlc
(
    -- Envelope fields (metadata)
    event_id            String,
    event_time          DateTime64(3, 'UTC'),
    ingest_time         DateTime64(3, 'UTC'),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- NSE BhavCopy payload fields (raw format)
    TradDt              Date,
    BizDt               Nullable(Date),
    Sgmt                LowCardinality(Nullable(String)),
    Src                 LowCardinality(Nullable(String)),
    FinInstrmTp         LowCardinality(Nullable(String)),
    FinInstrmId         Int64 DEFAULT 0,
    ISIN                Nullable(String),
    TckrSymb            String DEFAULT '',  -- Not nullable since it's in ORDER BY
    SctySrs             LowCardinality(Nullable(String)),
    XpryDt              Nullable(Date),
    FininstrmActlXpryDt Nullable(Date),
    StrkPric            Nullable(Float64),
    OptnTp              LowCardinality(Nullable(String)),
    FinInstrmNm         Nullable(String),
    OpnPric             Nullable(Float64),
    HghPric             Nullable(Float64),
    LwPric              Nullable(Float64),
    ClsPric             Nullable(Float64),
    LastPric            Nullable(Float64),
    PrvsClsgPric        Nullable(Float64),
    UndrlygPric         Nullable(Float64),
    SttlmPric           Nullable(Float64),
    OpnIntrst           Nullable(Int64),
    ChngInOpnIntrst     Nullable(Int64),
    TtlTradgVol         Nullable(Int64),
    TtlTrfVal           Nullable(Float64),
    TtlNbOfTxsExctd     Nullable(Int64),
    SsnId               Nullable(String),
    NewBrdLotQty        Nullable(Int64),
    Rmks                Nullable(String),
    Rsvd01              Nullable(String),
    Rsvd02              Nullable(String),
    Rsvd03              Nullable(String),
    Rsvd04              Nullable(String),
    year                Int64 DEFAULT toYear(TradDt),
    month               Int64 DEFAULT toMonth(TradDt),
    day                 Int64 DEFAULT toDayOfMonth(TradDt)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(TradDt)
ORDER BY (TckrSymb, FinInstrmId, TradDt, event_time)
TTL TradDt + INTERVAL 5 YEAR
SETTINGS 
    index_granularity = 8192;

-- Index for faster ISIN lookups
CREATE INDEX IF NOT EXISTS idx_raw_isin 
ON champion_market.raw_equity_ohlc (ISIN) 
TYPE bloom_filter GRANULARITY 4;

-- ==============================================================================
-- 2. NORMALIZED EQUITY OHLC TABLE
-- ==============================================================================
-- Purpose: Clean, CA-adjusted equity data for analytics
-- Retention: 3 years
-- Partitioning: Monthly (YYYYMM)
-- Engine: ReplacingMergeTree for upserts with late-arriving data

CREATE TABLE IF NOT EXISTS champion_market.normalized_equity_ohlc
(
    -- Envelope fields (metadata)
    event_id            String,
    event_time          DateTime64(3, 'UTC'),
    ingest_time         DateTime64(3, 'UTC'),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,

    -- Normalized payload (currently schema-aligned with lake files)
    TradDt              Date,
    BizDt               Nullable(Date),
    Sgmt                LowCardinality(Nullable(String)),
    Src                 LowCardinality(Nullable(String)),
    FinInstrmTp         LowCardinality(Nullable(String)),
    FinInstrmId         Int64 DEFAULT 0,
    ISIN                Nullable(String),
    TckrSymb            String DEFAULT '',
    SctySrs             LowCardinality(Nullable(String)),
    XpryDt              Nullable(Date),
    FininstrmActlXpryDt Nullable(Date),
    StrkPric            Nullable(Float64),
    OptnTp              LowCardinality(Nullable(String)),
    FinInstrmNm         Nullable(String),
    OpnPric             Nullable(Float64),
    HghPric             Nullable(Float64),
    LwPric              Nullable(Float64),
    ClsPric             Nullable(Float64),
    LastPric            Nullable(Float64),
    PrvsClsgPric        Nullable(Float64),
    UndrlygPric         Nullable(Float64),
    SttlmPric           Nullable(Float64),
    OpnIntrst           Nullable(Int64),
    ChngInOpnIntrst     Nullable(Int64),
    TtlTradgVol         Nullable(Int64),
    TtlTrfVal           Nullable(Float64),
    TtlNbOfTxsExctd     Nullable(Int64),
    SsnId               Nullable(String),
    NewBrdLotQty        Nullable(Int64),
    Rmks                Nullable(String),
    Rsvd01              Nullable(String),
    Rsvd02              Nullable(String),
    Rsvd03              Nullable(String),
    Rsvd04              Nullable(String),
    year                Int64 DEFAULT toYear(TradDt),
    month               Int64 DEFAULT toMonth(TradDt),
    day                 Int64 DEFAULT toDayOfMonth(TradDt)
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(TradDt)
ORDER BY (TckrSymb, FinInstrmId, TradDt, event_time)
TTL TradDt + INTERVAL 3 YEAR
SETTINGS 
    index_granularity = 8192;

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_norm_isin 
ON champion_market.normalized_equity_ohlc (ISIN) 
TYPE bloom_filter GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_norm_volume 
ON champion_market.normalized_equity_ohlc (TtlTradgVol) 
TYPE minmax GRANULARITY 1;

-- ==============================================================================
-- 3. FEATURES EQUITY INDICATORS TABLE
-- ==============================================================================
-- Purpose: Store computed technical indicators for ML and analytics
-- Retention: 1 year
-- Partitioning: Monthly (YYYYMM)

CREATE TABLE IF NOT EXISTS champion_market.features_equity_indicators
(
    -- Metadata
    symbol              String,
    trade_date          Date,
    feature_timestamp   DateTime64(3, 'UTC'),
    feature_version     LowCardinality(String),
    
    -- Moving averages
    sma_5               Nullable(Float64),
    sma_10              Nullable(Float64),
    sma_20              Nullable(Float64),
    sma_50              Nullable(Float64),
    sma_100             Nullable(Float64),
    sma_200             Nullable(Float64),
    ema_12              Nullable(Float64),
    ema_26              Nullable(Float64),
    ema_50              Nullable(Float64),
    
    -- Momentum indicators
    rsi_14              Nullable(Float64),
    macd                Nullable(Float64),
    macd_signal         Nullable(Float64),
    macd_histogram      Nullable(Float64),
    stochastic_k        Nullable(Float64),
    stochastic_d        Nullable(Float64),
    
    -- Volatility indicators
    bb_upper            Nullable(Float64),
    bb_middle           Nullable(Float64),
    bb_lower            Nullable(Float64),
    bb_width            Nullable(Float64),
    atr_14              Nullable(Float64),
    
    -- Volume indicators
    vwap                Nullable(Float64),
    obv                 Nullable(Int64),
    
    -- Computed timestamp
    computed_at         DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date, feature_timestamp)
TTL trade_date + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192;

-- ==============================================================================
-- MATERIALIZED VIEWS (Optional - for analytics)
-- ==============================================================================

-- Daily OHLC Summary View
CREATE MATERIALIZED VIEW IF NOT EXISTS champion_market.equity_ohlc_daily_summary
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date, exchange)
AS SELECT
    TradDt as trade_date,
    coalesce(Sgmt, '') as exchange,
    count() as total_symbols,
    sum(coalesce(TtlTradgVol, 0)) as total_volume,
    sum(coalesce(TtlTrfVal, 0.0)) as total_turnover,
    avg(coalesce(ClsPric, 0.0)) as avg_close_price,
    max(coalesce(HghPric, 0.0)) as max_high_price,
    min(coalesce(LwPric, 0.0)) as min_low_price
FROM champion_market.normalized_equity_ohlc
GROUP BY TradDt, coalesce(Sgmt, '');

-- ==============================================================================
-- 4. CORPORATE ACTIONS TABLE
-- ==============================================================================
-- Purpose: Store corporate action events for price adjustment computation
-- Retention: 10 years (historical reference)
-- Partitioning: Yearly (YYYY)

CREATE TABLE IF NOT EXISTS champion_market.corporate_actions
(
    -- Envelope fields (metadata)
    event_id            String,
    event_time          DateTime64(3, 'UTC'),
    ingest_time         DateTime64(3, 'UTC'),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- Corporate action payload
    ca_id               String,
    instrument_id       String,
    symbol              String DEFAULT '',
    exchange            LowCardinality(String) DEFAULT 'NSE',
    company_name        Nullable(String),
    series              LowCardinality(Nullable(String)),
    isin                Nullable(String),
    purpose             String,
    action_type         LowCardinality(String),
    
    -- Dates
    ex_date             Date,
    record_date         Nullable(Date),
    book_closure_start  Nullable(Date),
    book_closure_end    Nullable(Date),
    announcement_date   Nullable(Date),
    
    -- Ratios and amounts
    face_value          Nullable(Float64),
    split_old_shares    Nullable(Int64),
    split_new_shares    Nullable(Int64),
    bonus_new_shares    Nullable(Int64),
    bonus_existing      Nullable(Int64),
    dividend_amount     Nullable(Float64),
    dividend_type       LowCardinality(Nullable(String)),
    rights_shares       Nullable(Int64),
    rights_existing     Nullable(Int64),
    rights_price        Nullable(Float64),
    
    -- Adjustment
    adjustment_factor   Float64 DEFAULT 1.0,
    status              LowCardinality(String) DEFAULT 'ANNOUNCED',
    
    -- Metadata
    raw_purpose         String,
    year                Int64 DEFAULT toYear(ex_date),
    month               Int64 DEFAULT toMonth(ex_date),
    day                 Int64 DEFAULT toDayOfMonth(ex_date)
)
ENGINE = MergeTree()
PARTITION BY toYear(ex_date)
ORDER BY (symbol, ex_date, action_type, event_time)
TTL ex_date + INTERVAL 10 YEAR
SETTINGS 
    index_granularity = 8192;

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_ca_isin 
ON champion_market.corporate_actions (isin) 
TYPE bloom_filter GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_ca_type 
ON champion_market.corporate_actions (action_type) 
TYPE set(100) GRANULARITY 1;

-- ==============================================================================
-- 5. SYMBOL MASTER TABLE
-- ==============================================================================
-- Purpose: Canonical instrument master for validation and enrichment
--          Maps NSE TckrSymb + SctySrs + FinInstrmId + ISIN to canonical instrument IDs
-- Retention: 10 years (historical reference)
-- Partitioning: By year of valid_from date
-- This resolves one-to-many ticker cases (e.g., IBULHSGFIN with EQ + 18 NCD tranches)

CREATE TABLE IF NOT EXISTS champion_market.symbol_master
(
    -- Envelope fields (metadata)
    event_id            String,
    event_time          DateTime64(3, 'UTC'),
    ingest_time         DateTime64(3, 'UTC'),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- Canonical identifiers
    instrument_id       String,  -- Primary key: symbol:exchange or symbol:fiid:exchange
    symbol              String DEFAULT '',  -- Trading symbol (TckrSymb)
    exchange            LowCardinality(String) DEFAULT 'NSE',
    
    -- Company information
    company_name        Nullable(String),
    isin                Nullable(String),
    series              LowCardinality(Nullable(String)),  -- Security series (EQ, BE, GB, etc.)
    
    -- Listing details
    listing_date        Nullable(Date),
    face_value          Nullable(Float64),
    paid_up_value       Nullable(Float64),
    lot_size            Nullable(Int64),
    
    -- Classification (for enrichment)
    sector              LowCardinality(Nullable(String)),
    industry            LowCardinality(Nullable(String)),
    market_cap_category LowCardinality(Nullable(String)),  -- LARGE_CAP, MID_CAP, SMALL_CAP
    
    -- Trading details
    tick_size           Nullable(Float64),
    is_index_constituent Nullable(Bool),
    indices             Array(String),  -- List of indices (e.g., ['NIFTY50', 'NIFTYBANK'])
    
    -- Status
    status              LowCardinality(String) DEFAULT 'ACTIVE',  -- ACTIVE, SUSPENDED, DELISTED
    delisting_date      Nullable(Date),
    
    -- Additional metadata
    metadata            Map(String, String),  -- Flexible metadata as key-value pairs
    
    -- Temporal validity
    valid_from          Date,  -- Effective date for this version
    valid_to            Nullable(Date),  -- Expiry date (NULL = current version)
    
    -- Computed partition columns
    year                Int64 DEFAULT toYear(valid_from)
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYear(valid_from)
ORDER BY (symbol, instrument_id, valid_from, event_time)
TTL valid_from + INTERVAL 10 YEAR
SETTINGS 
    index_granularity = 8192;

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_sm_isin 
ON champion_market.symbol_master (isin) 
TYPE bloom_filter GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_sm_status 
ON champion_market.symbol_master (status) 
TYPE set(10) GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_sm_series 
ON champion_market.symbol_master (series) 
TYPE set(100) GRANULARITY 1;

-- ==============================================================================
-- 6. TRADING CALENDAR TABLE
-- ==============================================================================
-- Purpose: Trading calendar reference data for validation and date calculations
-- Retention: 10 years (historical reference)
-- Partitioning: Yearly (YYYY)

CREATE TABLE IF NOT EXISTS champion_market.trading_calendar
(
    -- Metadata
    trade_date          Date,
    is_trading_day      Bool,
    day_type            LowCardinality(String),  -- NORMAL_TRADING, WEEKEND, MARKET_HOLIDAY, etc.
    holiday_name        Nullable(String),
    exchange            LowCardinality(String) DEFAULT 'NSE',
    
    -- Partition columns
    year                Int64 DEFAULT toYear(trade_date),
    month               Int64 DEFAULT toMonth(trade_date),
    day                 Int64 DEFAULT toDayOfMonth(trade_date),
    weekday             Int8 DEFAULT toDayOfWeek(trade_date),
    
    -- Timestamp
    ingest_time         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYear(trade_date)
ORDER BY (exchange, trade_date)
TTL trade_date + INTERVAL 10 YEAR
SETTINGS 
    index_granularity = 8192;

-- Index for fast trading day lookups
CREATE INDEX IF NOT EXISTS idx_tc_trading_day 
ON champion_market.trading_calendar (is_trading_day) 
TYPE set(2) GRANULARITY 1;

-- Index for holiday lookups
CREATE INDEX IF NOT EXISTS idx_tc_day_type 
ON champion_market.trading_calendar (day_type) 
TYPE set(10) GRANULARITY 1;

-- Note: User permissions are managed via environment variables and docker-entrypoint
-- No need for explicit GRANT statements here
