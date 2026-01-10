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
    FinInstrmId         Nullable(Int64),
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
    Rsvd1               Nullable(String),
    Rsvd2               Nullable(String),
    Rsvd3               Nullable(String),
    Rsvd4               Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(TradDt)
ORDER BY (TckrSymb, TradDt, event_time)
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
    
    -- Normalized payload
    instrument_id       String,
    symbol              String,
    exchange            LowCardinality(String),
    isin                Nullable(String),
    instrument_type     LowCardinality(Nullable(String)),
    series              LowCardinality(Nullable(String)),
    trade_date          Date,
    prev_close          Nullable(Float64),
    open                Float64,
    high                Float64,
    low                 Float64,
    close               Float64,
    last_price          Nullable(Float64),
    settlement_price    Nullable(Float64),
    volume              Int64,
    turnover            Float64,
    trades              Nullable(Int64),
    adjustment_factor   Float64 DEFAULT 1.0,
    adjustment_date     Nullable(Date),
    is_trading_day      Bool
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date, instrument_id)
TTL trade_date + INTERVAL 3 YEAR
SETTINGS 
    index_granularity = 8192;

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_norm_isin 
ON champion_market.normalized_equity_ohlc (isin) 
TYPE bloom_filter GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_norm_volume 
ON champion_market.normalized_equity_ohlc (volume) 
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
    trade_date,
    exchange,
    count() as total_symbols,
    sum(volume) as total_volume,
    sum(turnover) as total_turnover,
    avg(close) as avg_close_price,
    max(high) as max_high_price,
    min(low) as min_low_price
FROM champion_market.normalized_equity_ohlc
GROUP BY trade_date, exchange;

-- Note: User permissions are managed via environment variables and docker-entrypoint
-- No need for explicit GRANT statements here
