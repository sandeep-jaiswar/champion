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
-- 6. INDEX CONSTITUENT TABLE
-- ==============================================================================
-- Purpose: Track index membership, rebalances, and constituent weights
-- Retention: 10 years (historical reference for backtesting)
-- Partitioning: By index_name and year of effective_date
-- Engine: MergeTree for full historical tracking

CREATE TABLE IF NOT EXISTS champion_market.index_constituent
(
    -- Envelope fields (metadata)
    event_id            String,
    event_time          DateTime64(3, 'UTC'),
    ingest_time         DateTime64(3, 'UTC'),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- Index constituent payload
    index_name          LowCardinality(String),
    symbol              String,  -- Required field, no default
    isin                Nullable(String),
    company_name        Nullable(String),
    effective_date      Date,
    action              LowCardinality(String),  -- ADD, REMOVE, REBALANCE
    weight              Nullable(Float64),
    free_float_market_cap Nullable(Float64),
    shares_for_index    Nullable(Int64),
    announcement_date   Nullable(Date),
    index_category      LowCardinality(Nullable(String)),
    sector              LowCardinality(Nullable(String)),
    industry            LowCardinality(Nullable(String)),
    metadata            Map(String, String),
    
    -- Computed partition columns
    year                Int64 DEFAULT toYear(effective_date),
    month               Int64 DEFAULT toMonth(effective_date),
    day                 Int64 DEFAULT toDayOfMonth(effective_date)
)
ENGINE = MergeTree()
PARTITION BY (index_name, toYear(effective_date))
ORDER BY (index_name, symbol, effective_date, event_time)
TTL effective_date + INTERVAL 10 YEAR
SETTINGS 
    index_granularity = 8192;

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_ic_isin 
ON champion_market.index_constituent (isin) 
TYPE bloom_filter GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_ic_action 
ON champion_market.index_constituent (action) 
TYPE set(10) GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_ic_effective_date 
ON champion_market.index_constituent (effective_date) 
TYPE minmax GRANULARITY 1;

-- 7. TRADING CALENDAR TABLE
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

-- ==============================================================================
-- 8. BULK AND BLOCK DEALS TABLE
-- ==============================================================================
-- Purpose: Track bulk and block deals for transparency and analysis
-- Retention: 10 years (regulatory and analysis reference)
-- Partitioning: By deal_type and year/month of deal_date
--
-- Bulk Deals: Transactions where total quantity > 0.5% of listed shares
-- Block Deals: Transactions with minimum 5 lakh shares or Rs 5 crore value

CREATE TABLE IF NOT EXISTS champion_market.bulk_block_deals
(
    -- Envelope fields (metadata)
    event_id            String,
    event_time          DateTime64(3, 'UTC'),
    ingest_time         DateTime64(3, 'UTC'),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- Deal payload
    deal_date           Date,
    symbol              String,  -- Trading symbol
    client_name         String,  -- Client/entity name
    quantity            Int64,   -- Number of shares traded
    avg_price           Float64, -- Average deal price
    deal_type           LowCardinality(String),  -- BULK or BLOCK
    transaction_type    LowCardinality(String),  -- BUY or SELL
    exchange            LowCardinality(String) DEFAULT 'NSE',
    
    -- Computed partition columns
    year                Int64 DEFAULT toYear(deal_date),
    month               Int64 DEFAULT toMonth(deal_date),
    day                 Int64 DEFAULT toDayOfMonth(deal_date)
)
ENGINE = MergeTree()
PARTITION BY (deal_type, toYear(deal_date), toMonth(deal_date))
ORDER BY (symbol, deal_date, transaction_type, event_time)
TTL deal_date + INTERVAL 10 YEAR
SETTINGS 
    index_granularity = 8192;

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_bbd_client 
ON champion_market.bulk_block_deals (client_name) 
TYPE bloom_filter GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_bbd_deal_type 
ON champion_market.bulk_block_deals (deal_type) 
TYPE set(2) GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_bbd_transaction_type 
ON champion_market.bulk_block_deals (transaction_type) 
TYPE set(2) GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_bbd_quantity 
ON champion_market.bulk_block_deals (quantity) 
TYPE minmax GRANULARITY 1;

-- ==============================================================================
-- 9. QUARTERLY FINANCIALS TABLE
-- ==============================================================================
-- Purpose: Store quarterly/annual financial statements from MCA filings
-- Retention: 10 years (historical reference for analysis)
-- Partitioning: By year and quarter of period_end_date

CREATE TABLE IF NOT EXISTS champion_market.quarterly_financials
(
    -- Envelope fields (metadata)
    event_id            String,
    event_time          DateTime64(3, 'UTC'),
    ingest_time         DateTime64(3, 'UTC'),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- Company identifiers
    symbol              String DEFAULT '',
    company_name        Nullable(String),
    cin                 Nullable(String),
    
    -- Period information
    period_end_date     Date,
    period_type         LowCardinality(String),  -- QUARTERLY, HALF_YEARLY, ANNUAL
    statement_type      LowCardinality(String),  -- STANDALONE, CONSOLIDATED
    filing_date         Nullable(Date),
    
    -- Profit & Loss items (in crores)
    revenue             Nullable(Float64),
    operating_profit    Nullable(Float64),
    net_profit          Nullable(Float64),
    depreciation        Nullable(Float64),
    interest_expense    Nullable(Float64),
    tax_expense         Nullable(Float64),
    
    -- Balance sheet items (in crores)
    total_assets        Nullable(Float64),
    total_liabilities   Nullable(Float64),
    equity              Nullable(Float64),
    total_debt          Nullable(Float64),
    current_assets      Nullable(Float64),
    current_liabilities Nullable(Float64),
    cash_and_equivalents Nullable(Float64),
    inventories         Nullable(Float64),
    
    -- Per share metrics
    eps                 Nullable(Float64),
    book_value_per_share Nullable(Float64),
    
    -- Computed ratios
    roe                 Nullable(Float64),
    roa                 Nullable(Float64),
    debt_to_equity      Nullable(Float64),
    current_ratio       Nullable(Float64),
    operating_margin    Nullable(Float64),
    net_margin          Nullable(Float64),
    
    -- Additional metadata
    metadata            Map(String, String),
    
    -- Computed partition columns
    year                Int64 DEFAULT toYear(period_end_date),
    quarter             Int64 DEFAULT toQuarter(period_end_date)
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY (year, quarter)
ORDER BY (symbol, period_end_date, statement_type, event_time)
TTL period_end_date + INTERVAL 10 YEAR
SETTINGS 
    index_granularity = 8192;

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_qf_cin 
ON champion_market.quarterly_financials (cin) 
TYPE bloom_filter GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_qf_period_type 
ON champion_market.quarterly_financials (period_type) 
TYPE set(10) GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_qf_statement_type 
ON champion_market.quarterly_financials (statement_type) 
TYPE set(10) GRANULARITY 1;

-- ==============================================================================
-- 10. SHAREHOLDING PATTERN TABLE
-- ==============================================================================
-- Purpose: Store shareholding pattern data from BSE/NSE disclosures
-- Retention: 10 years (historical reference for analysis)
-- Partitioning: By year and quarter of quarter_end_date

CREATE TABLE IF NOT EXISTS champion_market.shareholding_pattern
(
    -- Envelope fields (metadata)
    event_id            String,
    event_time          DateTime64(3, 'UTC'),
    ingest_time         DateTime64(3, 'UTC'),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- Company identifiers
    symbol              String DEFAULT '',
    company_name        Nullable(String),
    scrip_code          Nullable(String),
    isin                Nullable(String),
    
    -- Period information
    quarter_end_date    Date,
    filing_date         Nullable(Date),
    
    -- Promoter shareholding
    promoter_shareholding_percent Nullable(Float64),
    promoter_shares     Nullable(Int64),
    pledged_promoter_shares_percent Nullable(Float64),
    pledged_promoter_shares Nullable(Int64),
    
    -- Public shareholding
    public_shareholding_percent Nullable(Float64),
    public_shares       Nullable(Int64),
    
    -- Institutional shareholding
    institutional_shareholding_percent Nullable(Float64),
    institutional_shares Nullable(Int64),
    fii_shareholding_percent Nullable(Float64),
    fii_shares          Nullable(Int64),
    dii_shareholding_percent Nullable(Float64),
    dii_shares          Nullable(Int64),
    
    -- Specific institutional categories
    mutual_fund_shareholding_percent Nullable(Float64),
    mutual_fund_shares  Nullable(Int64),
    insurance_companies_percent Nullable(Float64),
    insurance_companies_shares Nullable(Int64),
    banks_shareholding_percent Nullable(Float64),
    banks_shares        Nullable(Int64),
    
    -- Employee shareholding
    employee_shareholding_percent Nullable(Float64),
    employee_shares     Nullable(Int64),
    
    -- Total shares
    total_shares_outstanding Nullable(Int64),
    
    -- Additional metadata
    metadata            Map(String, String),
    
    -- Computed partition columns
    year                Int64 DEFAULT toYear(quarter_end_date),
    quarter             Int64 DEFAULT toQuarter(quarter_end_date)
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY (year, quarter)
ORDER BY (symbol, quarter_end_date, event_time)
TTL quarter_end_date + INTERVAL 10 YEAR
SETTINGS 
    index_granularity = 8192;

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_sp_isin 
ON champion_market.shareholding_pattern (isin) 
TYPE bloom_filter GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_sp_scrip_code 
ON champion_market.shareholding_pattern (scrip_code) 
TYPE bloom_filter GRANULARITY 4;

-- ==============================================================================
-- 11. MACRO INDICATORS TABLE
-- ==============================================================================
-- Purpose: Store macroeconomic time series data from RBI, MOSPI, and other sources
-- Retention: 20 years (long-term economic analysis)
-- Partitioning: By indicator_category and year of indicator_date
-- Includes: Policy rates, CPI, WPI, FX reserves, GDP, employment metrics

CREATE TABLE IF NOT EXISTS champion_market.macro_indicators
(
    -- Envelope fields (metadata)
    event_id            String,
    event_time          DateTime64(3, 'UTC'),
    ingest_time         DateTime64(3, 'UTC'),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- Macro indicator payload
    indicator_date      Date,
    indicator_code      LowCardinality(String),  -- e.g., REPO_RATE, CPI_COMBINED, WPI_ALL
    indicator_name      String,
    indicator_category  LowCardinality(String),  -- POLICY_RATE, INFLATION, FX_RESERVE, GDP, EMPLOYMENT
    value               Float64,
    unit                LowCardinality(String),  -- %, INR Crore, USD Million, Index Points
    frequency           LowCardinality(String),  -- DAILY, WEEKLY, MONTHLY, QUARTERLY, ANNUAL
    source_url          Nullable(String),
    metadata            Map(String, String),
    
    -- Computed partition columns
    year                Int64 DEFAULT toYear(indicator_date),
    month               Int64 DEFAULT toMonth(indicator_date),
    quarter             Int64 DEFAULT toQuarter(indicator_date)
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY (indicator_category, toYear(indicator_date))
ORDER BY (indicator_code, indicator_date, event_time)
TTL indicator_date + INTERVAL 20 YEAR
SETTINGS 
    index_granularity = 8192;

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_mi_code 
ON champion_market.macro_indicators (indicator_code) 
TYPE set(100) GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_mi_category 
ON champion_market.macro_indicators (indicator_category) 
TYPE set(20) GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_mi_frequency 
ON champion_market.macro_indicators (frequency) 
TYPE set(10) GRANULARITY 1;

CREATE INDEX IF NOT EXISTS idx_mi_date 
ON champion_market.macro_indicators (indicator_date) 
TYPE minmax GRANULARITY 1;

-- Note: User permissions are managed via environment variables and docker-entrypoint
-- No need for explicit GRANT statements here
