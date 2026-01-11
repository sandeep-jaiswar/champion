# Bulk and Block Deals - ClickHouse Verification Queries

This document contains SQL queries for verifying and analyzing bulk/block deals data in ClickHouse.

## Data Quality Checks

### 1. Count deals by type

```sql
SELECT 
    deal_type,
    COUNT(*) as total_deals,
    COUNT(DISTINCT symbol) as unique_symbols,
    COUNT(DISTINCT client_name) as unique_clients,
    SUM(quantity) as total_quantity,
    SUM(quantity * avg_price) as total_value
FROM champion_market.bulk_block_deals
WHERE deal_date >= '2026-01-01'
GROUP BY deal_type
ORDER BY deal_type;
```

### 2. Check for missing dates in a month

```sql
-- Get all trading days from calendar
WITH trading_days AS (
    SELECT trade_date
    FROM champion_market.trading_calendar
    WHERE year = 2026 
      AND month = 1
      AND is_trading_day = true
),
-- Get dates with deals
deal_days AS (
    SELECT DISTINCT deal_date
    FROM champion_market.bulk_block_deals
    WHERE deal_date >= '2026-01-01'
      AND deal_date < '2026-02-01'
)
-- Find missing dates
SELECT 
    trade_date,
    'Missing deals data' as status
FROM trading_days
WHERE trade_date NOT IN (SELECT deal_date FROM deal_days)
ORDER BY trade_date;
```

### 3. Daily summary statistics

```sql
SELECT 
    deal_date,
    deal_type,
    COUNT(*) as deals,
    COUNT(DISTINCT symbol) as symbols,
    COUNT(DISTINCT client_name) as clients,
    SUM(CASE WHEN transaction_type = 'BUY' THEN quantity ELSE 0 END) as total_buy_quantity,
    SUM(CASE WHEN transaction_type = 'SELL' THEN quantity ELSE 0 END) as total_sell_quantity,
    SUM(CASE WHEN transaction_type = 'BUY' THEN quantity * avg_price ELSE 0 END) as total_buy_value,
    SUM(CASE WHEN transaction_type = 'SELL' THEN quantity * avg_price ELSE 0 END) as total_sell_value,
    AVG(avg_price) as avg_deal_price,
    AVG(quantity) as avg_deal_quantity
FROM champion_market.bulk_block_deals
WHERE deal_date >= '2026-01-01'
GROUP BY deal_date, deal_type
ORDER BY deal_date DESC, deal_type;
```

## Analysis Queries

### 4. Top 10 bulk deals by quantity (sample month)

```sql
SELECT 
    deal_date,
    symbol,
    client_name,
    quantity,
    avg_price,
    quantity * avg_price as total_value,
    transaction_type
FROM champion_market.bulk_block_deals
WHERE deal_type = 'BULK'
  AND deal_date >= '2026-01-01'
  AND deal_date < '2026-02-01'
ORDER BY quantity DESC
LIMIT 10;
```

### 5. Top 10 block deals by value

```sql
SELECT 
    deal_date,
    symbol,
    client_name,
    quantity,
    avg_price,
    quantity * avg_price as total_value,
    transaction_type
FROM champion_market.bulk_block_deals
WHERE deal_type = 'BLOCK'
  AND deal_date >= '2026-01-01'
  AND deal_date < '2026-02-01'
ORDER BY total_value DESC
LIMIT 10;
```

### 6. Most active clients

```sql
SELECT 
    client_name,
    COUNT(*) as total_deals,
    COUNT(DISTINCT symbol) as symbols_traded,
    SUM(quantity) as total_quantity,
    SUM(quantity * avg_price) as total_value,
    AVG(avg_price) as avg_price,
    SUM(CASE WHEN deal_type = 'BULK' THEN 1 ELSE 0 END) as bulk_deals,
    SUM(CASE WHEN deal_type = 'BLOCK' THEN 1 ELSE 0 END) as block_deals
FROM champion_market.bulk_block_deals
WHERE deal_date >= '2026-01-01'
  AND deal_date < '2026-02-01'
GROUP BY client_name
ORDER BY total_deals DESC
LIMIT 20;
```

### 7. Most actively traded symbols in bulk/block deals

```sql
SELECT 
    symbol,
    COUNT(*) as total_deals,
    COUNT(DISTINCT client_name) as unique_clients,
    SUM(quantity) as total_quantity,
    SUM(quantity * avg_price) as total_value,
    AVG(avg_price) as avg_price,
    SUM(CASE WHEN transaction_type = 'BUY' THEN quantity ELSE 0 END) as buy_quantity,
    SUM(CASE WHEN transaction_type = 'SELL' THEN quantity ELSE 0 END) as sell_quantity,
    SUM(CASE WHEN deal_type = 'BULK' THEN 1 ELSE 0 END) as bulk_deals,
    SUM(CASE WHEN deal_type = 'BLOCK' THEN 1 ELSE 0 END) as block_deals
FROM champion_market.bulk_block_deals
WHERE deal_date >= '2026-01-01'
  AND deal_date < '2026-02-01'
GROUP BY symbol
ORDER BY total_quantity DESC
LIMIT 20;
```

### 8. Price analysis - compare with OHLC

```sql
-- Compare bulk/block deal prices with regular trading prices
SELECT 
    bbd.deal_date,
    bbd.symbol,
    bbd.avg_price as deal_price,
    bbd.quantity,
    bbd.deal_type,
    bbd.transaction_type,
    ohlc.OpnPric as open_price,
    ohlc.HghPric as high_price,
    ohlc.LwPric as low_price,
    ohlc.ClsPric as close_price,
    -- Price deviation
    (bbd.avg_price - ohlc.ClsPric) / ohlc.ClsPric * 100 as price_deviation_pct
FROM champion_market.bulk_block_deals bbd
LEFT JOIN champion_market.normalized_equity_ohlc ohlc
    ON bbd.symbol = ohlc.TckrSymb
    AND bbd.deal_date = ohlc.TradDt
WHERE bbd.deal_date >= '2026-01-01'
  AND bbd.deal_date < '2026-02-01'
ORDER BY ABS(price_deviation_pct) DESC
LIMIT 20;
```

### 9. Client trading patterns

```sql
-- Identify clients with both buy and sell activity
SELECT 
    client_name,
    symbol,
    COUNT(CASE WHEN transaction_type = 'BUY' THEN 1 END) as buy_count,
    COUNT(CASE WHEN transaction_type = 'SELL' THEN 1 END) as sell_count,
    SUM(CASE WHEN transaction_type = 'BUY' THEN quantity ELSE 0 END) as buy_quantity,
    SUM(CASE WHEN transaction_type = 'SELL' THEN quantity ELSE 0 END) as sell_quantity,
    AVG(CASE WHEN transaction_type = 'BUY' THEN avg_price END) as avg_buy_price,
    AVG(CASE WHEN transaction_type = 'SELL' THEN avg_price END) as avg_sell_price
FROM champion_market.bulk_block_deals
WHERE deal_date >= '2026-01-01'
  AND deal_date < '2026-02-01'
GROUP BY client_name, symbol
HAVING buy_count > 0 AND sell_count > 0
ORDER BY (buy_quantity + sell_quantity) DESC;
```

### 10. Time series analysis

```sql
-- Weekly aggregation
SELECT 
    toMonday(deal_date) as week_start,
    deal_type,
    COUNT(*) as deals,
    COUNT(DISTINCT symbol) as symbols,
    SUM(quantity) as total_quantity,
    SUM(quantity * avg_price) as total_value,
    AVG(avg_price) as avg_price
FROM champion_market.bulk_block_deals
WHERE deal_date >= '2026-01-01'
GROUP BY week_start, deal_type
ORDER BY week_start DESC, deal_type;
```

## Data Validation

### 11. Detect outliers

```sql
-- Find deals with unusually large quantities
WITH stats AS (
    SELECT 
        symbol,
        deal_type,
        AVG(quantity) as avg_quantity,
        stddevPop(quantity) as stddev_quantity
    FROM champion_market.bulk_block_deals
    WHERE deal_date >= '2025-12-01'
    GROUP BY symbol, deal_type
)
SELECT 
    bbd.deal_date,
    bbd.symbol,
    bbd.client_name,
    bbd.quantity,
    bbd.deal_type,
    stats.avg_quantity,
    (bbd.quantity - stats.avg_quantity) / stats.stddev_quantity as z_score
FROM champion_market.bulk_block_deals bbd
JOIN stats ON bbd.symbol = stats.symbol AND bbd.deal_type = stats.deal_type
WHERE ABS((bbd.quantity - stats.avg_quantity) / stats.stddev_quantity) > 3
  AND bbd.deal_date >= '2026-01-01'
ORDER BY ABS(z_score) DESC;
```

### 12. Data completeness check

```sql
-- Check for missing or null fields
SELECT 
    'Missing symbols' as check_type,
    COUNT(*) as count
FROM champion_market.bulk_block_deals
WHERE symbol = '' OR symbol IS NULL

UNION ALL

SELECT 
    'Missing client names',
    COUNT(*)
FROM champion_market.bulk_block_deals
WHERE client_name = '' OR client_name IS NULL

UNION ALL

SELECT 
    'Zero quantities',
    COUNT(*)
FROM champion_market.bulk_block_deals
WHERE quantity = 0

UNION ALL

SELECT 
    'Zero prices',
    COUNT(*)
FROM champion_market.bulk_block_deals
WHERE avg_price = 0

UNION ALL

SELECT 
    'Negative quantities',
    COUNT(*)
FROM champion_market.bulk_block_deals
WHERE quantity < 0

UNION ALL

SELECT 
    'Negative prices',
    COUNT(*)
FROM champion_market.bulk_block_deals
WHERE avg_price < 0;
```

## Export Queries

### 13. Export for manual verification

```sql
-- Export sample for cross-checking with NSE website
SELECT 
    deal_date,
    symbol,
    client_name,
    quantity,
    avg_price,
    quantity * avg_price as total_value,
    deal_type,
    transaction_type
FROM champion_market.bulk_block_deals
WHERE deal_date = '2026-01-10'
ORDER BY quantity DESC
LIMIT 50
FORMAT CSV;
```

### 14. Monthly report

```sql
-- Generate monthly summary report
SELECT 
    toStartOfMonth(deal_date) as month,
    deal_type,
    transaction_type,
    COUNT(*) as total_deals,
    COUNT(DISTINCT symbol) as unique_symbols,
    COUNT(DISTINCT client_name) as unique_clients,
    SUM(quantity) as total_quantity,
    SUM(quantity * avg_price) as total_value,
    MIN(avg_price) as min_price,
    MAX(avg_price) as max_price,
    AVG(avg_price) as avg_price,
    MIN(quantity) as min_quantity,
    MAX(quantity) as max_quantity,
    AVG(quantity) as avg_quantity
FROM champion_market.bulk_block_deals
WHERE deal_date >= '2026-01-01'
GROUP BY month, deal_type, transaction_type
ORDER BY month DESC, deal_type, transaction_type;
```

## Notes

- All queries assume data is available for January 2026
- Adjust date ranges as needed for your verification
- Some queries join with other tables (trading_calendar, normalized_equity_ohlc) - ensure those tables have data
- Use `LIMIT` clause to avoid returning too many rows in exploratory queries
