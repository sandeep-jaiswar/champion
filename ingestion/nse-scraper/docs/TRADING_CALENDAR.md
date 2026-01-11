# Trading Calendar

NSE trading calendar ingestion and validation utilities.

## Overview

The trading calendar module provides:

- **Scraping**: Fetch NSE holiday calendar from API or generate minimal calendar
- **Parsing**: Generate complete year calendar with trading/non-trading day information
- **Storage**: Store in Parquet (data lake) and ClickHouse (warehouse)
- **Validation**: Utilities to check trading days and compute date offsets

## Features

### Data Captured

- Date (trade_date)
- Trading day flag (is_trading_day)
- Day type (NORMAL_TRADING, WEEKEND, MARKET_HOLIDAY, etc.)
- Holiday name (if applicable)
- Exchange (NSE)
- Year/Month/Day partitioning

### Coverage

- 365 days per year (complete calendar)
- ~251 trading days per year
- ~97 weekend days
- ~17 market holidays (varies by year)

## Usage

### Command Line

Run ETL for current year:

```bash
cd ingestion/nse-scraper
python run_trading_calendar.py
```

Run for specific year:

```bash
python run_trading_calendar.py --year 2026
```

Skip ClickHouse loading:

```bash
python run_trading_calendar.py --no-clickhouse
```

### Python API

#### Scraper

```python
from src.scrapers.trading_calendar import TradingCalendarScraper

scraper = TradingCalendarScraper()
json_path = scraper.scrape(year=2026)
scraper.close()
```

#### Parser

```python
from src.parsers.trading_calendar_parser import TradingCalendarParser

parser = TradingCalendarParser()
df = parser.parse(json_path, year=2026)
print(f"Trading days: {df.filter(df['is_trading_day']).count()}")
```

#### Validator

```python
from datetime import date
from src.utils.trading_calendar import TradingCalendarValidator

# Load calendar
validator = TradingCalendarValidator("path/to/calendar.parquet")

# Check if date is trading day
is_trading = validator.is_trading_day(date(2026, 1, 26))  # False (Republic Day)

# Get next trading day
next_day = validator.get_next_trading_day(date(2026, 1, 26))  # 2026-01-27

# Get previous trading day
prev_day = validator.get_previous_trading_day(date(2026, 1, 26))  # 2026-01-23

# Count trading days in month
count = validator.count_trading_days_in_month(2026, 1)  # 21

# Get holidays in range
holidays = validator.get_holidays_in_range(
    date(2026, 1, 1),
    date(2026, 12, 31)
)
```

#### Convenience Functions

```python
from datetime import date
from src.utils.trading_calendar import (
    is_trading_day,
    get_next_trading_day,
    get_previous_trading_day
)

# Uses global validator instance
if is_trading_day(date(2026, 1, 2)):
    print("Market is open!")

next_day = get_next_trading_day(date(2026, 1, 26))
```

## Data Flow

```text
NSE API
  ↓
Scraper (JSON)
  ↓
Parser (complete year calendar)
  ↓
Parquet (data lake)
  ↓
ClickHouse (warehouse)
  ↓
Validation utilities
```

## Storage

### Parquet

Location: `data/lake/reference/trading_calendar/year={YYYY}/`

Files: `trading_calendar_{YYYY}.parquet`

Partitioning: By year

### ClickHouse

Table: `champion_market.trading_calendar`

Partitioning: By year

Indexes:
- `is_trading_day` (set index)
- `day_type` (set index)

## ClickHouse Queries

### Count trading days per month

```sql
SELECT 
    year,
    month,
    SUM(CASE WHEN is_trading_day THEN 1 ELSE 0 END) as trading_days,
    COUNT(*) as total_days
FROM champion_market.trading_calendar
WHERE year = 2026
GROUP BY year, month
ORDER BY year, month;
```

### List all holidays

```sql
SELECT 
    trade_date,
    holiday_name,
    day_type
FROM champion_market.trading_calendar
WHERE day_type = 'MARKET_HOLIDAY'
    AND year = 2026
ORDER BY trade_date;
```

### Check if specific date is trading day

```sql
SELECT 
    trade_date,
    is_trading_day,
    day_type,
    holiday_name
FROM champion_market.trading_calendar
WHERE trade_date = '2026-01-26';
```

### Get next N trading days after a date

```sql
SELECT 
    trade_date
FROM champion_market.trading_calendar
WHERE trade_date > '2026-01-26'
    AND is_trading_day = true
ORDER BY trade_date
LIMIT 5;
```

## Scheduling

### Prefect Flow

The trading calendar flow can be scheduled to run monthly:

```python
from prefect.deployments import Deployment
from src.orchestration.trading_calendar_flow import trading_calendar_etl_flow

deployment = Deployment.build_from_flow(
    flow=trading_calendar_etl_flow,
    name="nse-trading-calendar-monthly",
    schedule="0 0 1 * *",  # First day of each month
    parameters={"year": None, "load_to_clickhouse": True}
)

deployment.apply()
```

## Testing

Run manual tests:

```bash
cd ingestion/nse-scraper

# Test all components
python tests/manual/test_trading_calendar.py

# Test specific component
python tests/manual/test_trading_calendar.py scraper
python tests/manual/test_trading_calendar.py parser
python tests/manual/test_trading_calendar.py validator
```

## NSE Holiday Calendar Structure

The NSE API returns data in this format:

```json
{
  "CM": [
    {
      "tradingDate": "26-Jan-2026",
      "weekDay": "Monday",
      "description": "Republic Day",
      "sr_no": 1
    }
  ],
  "FO": [...],
  "CD": [...]
}
```

If the API is unavailable, the scraper creates a minimal calendar with common Indian holidays.

## Integration with Normalization Pipeline

The trading calendar can be used to validate data ingestion:

```python
from src.utils.trading_calendar import is_trading_day
from datetime import date

trade_date = date(2026, 1, 26)

if not is_trading_day(trade_date):
    print(f"Warning: {trade_date} is not a trading day")
    # Skip processing or handle appropriately
```

## Future Enhancements

- [ ] Support for special sessions (Muhurat trading, half-day sessions)
- [ ] Multiple exchange support (BSE, MCX, etc.)
- [ ] Trading hours information (pre-open, normal, post-close)
- [ ] Settlement date calculations (T+1, T+2)
- [ ] Historical calendar backfill utility
- [ ] Calendar change notifications
- [ ] Integration with symbol master validation
