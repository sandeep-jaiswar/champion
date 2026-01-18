# Champion API Examples

This directory contains example scripts demonstrating how to use the Champion REST API.

## Examples

### `api_usage_example.py`

Comprehensive example showing:

- Authentication (JWT)
- Fetching OHLC data
- Getting latest prices
- Calculating technical indicators (SMA, RSI)
- Querying corporate actions (dividends)
- Retrieving candle data for charting

## Running the Examples

1. **Start the API server:**

```bash
champion api serve
```

1. **Install requests library (if not already installed):**

```bash
pip install requests
```

1. **Run the example:**

```bash
python examples/api/api_usage_example.py
```

## Using cURL

You can also test the API using cURL:

### Authentication

```bash
# Get JWT token
curl -X POST "http://localhost:8000/api/v1/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=demo&password=demo123"

# Save the token
export TOKEN="your-token-here"
```

### OHLC Data

```bash
# Get OHLC data
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/ohlc?symbol=INFY&from=2024-01-01&to=2024-12-31"

# Get latest price
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/ohlc/INFY/latest"

# Get candles
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/ohlc/INFY/candles?interval=1d"
```

### Technical Indicators

```bash
# Get SMA
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/indicators/INFY/sma?period=20"

# Get RSI
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/indicators/INFY/rsi?period=14"

# Get EMA
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/indicators/INFY/ema?period=12"
```

### Corporate Actions

```bash
# Get all corporate actions
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/corporate-actions?symbol=INFY"

# Get stock splits
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/corporate-actions/INFY/splits"

# Get dividends
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/corporate-actions/INFY/dividends"
```

### Index Data

```bash
# List all indices
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/indices"

# Get index constituents
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/indices/NIFTY50/constituents"

# Get index changes
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/indices/NIFTY50/changes"
```

## Using Postman

1. Import the OpenAPI schema from `http://localhost:8000/openapi.json`
2. Set up authentication:
   - Type: Bearer Token
   - Token: Get from `/api/v1/auth/token` endpoint
3. Test endpoints from the collection

## Interactive API Documentation

The API provides interactive documentation:

- **Swagger UI**: <http://localhost:8000/docs>
- **ReDoc**: <http://localhost:8000/redoc>

You can test all endpoints directly from the browser using these interfaces.

## Common Symbols

Indian stock symbols you can try:

- `INFY` - Infosys
- `TCS` - Tata Consultancy Services
- `RELIANCE` - Reliance Industries
- `HDFCBANK` - HDFC Bank
- `ICICIBANK` - ICICI Bank
- `SBIN` - State Bank of India
- `WIPRO` - Wipro
- `BHARTIARTL` - Bharti Airtel
- `ITC` - ITC Limited
- `ASIANPAINT` - Asian Paints

## Error Handling

All examples include basic error handling. Check the status code and response:

```python
response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    # Process data
elif response.status_code == 401:
    print("Authentication required or token expired")
elif response.status_code == 404:
    print("Resource not found")
elif response.status_code == 429:
    print("Rate limit exceeded")
else:
    print(f"Error: {response.status_code}")
    print(response.text)
```

## Rate Limiting

Remember that the API has rate limits (60 requests per minute by default). If you're making many requests, add delays:

```python
import time
time.sleep(1)  # Wait 1 second between requests
```

## Next Steps

- Explore the full API documentation at <http://localhost:8000/docs>
- Try different symbols and date ranges
- Combine multiple indicators for analysis
- Build your own trading strategies using the API
