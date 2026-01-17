"""Example script demonstrating how to use the Champion API.

This script shows how to:
1. Authenticate with the API
2. Fetch OHLC data
3. Get technical indicators
4. Query corporate actions
"""

import requests
from datetime import date, timedelta

# API Configuration
API_BASE_URL = "http://localhost:8000"
API_PREFIX = "/api/v1"

# Demo credentials
USERNAME = "demo"
PASSWORD = "demo123"


def get_auth_token():
    """Authenticate and get JWT token."""
    url = f"{API_BASE_URL}{API_PREFIX}/auth/token"
    
    response = requests.post(
        url,
        data={"username": USERNAME, "password": PASSWORD}
    )
    
    if response.status_code == 200:
        token_data = response.json()
        print(f"✓ Authentication successful")
        return token_data["access_token"]
    else:
        print(f"✗ Authentication failed: {response.status_code}")
        print(response.text)
        return None


def get_headers(token):
    """Get request headers with authentication."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def get_ohlc_data(token, symbol="INFY", days=30):
    """Fetch OHLC data for a symbol."""
    url = f"{API_BASE_URL}{API_PREFIX}/ohlc"
    
    to_date = date.today()
    from_date = to_date - timedelta(days=days)
    
    params = {
        "symbol": symbol,
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "page": 1,
        "page_size": 100,
    }
    
    response = requests.get(url, params=params, headers=get_headers(token))
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✓ OHLC Data for {symbol}:")
        print(f"  Total records: {data['count']}")
        if data['data']:
            latest = data['data'][0]
            print(f"  Latest: {latest['trade_date']} - Close: ₹{latest['close']}")
        return data
    else:
        print(f"✗ Failed to fetch OHLC data: {response.status_code}")
        return None


def get_latest_price(token, symbol="INFY"):
    """Get the latest price for a symbol."""
    url = f"{API_BASE_URL}{API_PREFIX}/ohlc/{symbol}/latest"
    
    response = requests.get(url, headers=get_headers(token))
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✓ Latest Price for {symbol}:")
        print(f"  Date: {data['trade_date']}")
        print(f"  Open: ₹{data['open']}")
        print(f"  High: ₹{data['high']}")
        print(f"  Low: ₹{data['low']}")
        print(f"  Close: ₹{data['close']}")
        print(f"  Volume: {data['volume']:,}")
        return data
    else:
        print(f"✗ Failed to fetch latest price: {response.status_code}")
        return None


def get_sma(token, symbol="INFY", period=20):
    """Get Simple Moving Average for a symbol."""
    url = f"{API_BASE_URL}{API_PREFIX}/indicators/{symbol}/sma"
    
    params = {"period": period, "page_size": 30}
    
    response = requests.get(url, params=params, headers=get_headers(token))
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✓ SMA-{period} for {symbol}:")
        print(f"  Total data points: {data['count']}")
        if data['data']:
            latest = data['data'][0]
            print(f"  Latest: {latest['trade_date']} - SMA: ₹{latest['sma_value']:.2f}")
        return data
    else:
        print(f"✗ Failed to fetch SMA: {response.status_code}")
        return None


def get_rsi(token, symbol="INFY", period=14):
    """Get Relative Strength Index for a symbol."""
    url = f"{API_BASE_URL}{API_PREFIX}/indicators/{symbol}/rsi"
    
    params = {"period": period, "page_size": 30}
    
    response = requests.get(url, params=params, headers=get_headers(token))
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✓ RSI-{period} for {symbol}:")
        print(f"  Total data points: {data['count']}")
        if data['data']:
            latest = data['data'][0]
            rsi = latest['rsi_value']
            if rsi:
                print(f"  Latest: {latest['trade_date']} - RSI: {rsi:.2f}")
                if rsi > 70:
                    print(f"  Signal: Overbought (RSI > 70)")
                elif rsi < 30:
                    print(f"  Signal: Oversold (RSI < 30)")
                else:
                    print(f"  Signal: Neutral")
        return data
    else:
        print(f"✗ Failed to fetch RSI: {response.status_code}")
        return None


def get_dividends(token, symbol="INFY"):
    """Get dividend history for a symbol."""
    url = f"{API_BASE_URL}{API_PREFIX}/corporate-actions/{symbol}/dividends"
    
    response = requests.get(url, headers=get_headers(token))
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✓ Dividend History for {symbol}:")
        print(f"  Total records: {data['count']}")
        if data['dividends']:
            for div in data['dividends'][:5]:
                print(f"  {div['ex_date']}: ₹{div['dividend_amount']} ({div['dividend_type']})")
        return data
    else:
        print(f"✗ Failed to fetch dividends: {response.status_code}")
        return None


def get_candles(token, symbol="INFY", interval="1d"):
    """Get candle data for charting."""
    url = f"{API_BASE_URL}{API_PREFIX}/ohlc/{symbol}/candles"
    
    params = {"interval": interval, "page_size": 30}
    
    response = requests.get(url, params=params, headers=get_headers(token))
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✓ Candle Data for {symbol} ({interval}):")
        print(f"  Total candles: {data['count']}")
        if data['candles']:
            latest = data['candles'][0]
            print(f"  Latest: {latest['timestamp']}")
            print(f"  OHLC: {latest['open']}/{latest['high']}/{latest['low']}/{latest['close']}")
        return data
    else:
        print(f"✗ Failed to fetch candles: {response.status_code}")
        return None


def main():
    """Main function demonstrating API usage."""
    print("Champion API Example")
    print("=" * 50)
    
    # 1. Authenticate
    token = get_auth_token()
    if not token:
        print("Cannot proceed without authentication")
        return
    
    # 2. Get OHLC data
    symbol = "INFY"
    get_ohlc_data(token, symbol=symbol)
    
    # 3. Get latest price
    get_latest_price(token, symbol=symbol)
    
    # 4. Get technical indicators
    get_sma(token, symbol=symbol, period=20)
    get_rsi(token, symbol=symbol, period=14)
    
    # 5. Get corporate actions
    get_dividends(token, symbol=symbol)
    
    # 6. Get candle data
    get_candles(token, symbol=symbol, interval="1d")
    
    print("\n" + "=" * 50)
    print("Example completed successfully!")


if __name__ == "__main__":
    main()
