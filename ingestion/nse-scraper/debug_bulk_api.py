#!/usr/bin/env python3
"""Debug bulk block deals API response."""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import httpx

target_date = date.today() - timedelta(days=5)
date_str = target_date.strftime("%d-%m-%Y")

# Establish session
session = httpx.Client(
    headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/html, text/csv, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/",
        "Origin": "https://nsewebsite-staging.nseindia.com",
    },
    timeout=30,
    follow_redirects=True,
)

# Visit main page first
print("Visiting NSE main page...")
session.get("https://www.nseindia.com/")

url = f"https://www.nseindia.com/api/historicalOR/bulk-block-short-deals?optionType=bulk_deals&from={date_str}&to={date_str}&csv=true"

print(f"\nRequesting: {url}")
response = session.get(url)

print(f"\nStatus: {response.status_code}")
print(f"Content-Type: {response.headers.get('Content-Type')}")
print(f"Content-Encoding: {response.headers.get('Content-Encoding')}")
print(f"Content-Length: {len(response.content)}")
print(f"First 50 bytes (hex): {response.content[:50].hex()}")
print(f"First 50 bytes (repr): {repr(response.content[:50])}")

# Try different decompression methods
print("\n--- Testing decompression methods ---")

# Try reading as text directly
try:
    text = response.text
    print(f"✅ Direct text decode works: {len(text)} chars")
    print(f"First 200 chars: {text[:200]}")
except Exception as e:
    print(f"❌ Direct text: {e}")

# Try gzip
try:
    import gzip
    decompressed = gzip.decompress(response.content)
    print(f"✅ Gzip works: {len(decompressed)} bytes")
except Exception as e:
    print(f"❌ Gzip: {e}")

# Try zlib
try:
    import zlib
    decompressed = zlib.decompress(response.content)
    print(f"✅ Zlib works: {len(decompressed)} bytes")
    print(f"First 200 chars: {decompressed[:200].decode('utf-8')}")
except Exception as e:
    print(f"❌ Zlib: {e}")

# Try brotli
try:
    import brotli
    decompressed = brotli.decompress(response.content)
    print(f"✅ Brotli works: {len(decompressed)} bytes")
    print(f"First 200 chars: {decompressed[:200].decode('utf-8')}")
except Exception as e:
    print(f"❌ Brotli: {e}")

session.close()
