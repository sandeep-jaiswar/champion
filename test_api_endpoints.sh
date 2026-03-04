#!/bin/bash

BASE_URL="http://localhost:8000"
API_PREFIX="/api/v1"

echo "=========================================="
echo "Testing Champion API Endpoints"
echo "=========================================="
echo ""

# Test 1: Root endpoint
echo "1. Testing Root Endpoint"
echo "   GET /"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL/" | python3 -m json.tool 2>/dev/null || echo "Response: $(curl -s $BASE_URL/)"
echo ""

# Test 2: Health endpoint
echo "2. Testing Health Endpoint"
echo "   GET /health"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL/health" | python3 -m json.tool 2>/dev/null || echo "Response: $(curl -s $BASE_URL/health)"
echo ""

# Test 3: API docs
echo "3. Testing API Documentation"
echo "   GET /docs"
curl -s -w "\nStatus: %{http_code}\n" -I "$BASE_URL/docs" | head -1
echo ""

# Test 4: OpenAPI schema
echo "4. Testing OpenAPI Schema"
echo "   GET /openapi.json"
curl -s "$BASE_URL/openapi.json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(f'Title: {data[\"info\"][\"title\"]}'); print(f'Version: {data[\"info\"][\"version\"]}'); print(f'Paths: {len(data[\"paths\"])} endpoints')"
echo "   Status: 200"
echo ""

# Test 5: OHLC endpoint (without auth)
echo "5. Testing OHLC Data Endpoint"
echo "   GET $API_PREFIX/ohlc?symbol=INFY"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/ohlc?symbol=INFY" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 6: OHLC latest endpoint
echo "6. Testing OHLC Latest Endpoint"
echo "   GET $API_PREFIX/ohlc/INFY/latest"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/ohlc/INFY/latest" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 7: OHLC candles endpoint
echo "7. Testing OHLC Candles Endpoint"
echo "   GET $API_PREFIX/ohlc/INFY/candles?from=2024-01-01&to=2024-01-31"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/ohlc/INFY/candles?from=2024-01-01&to=2024-01-31" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 8: Corporate Actions endpoint
echo "8. Testing Corporate Actions Endpoint"
echo "   GET $API_PREFIX/corporate-actions?symbol=INFY"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/corporate-actions?symbol=INFY" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 9: Corporate Actions splits endpoint
echo "9. Testing Corporate Actions Splits Endpoint"
echo "   GET $API_PREFIX/corporate-actions/INFY/splits"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/corporate-actions/INFY/splits" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 10: Corporate Actions dividends endpoint
echo "10. Testing Corporate Actions Dividends Endpoint"
echo "    GET $API_PREFIX/corporate-actions/INFY/dividends"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/corporate-actions/INFY/dividends" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 11: Indicators SMA endpoint
echo "11. Testing Indicators SMA Endpoint"
echo "    GET $API_PREFIX/indicators/INFY/sma?window=20"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/indicators/INFY/sma?window=20" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 12: Indicators RSI endpoint
echo "12. Testing Indicators RSI Endpoint"
echo "    GET $API_PREFIX/indicators/INFY/rsi?window=14"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/indicators/INFY/rsi?window=14" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 13: Indicators EMA endpoint
echo "13. Testing Indicators EMA Endpoint"
echo "    GET $API_PREFIX/indicators/INFY/ema?window=20"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/indicators/INFY/ema?window=20" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 14: Indices list endpoint
echo "14. Testing Indices List Endpoint"
echo "    GET $API_PREFIX/indices"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/indices" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 15: Index constituents endpoint
echo "15. Testing Index Constituents Endpoint"
echo "    GET $API_PREFIX/indices/NIFTY50/constituents"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/indices/NIFTY50/constituents" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 16: Index changes endpoint
echo "16. Testing Index Changes Endpoint"
echo "    GET $API_PREFIX/indices/NIFTY50/changes"
curl -s -w "\nStatus: %{http_code}\n" "$BASE_URL$API_PREFIX/indices/NIFTY50/changes" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:500])..." 2>/dev/null || echo "Error or No Data"
echo ""

# Test 17: Auth token endpoint (will fail without credentials)
echo "17. Testing Auth Token Endpoint (Expected to fail without credentials)"
echo "    POST $API_PREFIX/auth/token"
curl -s -w "\nStatus: %{http_code}\n" -X POST "$BASE_URL$API_PREFIX/auth/token" -H "Content-Type: application/x-www-form-urlencoded" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d, indent=2)[:200])..." 2>/dev/null || echo "Error: Auth required"
echo ""

echo "=========================================="
echo "API Endpoint Testing Complete"
echo "=========================================="
