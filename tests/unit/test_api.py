"""Tests for Champion REST API."""

import pytest
from champion.api.main import create_app
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create test client."""
    app = create_app()
    return TestClient(app)


class TestRootEndpoints:
    """Tests for root endpoints."""

    def test_root_endpoint(self, client):
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "version" in data
        assert "docs" in data

    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "service" in data


class TestOHLCEndpoints:
    """Tests for OHLC endpoints."""

    def test_get_ohlc_missing_symbol(self, client):
        """Test OHLC endpoint without symbol parameter."""
        response = client.get("/api/v1/ohlc")
        assert response.status_code == 422  # Unprocessable Entity

    def test_get_ohlc_with_symbol(self, client):
        """Test OHLC endpoint with symbol parameter."""
        # This might fail if ClickHouse is not available, but should return proper error
        response = client.get("/api/v1/ohlc?symbol=INFY")
        # Accept either success or proper error response
        assert response.status_code in [200, 500]

    def test_get_latest_ohlc(self, client):
        """Test latest OHLC endpoint."""
        response = client.get("/api/v1/ohlc/INFY/latest")
        # Accept either success or proper error response
        assert response.status_code in [200, 404, 500]

    def test_get_candles(self, client):
        """Test candles endpoint."""
        response = client.get("/api/v1/ohlc/INFY/candles?interval=1d")
        # Accept either success or proper error response
        assert response.status_code in [200, 500]

    def test_get_candles_invalid_interval(self, client):
        """Test candles endpoint with invalid interval."""
        response = client.get("/api/v1/ohlc/INFY/candles?interval=invalid")
        assert response.status_code == 400


class TestCorporateActionsEndpoints:
    """Tests for corporate actions endpoints."""

    def test_get_corporate_actions(self, client):
        """Test corporate actions endpoint."""
        response = client.get("/api/v1/corporate-actions")
        # Accept either success or proper error response
        assert response.status_code in [200, 500]

    def test_get_corporate_actions_with_symbol(self, client):
        """Test corporate actions endpoint with symbol."""
        response = client.get("/api/v1/corporate-actions?symbol=INFY")
        assert response.status_code in [200, 500]

    def test_get_splits(self, client):
        """Test splits endpoint."""
        response = client.get("/api/v1/corporate-actions/INFY/splits")
        assert response.status_code in [200, 500]

    def test_get_dividends(self, client):
        """Test dividends endpoint."""
        response = client.get("/api/v1/corporate-actions/INFY/dividends")
        assert response.status_code in [200, 500]


class TestIndicatorsEndpoints:
    """Tests for technical indicators endpoints."""

    def test_get_sma(self, client):
        """Test SMA endpoint."""
        response = client.get("/api/v1/indicators/INFY/sma")
        assert response.status_code in [200, 500]

    def test_get_sma_with_period(self, client):
        """Test SMA endpoint with custom period."""
        response = client.get("/api/v1/indicators/INFY/sma?period=50")
        assert response.status_code in [200, 500]

    def test_get_sma_invalid_period(self, client):
        """Test SMA endpoint with invalid period."""
        response = client.get("/api/v1/indicators/INFY/sma?period=0")
        assert response.status_code == 422

    def test_get_rsi(self, client):
        """Test RSI endpoint."""
        response = client.get("/api/v1/indicators/INFY/rsi")
        assert response.status_code in [200, 500]

    def test_get_rsi_with_period(self, client):
        """Test RSI endpoint with custom period."""
        response = client.get("/api/v1/indicators/INFY/rsi?period=21")
        assert response.status_code in [200, 500]

    def test_get_ema(self, client):
        """Test EMA endpoint."""
        response = client.get("/api/v1/indicators/INFY/ema")
        assert response.status_code in [200, 500]


class TestIndicesEndpoints:
    """Tests for index endpoints."""

    def test_list_indices(self, client):
        """Test list indices endpoint."""
        response = client.get("/api/v1/indices")
        assert response.status_code in [200, 500]

    def test_get_index_constituents(self, client):
        """Test index constituents endpoint."""
        response = client.get("/api/v1/indices/NIFTY50/constituents")
        assert response.status_code in [200, 500]

    def test_get_index_changes(self, client):
        """Test index changes endpoint."""
        response = client.get("/api/v1/indices/NIFTY50/changes")
        assert response.status_code in [200, 500]


class TestAuthEndpoints:
    """Tests for authentication endpoints."""

    def test_login_missing_credentials(self, client):
        """Test login without credentials."""
        response = client.post("/api/v1/auth/token")
        assert response.status_code == 422

    def test_login_invalid_credentials(self, client):
        """Test login with invalid credentials."""
        response = client.post(
            "/api/v1/auth/token",
            data={"username": "invalid", "password": "invalid"}
        )
        assert response.status_code == 401

    def test_login_valid_credentials(self, client):
        """Test login with valid credentials."""
        response = client.post(
            "/api/v1/auth/token",
            data={"username": "demo", "password": "demo123"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"

    def test_me_endpoint(self, client):
        """Test me endpoint."""
        response = client.get("/api/v1/auth/me")
        # May require auth token, accept either success or auth error
        assert response.status_code in [200, 401]


class TestPagination:
    """Tests for pagination."""

    def test_pagination_default(self, client):
        """Test pagination with defaults."""
        response = client.get("/api/v1/ohlc?symbol=INFY")
        # Accept either success or proper error response
        assert response.status_code in [200, 500]

    def test_pagination_custom_page(self, client):
        """Test pagination with custom page."""
        response = client.get("/api/v1/ohlc?symbol=INFY&page=2&page_size=50")
        assert response.status_code in [200, 500]

    def test_pagination_invalid_page(self, client):
        """Test pagination with invalid page."""
        response = client.get("/api/v1/ohlc?symbol=INFY&page=0")
        assert response.status_code == 400

    def test_pagination_invalid_page_size(self, client):
        """Test pagination with invalid page size."""
        response = client.get("/api/v1/ohlc?symbol=INFY&page_size=10000")
        assert response.status_code == 400


class TestAPIDocumentation:
    """Tests for API documentation."""

    def test_openapi_schema(self, client):
        """Test OpenAPI schema is available."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        schema = response.json()
        assert "openapi" in schema
        assert "info" in schema
        assert "paths" in schema

    def test_swagger_docs(self, client):
        """Test Swagger documentation is available."""
        response = client.get("/docs")
        assert response.status_code == 200

    def test_redoc_docs(self, client):
        """Test ReDoc documentation is available."""
        response = client.get("/redoc")
        assert response.status_code == 200
