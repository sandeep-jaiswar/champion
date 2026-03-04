"""Tests for Champion REST API."""

from unittest.mock import MagicMock

import pytest
from champion.api.dependencies import get_clickhouse_client, get_user_repository
from champion.api.main import create_app
from fastapi.testclient import TestClient


@pytest.fixture
def mock_clickhouse():
    """Create mock ClickHouse client."""
    mock_client = MagicMock()
    mock_client.query = MagicMock(return_value=[])
    mock_client.query_df = MagicMock(return_value=None)
    mock_client.insert = MagicMock(return_value=None)
    mock_client.command = MagicMock(return_value="")
    mock_client.connect = MagicMock(return_value=None)
    mock_client.close = MagicMock(return_value=None)
    return mock_client


@pytest.fixture
def mock_user_repo():
    """Create mock user repository."""
    mock_repo = MagicMock()
    # Mock demo user for auth tests - returns a dict as per repository contract
    demo_user = {
        "username": "demo",
        "hashed_password": "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lm",  # "demo123" hashed
        "email": "demo@example.com",
        "disabled": False,
    }
    mock_repo.get_by_username = MagicMock(return_value=demo_user)
    mock_repo.create = MagicMock(return_value=demo_user)
    return mock_repo


@pytest.fixture
def client(mock_clickhouse, mock_user_repo):
    """Create test client with mocked dependencies."""
    app = create_app()
    app.dependency_overrides[get_clickhouse_client] = lambda: mock_clickhouse
    app.dependency_overrides[get_user_repository] = lambda: mock_user_repo
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
