"""Pydantic schemas for API request/response models."""

from datetime import UTC, date, datetime
from typing import Any

from pydantic import BaseModel, Field


# Common Schemas
class PaginationParams(BaseModel):
    """Pagination parameters."""

    page: int = Field(default=1, ge=1, description="Page number")
    page_size: int = Field(default=100, ge=1, le=1000, description="Items per page")


class PaginatedResponse(BaseModel):
    """Paginated response wrapper."""

    data: list[Any]
    page: int
    page_size: int
    total: int | None = None
    has_more: bool = False


# OHLC Schemas
class OHLCData(BaseModel):
    """OHLC data response."""

    symbol: str = Field(..., description="Stock symbol")
    trade_date: date = Field(..., description="Trading date")
    open: float | None = Field(None, description="Open price")
    high: float | None = Field(None, description="High price")
    low: float | None = Field(None, description="Low price")
    close: float | None = Field(None, description="Close price")
    volume: int | None = Field(None, description="Trading volume")
    turnover: float | None = Field(None, description="Turnover value")

    class Config:
        from_attributes = True


class OHLCResponse(BaseModel):
    """OHLC response with metadata."""

    data: list[OHLCData]
    count: int
    symbol: str | None = None
    date_range: dict[str, date] | None = None


class CandleData(BaseModel):
    """Candle data for charting."""

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


# Corporate Action Schemas
class CorporateAction(BaseModel):
    """Corporate action data."""

    symbol: str
    ex_date: date
    action_type: str = Field(..., description="Type: split, dividend, bonus, rights")
    description: str | None = None
    ratio: str | None = None
    amount: float | None = None


class SplitData(BaseModel):
    """Stock split data."""

    symbol: str
    ex_date: date
    old_ratio: int
    new_ratio: int
    description: str | None = None


class DividendData(BaseModel):
    """Dividend data."""

    symbol: str
    ex_date: date
    record_date: date | None = None
    payment_date: date | None = None
    dividend_amount: float
    dividend_type: str = Field(..., description="Type: interim, final, special")


# Technical Indicator Schemas
class SMAData(BaseModel):
    """Simple Moving Average data."""

    symbol: str
    trade_date: date
    sma_value: float
    period: int


class RSIData(BaseModel):
    """Relative Strength Index data."""

    symbol: str
    trade_date: date
    rsi_value: float
    period: int = 14


class IndicatorResponse(BaseModel):
    """Technical indicator response."""

    symbol: str
    indicator: str
    period: int
    data: list[dict[str, Any]]
    count: int


# Index Schemas
class IndexConstituent(BaseModel):
    """Index constituent data."""

    index_name: str
    symbol: str
    company_name: str
    weightage: float | None = None
    effective_date: date


class IndexChange(BaseModel):
    """Index constituent change."""

    index_name: str
    change_date: date
    change_type: str = Field(..., description="Type: added, removed, rebalanced")
    symbol: str
    company_name: str


# Authentication Schemas
class Token(BaseModel):
    """JWT token response."""

    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Token payload data."""

    username: str | None = None


class User(BaseModel):
    """User model."""

    username: str
    email: str | None = None
    disabled: bool | None = None


# Error Schemas
class ErrorResponse(BaseModel):
    """Error response."""

    error: str
    detail: str | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
