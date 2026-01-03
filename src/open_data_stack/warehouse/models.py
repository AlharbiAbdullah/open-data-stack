"""Pydantic models for DuckDB warehouse tables."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class Stock(BaseModel):
    """Stock metadata model."""

    stock_id: UUID = Field(default_factory=uuid4)
    symbol: str = Field(..., max_length=10)
    company_name: str | None = Field(default=None, max_length=255)
    sector: str | None = Field(default=None, max_length=100)
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(from_attributes=True)


class StockPrice(BaseModel):
    """Real-time stock price model (streaming path)."""

    stock_price_id: UUID = Field(default_factory=uuid4)
    symbol: str = Field(..., max_length=10)
    price: Decimal = Field(..., decimal_places=4)
    volume: int | None = None
    bid: Decimal | None = Field(default=None, decimal_places=4)
    ask: Decimal | None = Field(default=None, decimal_places=4)
    timestamp: datetime
    source: str = "yahoo_finance"
    created_at: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(from_attributes=True)


class DailyAggregate(BaseModel):
    """Daily OHLCV aggregate model (batch path)."""

    daily_aggregate_id: UUID = Field(default_factory=uuid4)
    symbol: str = Field(..., max_length=10)
    date: date
    open_price: Decimal | None = Field(default=None, decimal_places=4)
    close_price: Decimal | None = Field(default=None, decimal_places=4)
    high_price: Decimal | None = Field(default=None, decimal_places=4)
    low_price: Decimal | None = Field(default=None, decimal_places=4)
    adj_close: Decimal | None = Field(default=None, decimal_places=4)
    volume: int | None = None
    daily_change: Decimal | None = Field(default=None, decimal_places=4)
    daily_change_percent: Decimal | None = Field(default=None, decimal_places=4)
    created_at: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(from_attributes=True)
