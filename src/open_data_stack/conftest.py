"""Shared pytest fixtures for open_data_stack tests."""

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

import duckdb
import pytest

from open_data_stack.warehouse.models import DailyAggregate, Stock, StockPrice


@pytest.fixture
def sample_stock() -> Stock:
    """Provide a sample stock for testing."""
    return Stock(
        stock_id=uuid4(),
        symbol="AAPL",
        company_name="Apple Inc.",
        sector="Technology",
        is_active=True,
    )


@pytest.fixture
def sample_stock_price() -> StockPrice:
    """Provide a sample stock price for testing."""
    return StockPrice(
        stock_price_id=uuid4(),
        symbol="AAPL",
        price=Decimal("150.25"),
        volume=1000000,
        timestamp=datetime.now(),
        source="yahoo_finance",
    )


@pytest.fixture
def sample_daily_aggregate() -> DailyAggregate:
    """Provide a sample daily aggregate for testing."""
    return DailyAggregate(
        daily_aggregate_id=uuid4(),
        symbol="AAPL",
        date=datetime.now().date(),
        open_price=Decimal("148.50"),
        close_price=Decimal("150.25"),
        high_price=Decimal("151.00"),
        low_price=Decimal("147.80"),
        volume=50000000,
        daily_change=Decimal("1.75"),
        daily_change_percent=Decimal("1.18"),
    )


@pytest.fixture
def in_memory_db() -> duckdb.DuckDBPyConnection:
    """Provide an in-memory DuckDB connection for testing."""
    conn = duckdb.connect(":memory:")

    # Create schema
    conn.execute("""
        CREATE TABLE stocks (
            stock_id UUID PRIMARY KEY,
            symbol VARCHAR(10) UNIQUE NOT NULL,
            company_name VARCHAR(255),
            sector VARCHAR(100),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE stock_prices (
            stock_price_id UUID PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            price DECIMAL(12, 4) NOT NULL,
            volume BIGINT,
            bid DECIMAL(12, 4),
            ask DECIMAL(12, 4),
            timestamp TIMESTAMPTZ NOT NULL,
            source VARCHAR(50) DEFAULT 'yahoo_finance',
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE daily_aggregates (
            daily_aggregate_id UUID PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            open_price DECIMAL(12, 4),
            close_price DECIMAL(12, 4),
            high_price DECIMAL(12, 4),
            low_price DECIMAL(12, 4),
            adj_close DECIMAL(12, 4),
            volume BIGINT,
            daily_change DECIMAL(12, 4),
            daily_change_percent DECIMAL(8, 4),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, date)
        )
    """)

    yield conn
    conn.close()
