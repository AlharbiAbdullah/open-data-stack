"""Unit tests for DuckDB repository."""

from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import duckdb
import pytest

from open_data_stack.warehouse.models import DailyAggregate, Stock, StockPrice
from open_data_stack.warehouse.repository import StockRepository


@pytest.fixture
def db_conn() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with schema."""
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


@pytest.fixture
def repo(db_conn: duckdb.DuckDBPyConnection) -> StockRepository:
    """Create a repository instance."""
    return StockRepository(db_conn)


@pytest.fixture
def sample_stock() -> Stock:
    """Sample stock for testing."""
    return Stock(
        stock_id=uuid4(),
        symbol="AAPL",
        company_name="Apple Inc.",
        sector="Technology",
        is_active=True,
    )


@pytest.fixture
def sample_price() -> StockPrice:
    """Sample stock price for testing."""
    return StockPrice(
        stock_price_id=uuid4(),
        symbol="AAPL",
        price=Decimal("150.25"),
        volume=1000000,
        timestamp=datetime.now(),
    )


@pytest.fixture
def sample_aggregate() -> DailyAggregate:
    """Sample daily aggregate for testing."""
    return DailyAggregate(
        daily_aggregate_id=uuid4(),
        symbol="AAPL",
        date=date.today(),
        open_price=Decimal("150.00"),
        close_price=Decimal("152.00"),
        high_price=Decimal("153.00"),
        low_price=Decimal("149.00"),
        volume=5000000,
        daily_change=Decimal("2.00"),
        daily_change_percent=Decimal("1.33"),
    )


class TestStockOperations:
    """Tests for stock-related repository operations."""

    def test_upsert_stock_insert(
        self,
        repo: StockRepository,
        sample_stock: Stock,
    ) -> None:
        repo.upsert_stock(sample_stock)

        result = repo.get_stock_by_symbol("AAPL")
        assert result is not None
        assert result.symbol == "AAPL"
        assert result.company_name == "Apple Inc."

    def test_upsert_stock_update(
        self,
        repo: StockRepository,
        sample_stock: Stock,
    ) -> None:
        repo.upsert_stock(sample_stock)

        # Update the stock
        sample_stock.company_name = "Apple Corporation"
        repo.upsert_stock(sample_stock)

        result = repo.get_stock_by_symbol("AAPL")
        assert result is not None
        assert result.company_name == "Apple Corporation"

    def test_get_stock_by_symbol_not_found(self, repo: StockRepository) -> None:
        result = repo.get_stock_by_symbol("INVALID")
        assert result is None

    def test_get_active_symbols(
        self,
        repo: StockRepository,
        sample_stock: Stock,
    ) -> None:
        repo.upsert_stock(sample_stock)

        inactive = Stock(
            stock_id=uuid4(),
            symbol="MSFT",
            company_name="Microsoft",
            is_active=False,
        )
        repo.upsert_stock(inactive)

        symbols = repo.get_active_symbols()
        assert "AAPL" in symbols
        assert "MSFT" not in symbols


class TestStockPriceOperations:
    """Tests for stock price repository operations."""

    def test_insert_stock_price(
        self,
        repo: StockRepository,
        sample_price: StockPrice,
    ) -> None:
        repo.insert_stock_price(sample_price)

        result = repo.get_latest_price("AAPL")
        assert result is not None
        assert result.symbol == "AAPL"
        assert result.price == Decimal("150.25")

    def test_insert_stock_prices_batch(self, repo: StockRepository) -> None:
        prices = [
            StockPrice(
                symbol="AAPL",
                price=Decimal("150.00"),
                timestamp=datetime.now(),
            ),
            StockPrice(
                symbol="GOOGL",
                price=Decimal("140.00"),
                timestamp=datetime.now(),
            ),
        ]

        count = repo.insert_stock_prices_batch(prices)
        assert count == 2

    def test_insert_stock_prices_batch_empty(self, repo: StockRepository) -> None:
        count = repo.insert_stock_prices_batch([])
        assert count == 0

    def test_get_latest_price_not_found(self, repo: StockRepository) -> None:
        result = repo.get_latest_price("INVALID")
        assert result is None

    def test_get_prices_since(self, repo: StockRepository) -> None:
        now = datetime.now()
        prices = [
            StockPrice(
                symbol="AAPL",
                price=Decimal("150.00"),
                timestamp=now - timedelta(hours=2),
            ),
            StockPrice(
                symbol="AAPL",
                price=Decimal("151.00"),
                timestamp=now - timedelta(hours=1),
            ),
            StockPrice(
                symbol="AAPL",
                price=Decimal("152.00"),
                timestamp=now,
            ),
        ]
        repo.insert_stock_prices_batch(prices)

        result = repo.get_prices_since("AAPL", now - timedelta(hours=1, minutes=30))
        assert len(result) == 2


class TestDailyAggregateOperations:
    """Tests for daily aggregate repository operations."""

    def test_upsert_daily_aggregate_insert(
        self,
        repo: StockRepository,
        sample_aggregate: DailyAggregate,
    ) -> None:
        repo.upsert_daily_aggregate(sample_aggregate)

        result = repo.get_daily_aggregate("AAPL", sample_aggregate.date)
        assert result is not None
        assert result.symbol == "AAPL"
        assert result.close_price == Decimal("152.00")

    def test_upsert_daily_aggregate_update(
        self,
        repo: StockRepository,
        sample_aggregate: DailyAggregate,
    ) -> None:
        repo.upsert_daily_aggregate(sample_aggregate)

        # Update with new close price
        sample_aggregate.close_price = Decimal("155.00")
        repo.upsert_daily_aggregate(sample_aggregate)

        result = repo.get_daily_aggregate("AAPL", sample_aggregate.date)
        assert result is not None
        assert result.close_price == Decimal("155.00")

    def test_upsert_daily_aggregates_batch(self, repo: StockRepository) -> None:
        aggregates = [
            DailyAggregate(
                symbol="AAPL",
                date=date(2024, 1, 1),
                open_price=Decimal("150.00"),
                close_price=Decimal("152.00"),
            ),
            DailyAggregate(
                symbol="AAPL",
                date=date(2024, 1, 2),
                open_price=Decimal("152.00"),
                close_price=Decimal("155.00"),
            ),
        ]

        count = repo.upsert_daily_aggregates_batch(aggregates)
        assert count == 2

    def test_get_daily_aggregate_not_found(self, repo: StockRepository) -> None:
        result = repo.get_daily_aggregate("INVALID", date.today())
        assert result is None

    def test_get_daily_aggregates_range(self, repo: StockRepository) -> None:
        aggregates = [
            DailyAggregate(
                symbol="AAPL",
                date=date(2024, 1, 1),
                close_price=Decimal("150.00"),
            ),
            DailyAggregate(
                symbol="AAPL",
                date=date(2024, 1, 2),
                close_price=Decimal("151.00"),
            ),
            DailyAggregate(
                symbol="AAPL",
                date=date(2024, 1, 3),
                close_price=Decimal("152.00"),
            ),
        ]
        repo.upsert_daily_aggregates_batch(aggregates)

        result = repo.get_daily_aggregates_range(
            "AAPL",
            date(2024, 1, 1),
            date(2024, 1, 2),
        )
        assert len(result) == 2

    def test_get_latest_aggregates(self, repo: StockRepository) -> None:
        aggregates = [
            DailyAggregate(
                symbol="AAPL",
                date=date(2024, 1, 1),
                close_price=Decimal("150.00"),
            ),
            DailyAggregate(
                symbol="GOOGL",
                date=date(2024, 1, 1),
                close_price=Decimal("140.00"),
            ),
        ]
        repo.upsert_daily_aggregates_batch(aggregates)

        result = repo.get_latest_aggregates(limit=10)
        assert len(result) == 2


class TestUtilityOperations:
    """Tests for utility repository operations."""

    def test_get_symbol_stats(self, repo: StockRepository) -> None:
        aggregates = [
            DailyAggregate(
                symbol="AAPL",
                date=date(2024, 1, 1),
                close_price=Decimal("150.00"),
                volume=1000000,
            ),
            DailyAggregate(
                symbol="AAPL",
                date=date(2024, 1, 2),
                close_price=Decimal("152.00"),
                volume=1200000,
            ),
        ]
        repo.upsert_daily_aggregates_batch(aggregates)

        stats = repo.get_symbol_stats("AAPL")
        assert stats["record_count"] == 2
        assert stats["first_date"] == date(2024, 1, 1)
        assert stats["last_date"] == date(2024, 1, 2)

    def test_get_symbol_stats_empty(self, repo: StockRepository) -> None:
        stats = repo.get_symbol_stats("INVALID")
        assert stats["record_count"] == 0

    def test_get_all_stats(self, repo: StockRepository) -> None:
        aggregates = [
            DailyAggregate(
                symbol="AAPL",
                date=date(2024, 1, 1),
                close_price=Decimal("150.00"),
            ),
            DailyAggregate(
                symbol="GOOGL",
                date=date(2024, 1, 1),
                close_price=Decimal("140.00"),
            ),
        ]
        repo.upsert_daily_aggregates_batch(aggregates)

        stats_df = repo.get_all_stats()
        assert len(stats_df) == 2
        assert set(stats_df["symbol"].tolist()) == {"AAPL", "GOOGL"}

    def test_query_to_dataframe(self, repo: StockRepository) -> None:
        aggregates = [
            DailyAggregate(
                symbol="AAPL",
                date=date(2024, 1, 1),
                close_price=Decimal("150.00"),
            ),
        ]
        repo.upsert_daily_aggregates_batch(aggregates)

        df = repo.query_to_dataframe(
            "SELECT symbol, close_price FROM daily_aggregates WHERE symbol = ?",
            ["AAPL"],
        )
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "AAPL"
