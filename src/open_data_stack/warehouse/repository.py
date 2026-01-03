"""Repository for DuckDB data operations."""

from datetime import date, datetime
from typing import TypeVar
from uuid import UUID

import duckdb
import pandas as pd
import structlog

from open_data_stack.warehouse.models import DailyAggregate, Stock, StockPrice

logger = structlog.get_logger(__name__)

T = TypeVar("T", Stock, StockPrice, DailyAggregate)


class StockRepository:
    """Repository for stock-related database operations."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Initialize the repository with a database connection.

        Args:
            conn: DuckDB connection instance
        """
        self.conn = conn

    # ==================== Stock Operations ====================

    def upsert_stock(self, stock: Stock) -> None:
        """Insert or update a stock record."""
        self.conn.execute(
            """
            INSERT INTO stocks (stock_id, symbol, company_name, sector, is_active, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                sector = EXCLUDED.sector,
                updated_at = EXCLUDED.updated_at
            """,
            [
                str(stock.stock_id),
                stock.symbol,
                stock.company_name,
                stock.sector,
                stock.is_active,
                datetime.now(),
            ],
        )
        logger.debug("upserted_stock", symbol=stock.symbol)

    def get_stock_by_symbol(self, symbol: str) -> Stock | None:
        """Get a stock by its symbol."""
        result = self.conn.execute(
            "SELECT * FROM stocks WHERE symbol = ?",
            [symbol],
        ).fetchone()

        if result is None:
            return None

        columns = [desc[0] for desc in self.conn.description]
        row_dict = dict(zip(columns, result))
        return Stock(**row_dict)

    def get_active_symbols(self) -> list[str]:
        """Get all active stock symbols."""
        result = self.conn.execute(
            "SELECT symbol FROM stocks WHERE is_active = TRUE"
        ).fetchall()
        return [row[0] for row in result]

    # ==================== Stock Price Operations ====================

    def insert_stock_price(self, price: StockPrice) -> None:
        """Insert a stock price record."""
        self.conn.execute(
            """
            INSERT INTO stock_prices (
                stock_price_id, symbol, price, volume, bid, ask, timestamp, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(price.stock_price_id),
                price.symbol,
                float(price.price),
                price.volume,
                float(price.bid) if price.bid else None,
                float(price.ask) if price.ask else None,
                price.timestamp,
                price.source,
            ],
        )
        logger.debug("inserted_stock_price", symbol=price.symbol, price=float(price.price))

    def insert_stock_prices_batch(self, prices: list[StockPrice]) -> int:
        """Insert multiple stock price records in batch."""
        if not prices:
            return 0

        records = [
            (
                str(p.stock_price_id),
                p.symbol,
                float(p.price),
                p.volume,
                float(p.bid) if p.bid else None,
                float(p.ask) if p.ask else None,
                p.timestamp,
                p.source,
            )
            for p in prices
        ]

        self.conn.executemany(
            """
            INSERT INTO stock_prices (
                stock_price_id, symbol, price, volume, bid, ask, timestamp, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
        logger.info("inserted_stock_prices_batch", count=len(records))
        return len(records)

    def get_latest_price(self, symbol: str) -> StockPrice | None:
        """Get the latest price for a symbol."""
        result = self.conn.execute(
            """
            SELECT * FROM stock_prices
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            [symbol],
        ).fetchone()

        if result is None:
            return None

        columns = [desc[0] for desc in self.conn.description]
        row_dict = dict(zip(columns, result))
        return StockPrice(**row_dict)

    def get_prices_since(
        self,
        symbol: str,
        since: datetime,
    ) -> list[StockPrice]:
        """Get all prices for a symbol since a given timestamp."""
        result = self.conn.execute(
            """
            SELECT * FROM stock_prices
            WHERE symbol = ? AND timestamp >= ?
            ORDER BY timestamp ASC
            """,
            [symbol, since],
        ).fetchall()

        columns = [desc[0] for desc in self.conn.description]
        return [StockPrice(**dict(zip(columns, row))) for row in result]

    # ==================== Daily Aggregate Operations ====================

    def upsert_daily_aggregate(self, aggregate: DailyAggregate) -> None:
        """Insert or update a daily aggregate record."""
        self.conn.execute(
            """
            INSERT INTO daily_aggregates (
                daily_aggregate_id, symbol, date, open_price, close_price,
                high_price, low_price, adj_close, volume,
                daily_change, daily_change_percent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, date) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                close_price = EXCLUDED.close_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                adj_close = EXCLUDED.adj_close,
                volume = EXCLUDED.volume,
                daily_change = EXCLUDED.daily_change,
                daily_change_percent = EXCLUDED.daily_change_percent
            """,
            [
                str(aggregate.daily_aggregate_id),
                aggregate.symbol,
                aggregate.date,
                float(aggregate.open_price) if aggregate.open_price else None,
                float(aggregate.close_price) if aggregate.close_price else None,
                float(aggregate.high_price) if aggregate.high_price else None,
                float(aggregate.low_price) if aggregate.low_price else None,
                float(aggregate.adj_close) if aggregate.adj_close else None,
                aggregate.volume,
                float(aggregate.daily_change) if aggregate.daily_change else None,
                float(aggregate.daily_change_percent) if aggregate.daily_change_percent else None,
            ],
        )
        logger.debug("upserted_daily_aggregate", symbol=aggregate.symbol, date=str(aggregate.date))

    def upsert_daily_aggregates_batch(self, aggregates: list[DailyAggregate]) -> int:
        """Insert or update multiple daily aggregate records in batch."""
        if not aggregates:
            return 0

        for agg in aggregates:
            self.upsert_daily_aggregate(agg)

        logger.info("upserted_daily_aggregates_batch", count=len(aggregates))
        return len(aggregates)

    def get_daily_aggregate(self, symbol: str, target_date: date) -> DailyAggregate | None:
        """Get a daily aggregate for a specific symbol and date."""
        result = self.conn.execute(
            """
            SELECT * FROM daily_aggregates
            WHERE symbol = ? AND date = ?
            """,
            [symbol, target_date],
        ).fetchone()

        if result is None:
            return None

        columns = [desc[0] for desc in self.conn.description]
        row_dict = dict(zip(columns, result))
        return DailyAggregate(**row_dict)

    def get_daily_aggregates_range(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[DailyAggregate]:
        """Get daily aggregates for a symbol within a date range."""
        result = self.conn.execute(
            """
            SELECT * FROM daily_aggregates
            WHERE symbol = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
            """,
            [symbol, start_date, end_date],
        ).fetchall()

        columns = [desc[0] for desc in self.conn.description]
        return [DailyAggregate(**dict(zip(columns, row))) for row in result]

    def get_latest_aggregates(self, limit: int = 100) -> list[DailyAggregate]:
        """Get the most recent daily aggregates across all symbols."""
        result = self.conn.execute(
            """
            SELECT * FROM daily_aggregates
            ORDER BY date DESC, symbol ASC
            LIMIT ?
            """,
            [limit],
        ).fetchall()

        columns = [desc[0] for desc in self.conn.description]
        return [DailyAggregate(**dict(zip(columns, row))) for row in result]

    # ==================== DataFrame Operations ====================

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        """
        Load a pandas DataFrame directly into a table.

        Args:
            df: DataFrame to load
            table_name: Target table name

        Returns:
            Number of rows loaded
        """
        if df.empty:
            return 0

        # Create a temporary table from the DataFrame
        self.conn.execute(f"CREATE OR REPLACE TEMP TABLE temp_{table_name} AS SELECT * FROM df")

        # Insert into main table
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_{table_name}")

        logger.info("loaded_dataframe", table=table_name, rows=len(df))
        return len(df)

    def query_to_dataframe(self, query: str, params: list | None = None) -> pd.DataFrame:
        """
        Execute a query and return results as a DataFrame.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            DataFrame with query results
        """
        if params:
            result = self.conn.execute(query, params)
        else:
            result = self.conn.execute(query)

        return result.fetchdf()

    # ==================== Utility Operations ====================

    def get_symbol_stats(self, symbol: str) -> dict:
        """Get statistics for a symbol."""
        result = self.conn.execute(
            """
            SELECT
                COUNT(*) as record_count,
                MIN(date) as first_date,
                MAX(date) as last_date,
                AVG(close_price) as avg_price,
                AVG(volume) as avg_volume
            FROM daily_aggregates
            WHERE symbol = ?
            """,
            [symbol],
        ).fetchone()

        if result is None:
            return {}

        columns = ["record_count", "first_date", "last_date", "avg_price", "avg_volume"]
        return dict(zip(columns, result))

    def get_all_stats(self) -> pd.DataFrame:
        """Get statistics for all symbols."""
        return self.query_to_dataframe(
            """
            SELECT
                symbol,
                COUNT(*) as record_count,
                MIN(date) as first_date,
                MAX(date) as last_date,
                ROUND(AVG(close_price), 2) as avg_price,
                ROUND(AVG(volume), 0) as avg_volume
            FROM daily_aggregates
            GROUP BY symbol
            ORDER BY symbol
            """
        )
