"""Initialize DuckDB database with schema for stock data warehouse."""

import duckdb
from pathlib import Path


def get_db_path() -> Path:
    """Get the path to the DuckDB database file."""
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir / "warehouse.duckdb"


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables in the database."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            stock_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            symbol VARCHAR(10) UNIQUE NOT NULL,
            company_name VARCHAR(255),
            sector VARCHAR(100),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            stock_price_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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
        CREATE TABLE IF NOT EXISTS daily_aggregates (
            daily_aggregate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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

    # Create indexes for better query performance
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_timestamp
        ON stock_prices(symbol, timestamp DESC)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_aggregates_symbol_date
        ON daily_aggregates(symbol, date DESC)
    """)


def seed_stocks(conn: duckdb.DuckDBPyConnection) -> None:
    """Insert default stock symbols."""
    default_stocks = [
        ("AAPL", "Apple Inc.", "Technology"),
        ("GOOGL", "Alphabet Inc.", "Technology"),
        ("MSFT", "Microsoft Corporation", "Technology"),
        ("AMZN", "Amazon.com Inc.", "Consumer Cyclical"),
        ("META", "Meta Platforms Inc.", "Technology"),
    ]

    conn.executemany(
        """
        INSERT INTO stocks (symbol, company_name, sector)
        VALUES (?, ?, ?)
        ON CONFLICT (symbol) DO NOTHING
        """,
        default_stocks,
    )


def main() -> None:
    """Initialize the database."""
    db_path = get_db_path()
    print(f"Initializing database at: {db_path}")

    conn = duckdb.connect(str(db_path))

    try:
        init_schema(conn)
        print("Schema created successfully")

        seed_stocks(conn)
        print("Default stocks seeded")

        # Verify tables
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"Tables created: {[t[0] for t in tables]}")

    finally:
        conn.close()

    print("Database initialization complete")


if __name__ == "__main__":
    main()
