"""Demo script to showcase the Open Data Stack pipeline."""

import sys
from datetime import date, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def main() -> None:
    """Run a complete demo of the data pipeline."""
    print("=" * 60)
    print("  Open Data Stack - Demo")
    print("=" * 60)
    print()

    # Step 1: Initialize database
    print("Step 1: Initializing DuckDB database...")
    print("-" * 40)

    import duckdb

    from open_data_stack.config import get_settings

    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(settings.duckdb_path))
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
    conn.close()
    print("Database initialized successfully")
    print()

    # Step 2: Fetch current prices
    print("Step 2: Fetching current stock prices...")
    print("-" * 40)

    from open_data_stack.ingestion import YahooFinanceFetcher

    fetcher = YahooFinanceFetcher()
    prices = fetcher.fetch_all_current_prices()

    print(f"{'Symbol':<8} {'Price':>12} {'Volume':>15}")
    print("-" * 40)
    for price in prices:
        vol = f"{price.volume:,}" if price.volume else "N/A"
        print(f"{price.symbol:<8} ${price.price:>10} {vol:>15}")
    print()

    # Step 3: Run historical ETL
    print("Step 3: Running historical ETL (1 month)...")
    print("-" * 40)

    from open_data_stack.batch import StockDataETL

    etl = StockDataETL()
    df = etl.run_historical_etl(period="1mo")

    print(f"Fetched {len(df)} records for {df['symbol'].nunique()} symbols")
    print()
    print("Sample data with calculated metrics:")
    print(df[["symbol", "date", "close_price", "daily_change_percent", "ma_5"]].tail(10).to_string())
    print()

    # Step 4: Load to DuckDB
    print("Step 4: Loading data to DuckDB warehouse...")
    print("-" * 40)

    from open_data_stack.warehouse.repository import StockRepository

    conn = duckdb.connect(str(settings.duckdb_path))
    repo = StockRepository(conn)
    data = etl.extract_historical_data(period="1mo")

    total = 0
    for symbol, aggregates in data.items():
        count = repo.upsert_daily_aggregates_batch(aggregates)
        total += count

    print(f"Loaded {total} records to DuckDB")
    print()

    # Step 5: Query the warehouse
    print("Step 5: Querying the data warehouse...")
    print("-" * 40)

    stats = repo.get_all_stats()
    print("Statistics by symbol:")
    print(stats.to_string())
    print()

    # Summary query
    result = conn.execute("""
        SELECT
            symbol,
            MIN(date) as start_date,
            MAX(date) as end_date,
            ROUND(AVG(close_price), 2) as avg_price,
            ROUND(AVG(daily_change_percent), 4) as avg_daily_change
        FROM daily_aggregates
        GROUP BY symbol
        ORDER BY avg_daily_change DESC
    """).fetchdf()

    print("Performance summary:")
    print(result.to_string())

    conn.close()

    print()
    print("=" * 60)
    print("  Demo Complete!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("  1. Start Docker services: docker-compose up -d")
    print("  2. Access Airflow: http://localhost:8080 (admin/admin)")
    print("  3. Access Superset: http://localhost:8088 (admin/admin)")
    print()


if __name__ == "__main__":
    main()
