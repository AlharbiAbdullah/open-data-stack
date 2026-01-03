# Open Data Stack

Open source data engineering stack demonstrating batch and streaming pipelines using financial market data.

## Features

- **Dual-path architecture**: Both batch (data warehouse) and streaming pipelines
- **Real stock data**: Live data from Yahoo Finance API (no API key required)
- **Full observability**: Superset dashboards for visualization
- **One-command startup**: Docker Compose orchestrates all services
- **Production patterns**: Proper ETL, data models, and testing

## Quick Start

```bash
# 1. Install Python dependencies
uv sync

# 2. Initialize database with schema and seed data
uv run python scripts/init_database.py

# 3. Load historical data (optional - for demo)
uv run python -c "
from open_data_stack.batch import StockDataETL
etl = StockDataETL()
df = etl.run_historical_etl(period='1mo')
print(f'Loaded {len(df)} records')
"

# 4. Start all Docker services
docker-compose up -d

# 5. Access the UIs
# - Airflow: http://localhost:8080 (admin/admin)
# - Superset: http://localhost:8088 (admin/admin)
# - Spark UI: http://localhost:8081
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                               │
│                  Yahoo Finance API                              │
│            (AAPL, GOOGL, MSFT, AMZN, META)                     │
└───────────────────────┬─────────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────────┐         ┌───────────────────┐
│    BATCH PATH     │         │   STREAM PATH     │
│  ─────────────    │         │  ─────────────    │
│  Airflow DAG      │         │  Kafka Producer   │
│       │           │         │       │           │
│       ▼           │         │       ▼           │
│  Pandas ETL       │         │  Spark Streaming  │
│       │           │         │       │           │
│       ▼           │         │       ▼           │
│    DuckDB         │         │    DuckDB         │
└───────────────────┘         └───────────────────┘
        │                               │
        └───────────────┬───────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    VISUALIZATION                                │
│                  Apache Superset                                │
│     ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
│     │  Prices  │  │  Volume  │  │  Movers  │                   │
│     └──────────┘  └──────────┘  └──────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Data Source | yfinance | Stock market data (free, no API key) |
| Message Queue | Apache Kafka | Real-time data streaming |
| Stream Processing | Apache Spark | Structured Streaming |
| Batch Orchestration | Apache Airflow | DAG scheduling |
| Data Processing | Pandas, PySpark | ETL transformations |
| Data Warehouse | DuckDB | Analytical queries (embedded) |
| Visualization | Apache Superset | Dashboards |

## Project Structure

```
open_data_stack/
├── src/open_data_stack/
│   ├── ingestion/          # Yahoo Finance data fetcher
│   ├── streaming/          # Kafka producer/consumer
│   ├── batch/              # Airflow ETL jobs
│   ├── processing/         # Spark streaming jobs
│   └── warehouse/          # DuckDB models & repository
├── dags/                   # Airflow DAG definitions
├── dashboards/             # Superset dashboard exports
├── docker/                 # Service configurations
├── scripts/                # Setup and utility scripts
└── data/                   # DuckDB database (gitignored)
```

## Services (Docker)

| Service | Port | Credentials |
|---------|------|-------------|
| Airflow | 8080 | admin / admin |
| Superset | 8088 | admin / admin |
| Spark Master UI | 8081 | - |
| Spark Worker UI | 8082 | - |
| Kafka | 9092 | - |
| Zookeeper | 2181 | - |
| PostgreSQL | 5432 | airflow / airflow |

## Usage Examples

### Fetch Current Stock Prices

```python
from open_data_stack.ingestion import YahooFinanceFetcher

fetcher = YahooFinanceFetcher(symbols=["AAPL", "GOOGL"])
prices = fetcher.fetch_all_current_prices()

for price in prices:
    print(f"{price.symbol}: ${price.price}")
```

### Run Batch ETL

```python
from open_data_stack.batch import StockDataETL

etl = StockDataETL()

# Get last month's data with metrics
df = etl.run_historical_etl(period="1mo")
print(df[["symbol", "date", "close_price", "ma_5"]].head())
```

### Load Data to DuckDB

```python
import duckdb
from open_data_stack.warehouse import StockRepository

conn = duckdb.connect("data/warehouse.duckdb")
repo = StockRepository(conn)

# Query loaded data
stats = repo.get_all_stats()
print(stats)
```

### Stream to Kafka (requires Docker)

```python
from open_data_stack.streaming import StockPriceProducer

producer = StockPriceProducer()
producer.run_continuous(interval_seconds=60, max_iterations=10)
```

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `stock_data_daily_etl` | 6 AM UTC daily | Fetches previous day's data |
| `stock_data_historical_backfill` | Manual | Backfills historical data |

## Data Models

### daily_aggregates
```sql
symbol              VARCHAR(10)    -- Stock ticker
date                DATE           -- Trading date
open_price          DECIMAL(12,4)  -- Opening price
close_price         DECIMAL(12,4)  -- Closing price
high_price          DECIMAL(12,4)  -- Highest price
low_price           DECIMAL(12,4)  -- Lowest price
volume              BIGINT         -- Trading volume
daily_change_percent DECIMAL(8,4)  -- % change
```

### stock_prices (streaming)
```sql
symbol              VARCHAR(10)    -- Stock ticker
price               DECIMAL(12,4)  -- Current price
volume              BIGINT         -- Volume
timestamp           TIMESTAMPTZ    -- Price timestamp
source              VARCHAR(50)    -- Data source
```

## Development

```bash
# Install dependencies
uv sync

# Run all tests (73 tests)
uv run pytest

# Run specific test module
uv run pytest src/open_data_stack/ingestion/tests/ -v

# Format code
uv run ruff format .

# Lint code
uv run ruff check .

# Type check
uv run mypy src/
```

## Test Coverage

| Module | Tests | Description |
|--------|-------|-------------|
| Ingestion | 18 | Yahoo Finance fetcher |
| Batch | 12 | ETL jobs |
| Warehouse | 19 | DuckDB repository |
| Streaming | 24 | Kafka producer/consumer |
| **Total** | **73** | All passing |

## Default Stock Symbols

```python
["AAPL", "GOOGL", "MSFT", "AMZN", "META"]
```

## Configuration

Environment variables (see `.env.example`):

```bash
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,AMZN,META
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
STREAMING_INTERVAL_SECONDS=60
```

## License

MIT
