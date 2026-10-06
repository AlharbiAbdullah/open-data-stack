# Reference

Services, configuration, DAGs, tables and Python usage for open-data-stack.

## Services (Docker Compose)

| Service | Port |
|---------|------|
| Airflow | 8080 |
| Superset | 8088 |
| Spark master UI | 8081 |
| Spark worker UI | 8082 |
| Kafka | 9092 |
| Zookeeper | 2181 |
| PostgreSQL (Airflow metadata) | 5432 |

Sign-in settings for Airflow, Superset and PostgreSQL are in `docker-compose.yml`.

## Configuration

Settings load from environment variables or `.env` (`src/open_data_stack/config.py`):

| Variable | Default | Used by |
|----------|---------|---------|
| `STOCK_SYMBOLS` | `["AAPL","GOOGL","MSFT","AMZN","META"]` | fetcher, batch ETL, producer |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | producer, consumer, Spark job |
| `KAFKA_TOPIC_STOCK_PRICES` | `stock_prices` | producer, consumer, Spark job |
| `STREAMING_INTERVAL_SECONDS` | `60` | producer |

`STOCK_SYMBOLS` is a list field, so pydantic-settings reads it as a JSON list.

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `stock_data_daily_etl` | 06:00 UTC daily | Extract, validate, load and report the run date's bars |
| `stock_data_historical_backfill` | Manual | Backfill a period (param `period`, default `1mo`) |

## Warehouse tables

From `scripts/init_database.py`:

### stocks

```sql
stock_id      UUID PRIMARY KEY
symbol        VARCHAR(10) UNIQUE NOT NULL
company_name  VARCHAR(255)
sector        VARCHAR(100)
is_active     BOOLEAN DEFAULT TRUE
created_at    TIMESTAMPTZ
updated_at    TIMESTAMPTZ
```

### daily_aggregates (batch)

```sql
daily_aggregate_id    UUID PRIMARY KEY
symbol                VARCHAR(10) NOT NULL
date                  DATE NOT NULL          -- UNIQUE(symbol, date)
open_price            DECIMAL(12,4)
close_price           DECIMAL(12,4)
high_price            DECIMAL(12,4)
low_price             DECIMAL(12,4)
adj_close             DECIMAL(12,4)
volume                BIGINT
daily_change          DECIMAL(12,4)          -- close minus open
daily_change_percent  DECIMAL(8,4)
created_at            TIMESTAMPTZ
```

### stock_prices (streaming)

```sql
stock_price_id  UUID PRIMARY KEY
symbol          VARCHAR(10) NOT NULL
price           DECIMAL(12,4) NOT NULL
volume          BIGINT
bid             DECIMAL(12,4)
ask             DECIMAL(12,4)
timestamp       TIMESTAMPTZ NOT NULL
source          VARCHAR(50)
created_at      TIMESTAMPTZ
```

## Python usage

### Fetch current stock prices

```python
from open_data_stack.ingestion import YahooFinanceFetcher

fetcher = YahooFinanceFetcher(symbols=["AAPL", "GOOGL"])
prices = fetcher.fetch_all_current_prices()

for price in prices:
    print(f"{price.symbol}: ${price.price}")
```

### Run the batch ETL with pandas metrics

```python
from open_data_stack.batch import StockDataETL

etl = StockDataETL()

# Last month's data with 5- and 20-day moving averages and 5-day volatility
df = etl.run_historical_etl(period="1mo")
print(df[["symbol", "date", "close_price", "ma_5"]].head())
```

### Query the warehouse

```python
import duckdb
from open_data_stack.warehouse import StockRepository

conn = duckdb.connect("data/warehouse.duckdb")
repo = StockRepository(conn)

stats = repo.get_all_stats()
print(stats)
```

### Stream to Kafka (requires Docker)

```python
from open_data_stack.streaming import StockPriceProducer

producer = StockPriceProducer()
producer.run_continuous(interval_seconds=60, max_iterations=10)
```

### Consume from Kafka into DuckDB

```python
from open_data_stack.streaming import run_consumer_pipeline

run_consumer_pipeline(max_messages=50)  # appends to stock_prices in batches of 100
```

### Spark windowed stats

Needs the `spark` extra (`uv sync --extra spark`).

```python
from open_data_stack.processing.spark_streaming import run_aggregation_job

run_aggregation_job()  # 5-minute windows per symbol, printed to the console
```
