# Open Data Stack - Project Specification

## Overview

A showcase data engineering project demonstrating a complete open-source data stack with dual-path processing (batch + streaming) using financial market data.

## Goals

1. **Demonstrate data pipeline automation** - Collect data from public APIs automatically
2. **Show dual-path architecture** - Both batch (data warehouse) and streaming paths
3. **Use DAG orchestration** - Airflow for workflow management
4. **Easy to run** - Single `docker-compose up` command
5. **Interview-ready** - Clean, documented, demonstrable

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Yahoo Finance API                             │    │
│  │              (via yfinance Python library)                       │    │
│  │         Stocks: AAPL, GOOGL, MSFT, AMZN, META                   │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │
           ┌───────────────────────┴───────────────────────┐
           │                                               │
           ▼                                               ▼
┌──────────────────────────┐               ┌──────────────────────────┐
│       BATCH PATH         │               │      STREAM PATH         │
│  ════════════════════    │               │  ════════════════════    │
│                          │               │                          │
│  ┌────────────────────┐  │               │  ┌────────────────────┐  │
│  │   Apache Airflow   │  │               │  │  Kafka Producer    │  │
│  │   DAG Scheduler    │  │               │  │  (Python)          │  │
│  └─────────┬──────────┘  │               │  └─────────┬──────────┘  │
│            │             │               │            │             │
│            ▼             │               │            ▼             │
│  ┌────────────────────┐  │               │  ┌────────────────────┐  │
│  │  Pandas / PySpark  │  │               │  │   Apache Kafka     │  │
│  │  ETL Processing    │  │               │  │   Message Queue    │  │
│  └─────────┬──────────┘  │               │  └─────────┬──────────┘  │
│            │             │               │            │             │
│            ▼             │               │            ▼             │
│  ┌────────────────────┐  │               │  ┌────────────────────┐  │
│  │      DuckDB        │  │               │  │  Spark Streaming   │  │
│  │  (Historical Data) │  │               │  │  (Real-time ETL)   │  │
│  └────────────────────┘  │               │  └─────────┬──────────┘  │
│                          │               │            │             │
└──────────────────────────┘               │            ▼             │
           │                               │  ┌────────────────────┐  │
           │                               │  │      DuckDB        │  │
           │                               │  │  (Live Data)       │  │
           │                               │  └────────────────────┘  │
           │                               └──────────────────────────┘
           │                                           │
           └───────────────────────┬───────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           VISUALIZATION                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                      Apache Superset                             │    │
│  │                                                                  │    │
│  │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │    │
│  │   │ Stock Chart │  │ Volume Bars │  │  Top Movers │             │    │
│  │   └─────────────┘  └─────────────┘  └─────────────┘             │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Data Ingestion Layer

**Purpose**: Fetch stock market data from Yahoo Finance API

**Implementation**:
- Python module using `yfinance` library
- Fetches: price, volume, open, close, high, low
- Supports historical and real-time data

**Files**:
- `src/open_data_stack/ingestion/yahoo_finance.py`

### 2. Streaming Path

**Purpose**: Real-time data processing pipeline

**Flow**:
```
Producer → Kafka Topic → Spark Streaming → DuckDB
```

**Components**:
- **Kafka Producer**: Publishes stock prices to Kafka topic
- **Kafka Topic**: `stock_prices` topic for price events
- **Spark Streaming**: Consumes from Kafka, transforms, writes to DuckDB

**Files**:
- `src/open_data_stack/streaming/producer.py`
- `src/open_data_stack/streaming/consumer.py`
- `src/open_data_stack/processing/spark_streaming.py`

### 3. Batch Path

**Purpose**: Scheduled ETL for historical data processing

**Flow**:
```
Airflow DAG → Fetch Data → Pandas Transform → DuckDB
```

**Components**:
- **Airflow DAG**: Schedules daily/hourly data fetch
- **ETL Job**: Pandas-based transformation
- **DuckDB Load**: Upsert to data warehouse

**Files**:
- `dags/stock_data_dag.py`
- `src/open_data_stack/batch/etl_jobs.py`

### 4. Data Warehouse (DuckDB)

**Purpose**: Analytical data store

**Why DuckDB**:
- No server required (embedded database)
- Single file storage (portable)
- SQL interface
- Great for analytical queries
- Perfect for demos

**Tables**:

```sql
-- Raw stock prices (streaming)
CREATE TABLE stock_prices (
    stock_price_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(10) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    volume BIGINT,
    timestamp TIMESTAMPTZ NOT NULL,
    source VARCHAR(50) DEFAULT 'yahoo_finance',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Daily aggregates (batch)
CREATE TABLE daily_aggregates (
    daily_aggregate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(10, 2),
    close_price DECIMAL(10, 2),
    high_price DECIMAL(10, 2),
    low_price DECIMAL(10, 2),
    volume BIGINT,
    daily_change_percent DECIMAL(5, 2),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date)
);

-- Stock metadata
CREATE TABLE stocks (
    stock_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

### 5. Visualization (Superset)

**Purpose**: Dashboards and analytics

**Dashboards**:
1. **Stock Overview**: Price trends for all tracked stocks
2. **Volume Analysis**: Trading volume over time
3. **Top Movers**: Biggest gainers/losers

**Files**:
- `dashboards/stock_dashboard.json` (exported config)

## Data Flow

### Batch Pipeline (Daily)

```
1. Airflow triggers DAG at 6:00 AM UTC
2. Fetch previous day's data from Yahoo Finance
3. Transform with Pandas:
   - Calculate daily change %
   - Clean null values
   - Validate data types
4. Load to DuckDB daily_aggregates table
5. Trigger Superset dashboard refresh
```

### Streaming Pipeline (Real-time)

```
1. Producer polls Yahoo Finance every 60 seconds
2. Publish to Kafka topic 'stock_prices'
3. Spark Streaming reads from Kafka
4. Transform:
   - Parse JSON
   - Add processing timestamp
   - Validate price ranges
5. Write to DuckDB stock_prices table
6. Superset auto-refreshes dashboard
```

## Docker Services

```yaml
services:
  zookeeper:     # Kafka dependency
  kafka:         # Message queue
  spark-master:  # Spark cluster master
  spark-worker:  # Spark cluster worker
  airflow:       # DAG scheduler
  superset:      # Visualization
```

## Configuration

### Environment Variables

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=stock_prices

# DuckDB
DUCKDB_PATH=/data/warehouse.duckdb

# Airflow
AIRFLOW_HOME=/opt/airflow

# Stock symbols
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,AMZN,META

# Polling interval (seconds)
STREAMING_INTERVAL=60
BATCH_SCHEDULE="0 6 * * *"  # Daily at 6 AM UTC
```

## API Endpoints (Optional REST API)

```
GET  /api/v1/stocks                    # List all stocks
GET  /api/v1/stocks/{symbol}           # Get stock details
GET  /api/v1/stocks/{symbol}/prices    # Get price history
GET  /api/v1/aggregates/daily          # Get daily aggregates
GET  /api/v1/health                    # Health check
```

## Success Metrics

1. **Pipeline runs without errors** - Both batch and streaming paths execute cleanly
2. **Data freshness** - Streaming data < 2 minutes old
3. **Dashboard loads** - Superset displays current data
4. **Easy demo** - Single command starts everything

## Implementation Phases

### Phase 1: Foundation
- [ ] Project structure setup
- [ ] pyproject.toml with dependencies
- [ ] Docker Compose skeleton
- [ ] DuckDB schema creation

### Phase 2: Ingestion
- [ ] Yahoo Finance data fetcher
- [ ] Unit tests for ingestion

### Phase 3: Batch Pipeline
- [ ] Pandas ETL jobs
- [ ] Airflow DAG
- [ ] DuckDB load functions

### Phase 4: Streaming Pipeline
- [ ] Kafka producer
- [ ] Spark Streaming job
- [ ] DuckDB streaming writes

### Phase 5: Visualization
- [ ] Superset setup
- [ ] Dashboard creation
- [ ] Export dashboard config

### Phase 6: Polish
- [ ] README with screenshots
- [ ] End-to-end testing
- [ ] Documentation cleanup

## Non-Goals

- Production deployment (this is a demo)
- High availability / fault tolerance
- Security hardening
- Multi-user support
- Real-time trading (just visualization)

## References

- [yfinance Documentation](https://github.com/ranaroussi/yfinance)
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [Apache Airflow](https://airflow.apache.org/)
- [DuckDB](https://duckdb.org/)
- [Apache Superset](https://superset.apache.org/)
