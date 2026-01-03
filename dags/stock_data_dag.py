"""Airflow DAG for stock data ETL pipeline."""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract_daily_data(**context) -> dict:
    """Extract daily stock data from Yahoo Finance."""
    from open_data_stack.batch import StockDataETL

    execution_date = context["ds"]
    target_date = datetime.strptime(execution_date, "%Y-%m-%d").date()

    etl = StockDataETL()
    data = etl.extract_daily_data(target_date)

    # Convert to serializable format for XCom
    result = {}
    for symbol, agg in data.items():
        if agg:
            result[symbol] = agg.model_dump_json()

    return result


def transform_data(**context) -> dict:
    """Transform and validate extracted data."""
    from open_data_stack.warehouse.models import DailyAggregate

    ti = context["ti"]
    extracted_data = ti.xcom_pull(task_ids="extract_daily_data")

    if not extracted_data:
        return {"status": "no_data", "records": 0}

    # Parse and validate data
    validated = []
    for symbol, json_data in extracted_data.items():
        try:
            agg = DailyAggregate.model_validate_json(json_data)
            validated.append(agg.model_dump_json())
        except Exception as e:
            print(f"Validation failed for {symbol}: {e}")

    return {"status": "success", "records": len(validated), "data": validated}


def load_to_warehouse(**context) -> dict:
    """Load transformed data into DuckDB warehouse."""
    import duckdb

    from open_data_stack.config import get_settings
    from open_data_stack.warehouse.models import DailyAggregate
    from open_data_stack.warehouse.repository import StockRepository

    ti = context["ti"]
    transform_result = ti.xcom_pull(task_ids="transform_data")

    if transform_result.get("status") != "success" or not transform_result.get("data"):
        return {"status": "skipped", "loaded": 0}

    # Ensure data directory exists
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB and load data
    conn = duckdb.connect(str(settings.duckdb_path))

    try:
        # Initialize schema if needed
        _ensure_schema(conn)

        repo = StockRepository(conn)

        loaded = 0
        for json_data in transform_result["data"]:
            agg = DailyAggregate.model_validate_json(json_data)
            repo.upsert_daily_aggregate(agg)
            loaded += 1

        return {"status": "success", "loaded": loaded}

    finally:
        conn.close()


def _ensure_schema(conn: "duckdb.DuckDBPyConnection") -> None:
    """Ensure database schema exists."""
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


def generate_report(**context) -> dict:
    """Generate a summary report of the ETL run."""
    ti = context["ti"]
    load_result = ti.xcom_pull(task_ids="load_to_warehouse")

    execution_date = context["ds"]

    report = {
        "execution_date": execution_date,
        "status": load_result.get("status", "unknown"),
        "records_loaded": load_result.get("loaded", 0),
        "completed_at": datetime.now().isoformat(),
    }

    print(f"ETL Report: {report}")
    return report


# Define the DAG
with DAG(
    dag_id="stock_data_daily_etl",
    default_args=default_args,
    description="Daily ETL pipeline for stock market data",
    schedule_interval="0 6 * * *",  # Run daily at 6 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stock", "etl", "daily"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_daily_data",
        python_callable=extract_daily_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
    )

    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task >> report_task


# Historical backfill DAG
with DAG(
    dag_id="stock_data_historical_backfill",
    default_args=default_args,
    description="Backfill historical stock data",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stock", "etl", "backfill"],
) as backfill_dag:

    def run_historical_backfill(**context) -> dict:
        """Run historical data backfill."""
        import duckdb

        from open_data_stack.batch import StockDataETL
        from open_data_stack.config import get_settings
        from open_data_stack.warehouse.repository import StockRepository

        # Get period from DAG params or default to 1 month
        period = context["params"].get("period", "1mo")

        etl = StockDataETL()
        data = etl.extract_historical_data(period)

        settings = get_settings()
        settings.data_dir.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect(str(settings.duckdb_path))

        try:
            _ensure_schema(conn)
            repo = StockRepository(conn)

            total_loaded = 0
            for symbol, aggregates in data.items():
                loaded = repo.upsert_daily_aggregates_batch(aggregates)
                total_loaded += loaded

            return {"status": "success", "period": period, "total_loaded": total_loaded}

        finally:
            conn.close()

    backfill_task = PythonOperator(
        task_id="run_historical_backfill",
        python_callable=run_historical_backfill,
        params={"period": "1mo"},
    )
