"""Initialize Superset with DuckDB datasource and sample charts."""

import json
from pathlib import Path


def get_database_config() -> dict:
    """Get DuckDB database configuration for Superset."""
    return {
        "database_name": "Stock Data Warehouse",
        "sqlalchemy_uri": "duckdb:////app/data/warehouse.duckdb",
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "extra": json.dumps({
            "metadata_params": {},
            "engine_params": {},
            "metadata_cache_timeout": {},
            "schemas_allowed_for_csv_upload": [],
        }),
    }


def get_dataset_configs() -> list[dict]:
    """Get dataset configurations for Superset."""
    return [
        {
            "table_name": "daily_aggregates",
            "database_name": "Stock Data Warehouse",
            "schema": "main",
            "columns": [
                {"column_name": "symbol", "type": "VARCHAR", "filterable": True, "groupby": True},
                {"column_name": "date", "type": "DATE", "filterable": True, "groupby": True},
                {"column_name": "open_price", "type": "DECIMAL", "filterable": True},
                {"column_name": "close_price", "type": "DECIMAL", "filterable": True},
                {"column_name": "high_price", "type": "DECIMAL", "filterable": True},
                {"column_name": "low_price", "type": "DECIMAL", "filterable": True},
                {"column_name": "volume", "type": "BIGINT", "filterable": True},
                {"column_name": "daily_change_percent", "type": "DECIMAL", "filterable": True},
            ],
            "metrics": [
                {"metric_name": "avg_close", "expression": "AVG(close_price)"},
                {"metric_name": "total_volume", "expression": "SUM(volume)"},
                {"metric_name": "avg_daily_change", "expression": "AVG(daily_change_percent)"},
                {"metric_name": "count", "expression": "COUNT(*)"},
            ],
        },
        {
            "table_name": "stock_prices",
            "database_name": "Stock Data Warehouse",
            "schema": "main",
            "columns": [
                {"column_name": "symbol", "type": "VARCHAR", "filterable": True, "groupby": True},
                {"column_name": "price", "type": "DECIMAL", "filterable": True},
                {"column_name": "volume", "type": "BIGINT", "filterable": True},
                {"column_name": "timestamp", "type": "TIMESTAMP", "filterable": True, "groupby": True},
            ],
            "metrics": [
                {"metric_name": "latest_price", "expression": "MAX(price)"},
                {"metric_name": "avg_price", "expression": "AVG(price)"},
                {"metric_name": "total_volume", "expression": "SUM(volume)"},
            ],
        },
    ]


def get_chart_configs() -> list[dict]:
    """Get sample chart configurations."""
    return [
        {
            "slice_name": "Stock Price Trends",
            "viz_type": "echarts_timeseries_line",
            "datasource_type": "table",
            "datasource_name": "daily_aggregates",
            "params": {
                "metrics": ["avg_close"],
                "groupby": ["symbol"],
                "time_column": "date",
                "row_limit": 1000,
            },
        },
        {
            "slice_name": "Trading Volume by Symbol",
            "viz_type": "echarts_timeseries_bar",
            "datasource_type": "table",
            "datasource_name": "daily_aggregates",
            "params": {
                "metrics": ["total_volume"],
                "groupby": ["symbol"],
                "time_column": "date",
                "row_limit": 1000,
            },
        },
        {
            "slice_name": "Daily Price Changes",
            "viz_type": "big_number_total",
            "datasource_type": "table",
            "datasource_name": "daily_aggregates",
            "params": {
                "metric": "avg_daily_change",
                "subheader": "Average Daily Change %",
            },
        },
        {
            "slice_name": "Top Performers",
            "viz_type": "table",
            "datasource_type": "table",
            "datasource_name": "daily_aggregates",
            "params": {
                "metrics": ["avg_daily_change", "total_volume"],
                "groupby": ["symbol"],
                "order_desc": True,
                "row_limit": 10,
            },
        },
    ]


def get_dashboard_config() -> dict:
    """Get dashboard configuration."""
    return {
        "dashboard_title": "Stock Market Overview",
        "slug": "stock-market-overview",
        "position_json": json.dumps({
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "GRID_ID": {
                "type": "GRID",
                "id": "GRID_ID",
                "children": ["ROW-1", "ROW-2"],
            },
            "ROW-1": {
                "type": "ROW",
                "id": "ROW-1",
                "children": ["CHART-1", "CHART-2"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            },
            "ROW-2": {
                "type": "ROW",
                "id": "ROW-2",
                "children": ["CHART-3", "CHART-4"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            },
        }),
        "css": "",
        "published": True,
    }


if __name__ == "__main__":
    print("Database Config:")
    print(json.dumps(get_database_config(), indent=2))
    print("\nDataset Configs:")
    print(json.dumps(get_dataset_configs(), indent=2))
    print("\nChart Configs:")
    print(json.dumps(get_chart_configs(), indent=2))
    print("\nDashboard Config:")
    print(json.dumps(get_dashboard_config(), indent=2))
