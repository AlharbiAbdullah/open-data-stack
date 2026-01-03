"""Data ingestion module for fetching stock data from external APIs."""

from open_data_stack.ingestion.yahoo_finance import (
    YahooFinanceFetcher,
    fetch_historical,
    fetch_stock_prices,
)

__all__ = [
    "YahooFinanceFetcher",
    "fetch_stock_prices",
    "fetch_historical",
]
