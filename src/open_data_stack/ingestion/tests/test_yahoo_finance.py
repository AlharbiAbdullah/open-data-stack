"""Unit tests for Yahoo Finance data fetcher."""

from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from open_data_stack.ingestion.yahoo_finance import (
    YahooFinanceFetcher,
    _to_decimal,
    fetch_historical,
    fetch_stock_prices,
)
from open_data_stack.warehouse.models import DailyAggregate, StockPrice


class TestToDecimal:
    """Tests for _to_decimal helper function."""

    def test_converts_float_to_decimal(self) -> None:
        result = _to_decimal(150.25)
        assert result == Decimal("150.25")

    def test_converts_int_to_decimal(self) -> None:
        result = _to_decimal(100)
        assert result == Decimal("100")

    def test_converts_string_to_decimal(self) -> None:
        result = _to_decimal("99.99")
        assert result == Decimal("99.99")

    def test_returns_none_for_none(self) -> None:
        result = _to_decimal(None)
        assert result is None

    def test_returns_none_for_invalid_value(self) -> None:
        result = _to_decimal("not a number")
        assert result is None


class TestYahooFinanceFetcher:
    """Tests for YahooFinanceFetcher class."""

    def test_init_with_custom_symbols(self) -> None:
        symbols = ["AAPL", "GOOGL"]
        fetcher = YahooFinanceFetcher(symbols=symbols)
        assert fetcher.symbols == symbols

    def test_init_with_default_symbols(self) -> None:
        with patch("open_data_stack.ingestion.yahoo_finance.get_settings") as mock:
            mock.return_value.stock_symbols = ["MSFT", "META"]
            fetcher = YahooFinanceFetcher()
            assert fetcher.symbols == ["MSFT", "META"]

    @patch("open_data_stack.ingestion.yahoo_finance.yf.Ticker")
    def test_fetch_current_price_success(self, mock_ticker_class: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker.fast_info.last_price = 150.25
        mock_ticker.fast_info.last_volume = 1000000
        mock_ticker.fast_info.bid = 150.20
        mock_ticker.fast_info.ask = 150.30
        mock_ticker_class.return_value = mock_ticker

        fetcher = YahooFinanceFetcher(symbols=["AAPL"])
        result = fetcher.fetch_current_price("AAPL")

        assert result is not None
        assert isinstance(result, StockPrice)
        assert result.symbol == "AAPL"
        assert result.price == Decimal("150.25")
        assert result.volume == 1000000
        assert result.source == "yahoo_finance"

    @patch("open_data_stack.ingestion.yahoo_finance.yf.Ticker")
    def test_fetch_current_price_no_data(self, mock_ticker_class: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker.fast_info.last_price = None
        mock_ticker_class.return_value = mock_ticker

        fetcher = YahooFinanceFetcher(symbols=["INVALID"])
        result = fetcher.fetch_current_price("INVALID")

        assert result is None

    @patch("open_data_stack.ingestion.yahoo_finance.yf.Ticker")
    def test_fetch_current_price_exception(self, mock_ticker_class: MagicMock) -> None:
        mock_ticker_class.side_effect = Exception("API Error")

        fetcher = YahooFinanceFetcher(symbols=["AAPL"])
        result = fetcher.fetch_current_price("AAPL")

        assert result is None

    @patch("open_data_stack.ingestion.yahoo_finance.yf.Ticker")
    def test_fetch_all_current_prices(self, mock_ticker_class: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker.fast_info.last_price = 150.0
        mock_ticker.fast_info.last_volume = 500000
        mock_ticker.fast_info.bid = None
        mock_ticker.fast_info.ask = None
        mock_ticker_class.return_value = mock_ticker

        fetcher = YahooFinanceFetcher(symbols=["AAPL", "GOOGL"])
        results = fetcher.fetch_all_current_prices()

        assert len(results) == 2
        assert all(isinstance(r, StockPrice) for r in results)

    @patch("open_data_stack.ingestion.yahoo_finance.yf.Ticker")
    def test_fetch_historical_data_success(self, mock_ticker_class: MagicMock) -> None:
        import pandas as pd

        mock_ticker = MagicMock()
        mock_df = pd.DataFrame(
            {
                "Open": [100.0, 101.0],
                "High": [105.0, 106.0],
                "Low": [99.0, 100.0],
                "Close": [104.0, 105.0],
                "Volume": [1000000, 1100000],
            },
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        mock_ticker.history.return_value = mock_df
        mock_ticker_class.return_value = mock_ticker

        fetcher = YahooFinanceFetcher(symbols=["AAPL"])
        results = fetcher.fetch_historical_data("AAPL", period="5d")

        assert len(results) == 2
        assert all(isinstance(r, DailyAggregate) for r in results)
        assert results[0].symbol == "AAPL"
        assert results[0].open_price == Decimal("100.0")
        assert results[0].close_price == Decimal("104.0")

    @patch("open_data_stack.ingestion.yahoo_finance.yf.Ticker")
    def test_fetch_historical_data_empty(self, mock_ticker_class: MagicMock) -> None:
        import pandas as pd

        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()
        mock_ticker_class.return_value = mock_ticker

        fetcher = YahooFinanceFetcher(symbols=["INVALID"])
        results = fetcher.fetch_historical_data("INVALID")

        assert results == []

    @patch("open_data_stack.ingestion.yahoo_finance.yf.Ticker")
    def test_fetch_historical_data_with_dates(self, mock_ticker_class: MagicMock) -> None:
        import pandas as pd

        mock_ticker = MagicMock()
        mock_df = pd.DataFrame(
            {
                "Open": [100.0],
                "High": [105.0],
                "Low": [99.0],
                "Close": [104.0],
                "Volume": [1000000],
            },
            index=pd.to_datetime(["2024-01-15"]),
        )
        mock_ticker.history.return_value = mock_df
        mock_ticker_class.return_value = mock_ticker

        fetcher = YahooFinanceFetcher(symbols=["AAPL"])
        start = date(2024, 1, 1)
        end = date(2024, 1, 31)
        results = fetcher.fetch_historical_data("AAPL", start_date=start, end_date=end)

        mock_ticker.history.assert_called_once_with(start=start, end=end)
        assert len(results) == 1

    @patch("open_data_stack.ingestion.yahoo_finance.yf.Ticker")
    def test_daily_change_calculation(self, mock_ticker_class: MagicMock) -> None:
        import pandas as pd

        mock_ticker = MagicMock()
        mock_df = pd.DataFrame(
            {
                "Open": [100.0],
                "High": [110.0],
                "Low": [95.0],
                "Close": [105.0],
                "Volume": [1000000],
            },
            index=pd.to_datetime(["2024-01-01"]),
        )
        mock_ticker.history.return_value = mock_df
        mock_ticker_class.return_value = mock_ticker

        fetcher = YahooFinanceFetcher(symbols=["AAPL"])
        results = fetcher.fetch_historical_data("AAPL")

        assert len(results) == 1
        assert results[0].daily_change == Decimal("5.0")
        assert results[0].daily_change_percent == Decimal("5.0")

    @patch("open_data_stack.ingestion.yahoo_finance.yf.Ticker")
    def test_get_stock_info(self, mock_ticker_class: MagicMock) -> None:
        mock_ticker = MagicMock()
        mock_ticker.info = {
            "longName": "Apple Inc.",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "marketCap": 3000000000000,
            "currency": "USD",
        }
        mock_ticker_class.return_value = mock_ticker

        fetcher = YahooFinanceFetcher(symbols=["AAPL"])
        result = fetcher.get_stock_info("AAPL")

        assert result["symbol"] == "AAPL"
        assert result["company_name"] == "Apple Inc."
        assert result["sector"] == "Technology"


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    @patch("open_data_stack.ingestion.yahoo_finance.YahooFinanceFetcher")
    def test_fetch_stock_prices(self, mock_fetcher_class: MagicMock) -> None:
        mock_fetcher = MagicMock()
        mock_fetcher.fetch_all_current_prices.return_value = []
        mock_fetcher_class.return_value = mock_fetcher

        fetch_stock_prices(["AAPL"])

        mock_fetcher_class.assert_called_once_with(["AAPL"])
        mock_fetcher.fetch_all_current_prices.assert_called_once()

    @patch("open_data_stack.ingestion.yahoo_finance.YahooFinanceFetcher")
    def test_fetch_historical(self, mock_fetcher_class: MagicMock) -> None:
        mock_fetcher = MagicMock()
        mock_fetcher.fetch_all_historical_data.return_value = {}
        mock_fetcher_class.return_value = mock_fetcher

        fetch_historical(["AAPL"], period="3mo")

        mock_fetcher_class.assert_called_once_with(["AAPL"])
        mock_fetcher.fetch_all_historical_data.assert_called_once_with(period="3mo")
