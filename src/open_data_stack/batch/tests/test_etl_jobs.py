"""Unit tests for batch ETL jobs."""

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pandas as pd
import pytest

from open_data_stack.batch.etl_jobs import StockDataETL, run_daily_job, run_historical_job
from open_data_stack.warehouse.models import DailyAggregate


@pytest.fixture
def sample_aggregates() -> dict[str, list[DailyAggregate]]:
    """Sample aggregates for testing."""
    return {
        "AAPL": [
            DailyAggregate(
                daily_aggregate_id=uuid4(),
                symbol="AAPL",
                date=date(2024, 1, 1),
                open_price=Decimal("150.00"),
                close_price=Decimal("152.00"),
                high_price=Decimal("153.00"),
                low_price=Decimal("149.00"),
                volume=1000000,
                daily_change=Decimal("2.00"),
                daily_change_percent=Decimal("1.33"),
            ),
            DailyAggregate(
                daily_aggregate_id=uuid4(),
                symbol="AAPL",
                date=date(2024, 1, 2),
                open_price=Decimal("152.00"),
                close_price=Decimal("155.00"),
                high_price=Decimal("156.00"),
                low_price=Decimal("151.00"),
                volume=1200000,
                daily_change=Decimal("3.00"),
                daily_change_percent=Decimal("1.97"),
            ),
        ],
        "GOOGL": [
            DailyAggregate(
                daily_aggregate_id=uuid4(),
                symbol="GOOGL",
                date=date(2024, 1, 1),
                open_price=Decimal("140.00"),
                close_price=Decimal("142.00"),
                high_price=Decimal("143.00"),
                low_price=Decimal("139.00"),
                volume=800000,
                daily_change=Decimal("2.00"),
                daily_change_percent=Decimal("1.43"),
            ),
        ],
    }


class TestStockDataETL:
    """Tests for StockDataETL class."""

    def test_init_with_custom_symbols(self) -> None:
        etl = StockDataETL(symbols=["AAPL", "GOOGL"])
        assert etl.symbols == ["AAPL", "GOOGL"]

    @patch("open_data_stack.batch.etl_jobs.get_settings")
    def test_init_with_default_symbols(self, mock_settings: MagicMock) -> None:
        mock_settings.return_value.stock_symbols = ["MSFT"]
        etl = StockDataETL()
        assert etl.symbols == ["MSFT"]

    @patch("open_data_stack.batch.etl_jobs.YahooFinanceFetcher")
    def test_extract_daily_data(self, mock_fetcher_class: MagicMock) -> None:
        mock_fetcher = MagicMock()
        mock_agg = DailyAggregate(
            symbol="AAPL",
            date=date(2024, 1, 1),
            open_price=Decimal("150.00"),
            close_price=Decimal("152.00"),
        )
        mock_fetcher.fetch_historical_data.return_value = [mock_agg]
        mock_fetcher_class.return_value = mock_fetcher

        etl = StockDataETL(symbols=["AAPL"])
        result = etl.extract_daily_data(date(2024, 1, 1))

        assert "AAPL" in result
        assert result["AAPL"] == mock_agg

    @patch("open_data_stack.batch.etl_jobs.YahooFinanceFetcher")
    def test_extract_daily_data_no_data(self, mock_fetcher_class: MagicMock) -> None:
        mock_fetcher = MagicMock()
        mock_fetcher.fetch_historical_data.return_value = []
        mock_fetcher_class.return_value = mock_fetcher

        etl = StockDataETL(symbols=["INVALID"])
        result = etl.extract_daily_data(date(2024, 1, 1))

        assert result["INVALID"] is None

    def test_transform_to_dataframe(
        self,
        sample_aggregates: dict[str, list[DailyAggregate]],
    ) -> None:
        etl = StockDataETL(symbols=["AAPL", "GOOGL"])
        df = etl.transform_to_dataframe(sample_aggregates)

        assert len(df) == 3
        assert "symbol" in df.columns
        assert "close_price" in df.columns
        assert set(df["symbol"].unique()) == {"AAPL", "GOOGL"}

    def test_transform_to_dataframe_empty(self) -> None:
        etl = StockDataETL(symbols=["AAPL"])
        df = etl.transform_to_dataframe({})

        assert df.empty

    def test_calculate_metrics(
        self,
        sample_aggregates: dict[str, list[DailyAggregate]],
    ) -> None:
        etl = StockDataETL(symbols=["AAPL", "GOOGL"])
        df = etl.transform_to_dataframe(sample_aggregates)
        df_with_metrics = etl.calculate_metrics(df)

        assert "ma_5" in df_with_metrics.columns
        assert "ma_20" in df_with_metrics.columns
        assert "volatility_5" in df_with_metrics.columns

    def test_calculate_metrics_empty(self) -> None:
        etl = StockDataETL(symbols=["AAPL"])
        df = pd.DataFrame()
        result = etl.calculate_metrics(df)

        assert result.empty

    @patch("open_data_stack.batch.etl_jobs.YahooFinanceFetcher")
    def test_run_daily_etl(self, mock_fetcher_class: MagicMock) -> None:
        mock_fetcher = MagicMock()
        mock_agg = DailyAggregate(
            symbol="AAPL",
            date=date(2024, 1, 1),
            open_price=Decimal("150.00"),
            close_price=Decimal("152.00"),
        )
        mock_fetcher.fetch_historical_data.return_value = [mock_agg]
        mock_fetcher_class.return_value = mock_fetcher

        etl = StockDataETL(symbols=["AAPL"])
        result = etl.run_daily_etl(date(2024, 1, 1))

        assert len(result) == 1
        assert result[0].symbol == "AAPL"

    @patch("open_data_stack.batch.etl_jobs.YahooFinanceFetcher")
    def test_run_historical_etl(
        self,
        mock_fetcher_class: MagicMock,
        sample_aggregates: dict[str, list[DailyAggregate]],
    ) -> None:
        mock_fetcher = MagicMock()
        mock_fetcher.fetch_all_historical_data.return_value = sample_aggregates
        mock_fetcher_class.return_value = mock_fetcher

        etl = StockDataETL(symbols=["AAPL", "GOOGL"])
        result = etl.run_historical_etl("1mo")

        assert len(result) == 3
        assert "ma_5" in result.columns


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    @patch("open_data_stack.batch.etl_jobs.StockDataETL")
    def test_run_daily_job(self, mock_etl_class: MagicMock) -> None:
        mock_etl = MagicMock()
        mock_etl.run_daily_etl.return_value = [MagicMock(), MagicMock()]
        mock_etl_class.return_value = mock_etl

        result = run_daily_job(["AAPL"], date(2024, 1, 1))

        assert result == 2
        mock_etl_class.assert_called_once_with(["AAPL"])
        mock_etl.run_daily_etl.assert_called_once_with(date(2024, 1, 1))

    @patch("open_data_stack.batch.etl_jobs.StockDataETL")
    def test_run_historical_job(self, mock_etl_class: MagicMock) -> None:
        mock_etl = MagicMock()
        mock_etl.run_historical_etl.return_value = pd.DataFrame({"a": [1, 2, 3]})
        mock_etl_class.return_value = mock_etl

        result = run_historical_job(["AAPL"], "3mo")

        assert len(result) == 3
        mock_etl_class.assert_called_once_with(["AAPL"])
        mock_etl.run_historical_etl.assert_called_once_with("3mo")
