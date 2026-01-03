"""Unit tests for Kafka producer."""

import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from open_data_stack.streaming.producer import (
    StockPriceProducer,
    create_producer,
    produce_once,
)
from open_data_stack.warehouse.models import StockPrice


@pytest.fixture
def sample_price() -> StockPrice:
    """Sample stock price for testing."""
    return StockPrice(
        stock_price_id=uuid4(),
        symbol="AAPL",
        price=Decimal("150.25"),
        volume=1000000,
        bid=Decimal("150.20"),
        ask=Decimal("150.30"),
        timestamp=datetime.now(),
        source="yahoo_finance",
    )


class TestStockPriceProducer:
    """Tests for StockPriceProducer class."""

    @patch("open_data_stack.streaming.producer.get_settings")
    def test_init_with_defaults(self, mock_settings: MagicMock) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"
        mock_settings.return_value.stock_symbols = ["AAPL"]

        producer = StockPriceProducer()

        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.topic == "test_topic"
        assert producer.symbols == ["AAPL"]

    def test_init_with_custom_values(self) -> None:
        producer = StockPriceProducer(
            bootstrap_servers="custom:9092",
            topic="custom_topic",
            symbols=["GOOGL"],
        )

        assert producer.bootstrap_servers == "custom:9092"
        assert producer.topic == "custom_topic"
        assert producer.symbols == ["GOOGL"]

    @patch("confluent_kafka.Producer")
    @patch("open_data_stack.streaming.producer.get_settings")
    def test_produce_price_success(
        self,
        mock_settings: MagicMock,
        mock_producer_class: MagicMock,
        sample_price: StockPrice,
    ) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"
        mock_settings.return_value.stock_symbols = ["AAPL"]

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        producer = StockPriceProducer()
        result = producer.produce_price(sample_price)

        assert result is True
        mock_producer.produce.assert_called_once()

        # Verify the message content
        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs.kwargs["topic"] == "test_topic"
        assert call_kwargs.kwargs["key"] == b"AAPL"

        message = json.loads(call_kwargs.kwargs["value"].decode("utf-8"))
        assert message["symbol"] == "AAPL"
        assert message["price"] == "150.25"

    @patch("confluent_kafka.Producer")
    @patch("open_data_stack.streaming.producer.get_settings")
    def test_produce_price_exception(
        self,
        mock_settings: MagicMock,
        mock_producer_class: MagicMock,
        sample_price: StockPrice,
    ) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"
        mock_settings.return_value.stock_symbols = ["AAPL"]

        mock_producer = MagicMock()
        mock_producer.produce.side_effect = Exception("Kafka error")
        mock_producer_class.return_value = mock_producer

        producer = StockPriceProducer()
        result = producer.produce_price(sample_price)

        assert result is False

    @patch("open_data_stack.streaming.producer.YahooFinanceFetcher")
    @patch("confluent_kafka.Producer")
    @patch("open_data_stack.streaming.producer.get_settings")
    def test_produce_all_prices(
        self,
        mock_settings: MagicMock,
        mock_producer_class: MagicMock,
        mock_fetcher_class: MagicMock,
        sample_price: StockPrice,
    ) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"
        mock_settings.return_value.stock_symbols = ["AAPL", "GOOGL"]

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        mock_fetcher = MagicMock()
        mock_fetcher.fetch_all_current_prices.return_value = [sample_price, sample_price]
        mock_fetcher_class.return_value = mock_fetcher

        producer = StockPriceProducer()
        count = producer.produce_all_prices()

        assert count == 2
        assert mock_producer.produce.call_count == 2

    @patch("confluent_kafka.Producer")
    @patch("open_data_stack.streaming.producer.get_settings")
    def test_flush(
        self,
        mock_settings: MagicMock,
        mock_producer_class: MagicMock,
    ) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"
        mock_settings.return_value.stock_symbols = ["AAPL"]

        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_producer_class.return_value = mock_producer

        producer = StockPriceProducer()
        producer._get_producer()  # Initialize producer
        result = producer.flush()

        assert result == 0
        mock_producer.flush.assert_called_once()

    @patch("confluent_kafka.Producer")
    @patch("open_data_stack.streaming.producer.get_settings")
    def test_close(
        self,
        mock_settings: MagicMock,
        mock_producer_class: MagicMock,
    ) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"
        mock_settings.return_value.stock_symbols = ["AAPL"]

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        producer = StockPriceProducer()
        producer._get_producer()  # Initialize producer
        producer.close()

        mock_producer.flush.assert_called_once()
        assert producer._producer is None


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    @patch("open_data_stack.streaming.producer.StockPriceProducer")
    def test_create_producer(self, mock_producer_class: MagicMock) -> None:
        create_producer(["AAPL"])
        mock_producer_class.assert_called_once_with(symbols=["AAPL"])

    @patch("open_data_stack.streaming.producer.create_producer")
    def test_produce_once(self, mock_create: MagicMock) -> None:
        mock_producer = MagicMock()
        mock_producer.produce_all_prices.return_value = 5
        mock_create.return_value = mock_producer

        result = produce_once(["AAPL"])

        assert result == 5
        mock_producer.close.assert_called_once()
