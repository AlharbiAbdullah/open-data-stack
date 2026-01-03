"""Unit tests for Kafka consumer."""

import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from open_data_stack.streaming.consumer import (
    StockPriceConsumer,
    StockPriceProcessor,
)
from open_data_stack.warehouse.models import StockPrice


@pytest.fixture
def sample_message() -> dict:
    """Sample Kafka message payload."""
    return {
        "stock_price_id": str(uuid4()),
        "symbol": "AAPL",
        "price": "150.25",
        "volume": 1000000,
        "bid": "150.20",
        "ask": "150.30",
        "timestamp": datetime.now().isoformat(),
        "source": "yahoo_finance",
    }


@pytest.fixture
def sample_price() -> StockPrice:
    """Sample stock price for testing."""
    return StockPrice(
        stock_price_id=uuid4(),
        symbol="AAPL",
        price=Decimal("150.25"),
        volume=1000000,
        timestamp=datetime.now(),
    )


class TestStockPriceConsumer:
    """Tests for StockPriceConsumer class."""

    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_init_with_defaults(self, mock_settings: MagicMock) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"

        consumer = StockPriceConsumer()

        assert consumer.bootstrap_servers == "localhost:9092"
        assert consumer.topic == "test_topic"
        assert consumer.group_id == "stock-price-consumer"

    def test_init_with_custom_values(self) -> None:
        consumer = StockPriceConsumer(
            bootstrap_servers="custom:9092",
            topic="custom_topic",
            group_id="custom-group",
        )

        assert consumer.bootstrap_servers == "custom:9092"
        assert consumer.topic == "custom_topic"
        assert consumer.group_id == "custom-group"

    def test_parse_message_success(self, sample_message: dict) -> None:
        consumer = StockPriceConsumer(
            bootstrap_servers="localhost:9092",
            topic="test",
        )

        message_bytes = json.dumps(sample_message).encode("utf-8")
        result = consumer._parse_message(message_bytes)

        assert result is not None
        assert result.symbol == "AAPL"
        assert result.price == Decimal("150.25")
        assert result.volume == 1000000

    def test_parse_message_invalid_json(self) -> None:
        consumer = StockPriceConsumer(
            bootstrap_servers="localhost:9092",
            topic="test",
        )

        result = consumer._parse_message(b"invalid json")

        assert result is None

    def test_parse_message_missing_fields(self) -> None:
        consumer = StockPriceConsumer(
            bootstrap_servers="localhost:9092",
            topic="test",
        )

        incomplete_message = {"symbol": "AAPL"}
        message_bytes = json.dumps(incomplete_message).encode("utf-8")
        result = consumer._parse_message(message_bytes)

        assert result is None

    @patch("confluent_kafka.Consumer")
    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_consume_one_success(
        self,
        mock_settings: MagicMock,
        mock_consumer_class: MagicMock,
        sample_message: dict,
    ) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = json.dumps(sample_message).encode("utf-8")

        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = mock_msg
        mock_consumer_class.return_value = mock_consumer

        consumer = StockPriceConsumer()
        result = consumer.consume_one()

        assert result is not None
        assert result.symbol == "AAPL"

    @patch("confluent_kafka.Consumer")
    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_consume_one_no_message(
        self,
        mock_settings: MagicMock,
        mock_consumer_class: MagicMock,
    ) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"

        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        mock_consumer_class.return_value = mock_consumer

        consumer = StockPriceConsumer()
        result = consumer.consume_one()

        assert result is None

    @patch("confluent_kafka.Consumer")
    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_consume_batch(
        self,
        mock_settings: MagicMock,
        mock_consumer_class: MagicMock,
        sample_message: dict,
    ) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = json.dumps(sample_message).encode("utf-8")

        mock_consumer = MagicMock()
        # Return message twice, then None
        mock_consumer.poll.side_effect = [mock_msg, mock_msg, None]
        mock_consumer_class.return_value = mock_consumer

        consumer = StockPriceConsumer()
        result = consumer.consume_batch(max_messages=10)

        assert len(result) == 2

    @patch("confluent_kafka.Consumer")
    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_close(
        self,
        mock_settings: MagicMock,
        mock_consumer_class: MagicMock,
    ) -> None:
        mock_settings.return_value.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.return_value.kafka_topic_stock_prices = "test_topic"

        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = StockPriceConsumer()
        consumer._get_consumer()  # Initialize
        consumer.close()

        mock_consumer.close.assert_called_once()
        assert consumer._consumer is None


class TestStockPriceProcessor:
    """Tests for StockPriceProcessor class."""

    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_init(self, mock_settings: MagicMock) -> None:
        mock_settings.return_value.duckdb_path = "/tmp/test.duckdb"

        processor = StockPriceProcessor(batch_size=50)

        assert processor.batch_size == 50

    @patch("open_data_stack.streaming.consumer.duckdb")
    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_process_buffers_until_batch_size(
        self,
        mock_settings: MagicMock,
        mock_duckdb: MagicMock,
        sample_price: StockPrice,
    ) -> None:
        mock_settings.return_value.duckdb_path = "/tmp/test.duckdb"

        processor = StockPriceProcessor(batch_size=5)

        # Process fewer than batch_size
        for _ in range(3):
            result = processor.process(sample_price)
            assert result == 0  # Not flushed yet

        assert len(processor._buffer) == 3

    @patch("open_data_stack.streaming.consumer.StockRepository")
    @patch("open_data_stack.streaming.consumer.duckdb")
    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_process_flushes_at_batch_size(
        self,
        mock_settings: MagicMock,
        mock_duckdb: MagicMock,
        mock_repo_class: MagicMock,
        sample_price: StockPrice,
    ) -> None:
        mock_settings.return_value.duckdb_path = "/tmp/test.duckdb"

        mock_repo = MagicMock()
        mock_repo.insert_stock_prices_batch.return_value = 5
        mock_repo_class.return_value = mock_repo

        processor = StockPriceProcessor(batch_size=5)

        # Process exactly batch_size
        for i in range(5):
            result = processor.process(sample_price)

        # Last process should trigger flush
        assert result == 5
        assert len(processor._buffer) == 0

    @patch("open_data_stack.streaming.consumer.StockRepository")
    @patch("open_data_stack.streaming.consumer.duckdb")
    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_flush_writes_buffer(
        self,
        mock_settings: MagicMock,
        mock_duckdb: MagicMock,
        mock_repo_class: MagicMock,
        sample_price: StockPrice,
    ) -> None:
        mock_settings.return_value.duckdb_path = "/tmp/test.duckdb"

        mock_repo = MagicMock()
        mock_repo.insert_stock_prices_batch.return_value = 3
        mock_repo_class.return_value = mock_repo

        processor = StockPriceProcessor(batch_size=10)
        processor._buffer = [sample_price, sample_price, sample_price]

        result = processor.flush()

        assert result == 3
        assert len(processor._buffer) == 0
        mock_repo.insert_stock_prices_batch.assert_called_once()

    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_flush_empty_buffer(self, mock_settings: MagicMock) -> None:
        mock_settings.return_value.duckdb_path = "/tmp/test.duckdb"

        processor = StockPriceProcessor()
        result = processor.flush()

        assert result == 0

    @patch("open_data_stack.streaming.consumer.duckdb")
    @patch("open_data_stack.streaming.consumer.get_settings")
    def test_close_flushes_and_closes(
        self,
        mock_settings: MagicMock,
        mock_duckdb: MagicMock,
    ) -> None:
        mock_settings.return_value.duckdb_path = "/tmp/test.duckdb"

        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        processor = StockPriceProcessor()
        processor._get_connection()  # Initialize
        processor.close()

        mock_conn.close.assert_called_once()
        assert processor._conn is None
