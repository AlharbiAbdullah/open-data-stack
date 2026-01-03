"""Kafka consumer for processing stock price streams."""

import json
from datetime import datetime
from decimal import Decimal
from typing import Callable, Iterator
from uuid import UUID

import duckdb
import structlog

from open_data_stack.config import get_settings
from open_data_stack.warehouse.models import StockPrice
from open_data_stack.warehouse.repository import StockRepository

logger = structlog.get_logger(__name__)


class StockPriceConsumer:
    """Consumes stock price messages from Kafka."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        group_id: str = "stock-price-consumer",
        auto_offset_reset: str = "earliest",
    ) -> None:
        """
        Initialize the Kafka consumer.

        Args:
            bootstrap_servers: Kafka bootstrap servers. Defaults to config.
            topic: Kafka topic name. Defaults to config.
            group_id: Consumer group ID.
            auto_offset_reset: Where to start consuming ('earliest' or 'latest').
        """
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_topic_stock_prices
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self._consumer = None

    def _get_consumer(self):
        """Lazy initialization of Kafka consumer."""
        if self._consumer is None:
            from confluent_kafka import Consumer

            self._consumer = Consumer({
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": self.auto_offset_reset,
                "enable.auto.commit": True,
                "auto.commit.interval.ms": 5000,
            })
            self._consumer.subscribe([self.topic])
            logger.info("consumer_subscribed", topic=self.topic)
        return self._consumer

    def _parse_message(self, message_value: bytes) -> StockPrice | None:
        """Parse a Kafka message into a StockPrice model."""
        try:
            data = json.loads(message_value.decode("utf-8"))

            return StockPrice(
                stock_price_id=UUID(data["stock_price_id"]),
                symbol=data["symbol"],
                price=Decimal(data["price"]),
                volume=data.get("volume"),
                bid=Decimal(data["bid"]) if data.get("bid") else None,
                ask=Decimal(data["ask"]) if data.get("ask") else None,
                timestamp=datetime.fromisoformat(data["timestamp"]),
                source=data.get("source", "kafka"),
            )
        except Exception as e:
            logger.error("parse_message_failed", error=str(e))
            return None

    def consume_one(self, timeout: float = 1.0) -> StockPrice | None:
        """
        Consume a single message from Kafka.

        Args:
            timeout: Poll timeout in seconds

        Returns:
            StockPrice model or None if no message available
        """
        consumer = self._get_consumer()
        msg = consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            logger.error("consumer_error", error=str(msg.error()))
            return None

        price = self._parse_message(msg.value())
        if price:
            logger.debug(
                "consumed_message",
                symbol=price.symbol,
                price=str(price.price),
            )
        return price

    def consume_batch(
        self,
        max_messages: int = 100,
        timeout: float = 1.0,
    ) -> list[StockPrice]:
        """
        Consume a batch of messages from Kafka.

        Args:
            max_messages: Maximum messages to consume
            timeout: Poll timeout in seconds

        Returns:
            List of StockPrice models
        """
        prices = []
        for _ in range(max_messages):
            price = self.consume_one(timeout)
            if price is None:
                break
            prices.append(price)

        if prices:
            logger.info("consumed_batch", count=len(prices))
        return prices

    def consume_stream(
        self,
        timeout: float = 1.0,
    ) -> Iterator[StockPrice]:
        """
        Continuously consume messages as a generator.

        Args:
            timeout: Poll timeout in seconds

        Yields:
            StockPrice models as they arrive
        """
        logger.info("starting_stream_consumption")
        try:
            while True:
                price = self.consume_one(timeout)
                if price:
                    yield price
        except KeyboardInterrupt:
            logger.info("stream_consumption_interrupted")
        finally:
            self.close()

    def close(self) -> None:
        """Close the consumer connection."""
        if self._consumer:
            self._consumer.close()
            self._consumer = None
            logger.info("consumer_closed")


class StockPriceProcessor:
    """Processes consumed stock prices and loads to DuckDB."""

    def __init__(
        self,
        db_path: str | None = None,
        batch_size: int = 100,
    ) -> None:
        """
        Initialize the processor.

        Args:
            db_path: Path to DuckDB database. Defaults to config.
            batch_size: Number of records to batch before writing.
        """
        settings = get_settings()
        self.db_path = db_path or str(settings.duckdb_path)
        self.batch_size = batch_size
        self._buffer: list[StockPrice] = []
        self._conn: duckdb.DuckDBPyConnection | None = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            self._ensure_schema()
        return self._conn

    def _ensure_schema(self) -> None:
        """Ensure the stock_prices table exists."""
        if self._conn:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    stock_price_id UUID PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    price DECIMAL(12, 4) NOT NULL,
                    volume BIGINT,
                    bid DECIMAL(12, 4),
                    ask DECIMAL(12, 4),
                    timestamp TIMESTAMPTZ NOT NULL,
                    source VARCHAR(50) DEFAULT 'kafka',
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def process(self, price: StockPrice) -> int:
        """
        Process a single stock price.

        Args:
            price: StockPrice to process

        Returns:
            Number of records written (0 if buffered, batch_size if flushed)
        """
        self._buffer.append(price)

        if len(self._buffer) >= self.batch_size:
            return self.flush()
        return 0

    def process_batch(self, prices: list[StockPrice]) -> int:
        """
        Process a batch of stock prices.

        Args:
            prices: List of StockPrice models

        Returns:
            Number of records written
        """
        self._buffer.extend(prices)

        if len(self._buffer) >= self.batch_size:
            return self.flush()
        return 0

    def flush(self) -> int:
        """
        Flush buffered records to database.

        Returns:
            Number of records written
        """
        if not self._buffer:
            return 0

        conn = self._get_connection()
        repo = StockRepository(conn)

        count = repo.insert_stock_prices_batch(self._buffer)
        self._buffer = []

        logger.info("flushed_to_database", count=count)
        return count

    def close(self) -> None:
        """Close processor and flush remaining records."""
        self.flush()
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("processor_closed")


def run_consumer_pipeline(
    max_messages: int | None = None,
    batch_size: int = 100,
    on_process: Callable[[StockPrice], None] | None = None,
) -> int:
    """
    Run the complete consumer pipeline.

    Args:
        max_messages: Maximum messages to process (None for infinite).
        batch_size: Records to batch before writing.
        on_process: Optional callback for each processed message.

    Returns:
        Total number of records written to database
    """
    consumer = StockPriceConsumer()
    processor = StockPriceProcessor(batch_size=batch_size)

    total_written = 0
    messages_processed = 0

    try:
        logger.info("starting_consumer_pipeline", max_messages=max_messages)

        for price in consumer.consume_stream():
            written = processor.process(price)
            total_written += written
            messages_processed += 1

            if on_process:
                on_process(price)

            if max_messages and messages_processed >= max_messages:
                break

        # Flush remaining records
        total_written += processor.flush()

    finally:
        consumer.close()
        processor.close()

    logger.info(
        "consumer_pipeline_complete",
        messages_processed=messages_processed,
        total_written=total_written,
    )
    return total_written
