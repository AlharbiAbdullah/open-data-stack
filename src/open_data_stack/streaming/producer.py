"""Kafka producer for streaming stock price data."""

import json
import time
from datetime import datetime
from typing import Callable

import structlog

from open_data_stack.config import get_settings
from open_data_stack.ingestion import YahooFinanceFetcher
from open_data_stack.warehouse.models import StockPrice

logger = structlog.get_logger(__name__)


class StockPriceProducer:
    """Produces stock price messages to Kafka."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        symbols: list[str] | None = None,
    ) -> None:
        """
        Initialize the Kafka producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers. Defaults to config.
            topic: Kafka topic name. Defaults to config.
            symbols: Stock symbols to stream. Defaults to config.
        """
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_topic_stock_prices
        self.symbols = symbols or settings.stock_symbols
        self.fetcher = YahooFinanceFetcher(self.symbols)
        self._producer = None

    def _get_producer(self):
        """Lazy initialization of Kafka producer."""
        if self._producer is None:
            from confluent_kafka import Producer

            self._producer = Producer({
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "stock-price-producer",
                "acks": "all",
            })
        return self._producer

    def _delivery_callback(self, err, msg) -> None:
        """Callback for message delivery confirmation."""
        if err:
            logger.error("message_delivery_failed", error=str(err))
        else:
            logger.debug(
                "message_delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    def produce_price(self, price: StockPrice) -> bool:
        """
        Produce a single stock price message to Kafka.

        Args:
            price: StockPrice model to publish

        Returns:
            True if message was queued successfully
        """
        try:
            producer = self._get_producer()

            # Serialize to JSON
            message = {
                "stock_price_id": str(price.stock_price_id),
                "symbol": price.symbol,
                "price": str(price.price),
                "volume": price.volume,
                "bid": str(price.bid) if price.bid else None,
                "ask": str(price.ask) if price.ask else None,
                "timestamp": price.timestamp.isoformat(),
                "source": price.source,
            }

            producer.produce(
                topic=self.topic,
                key=price.symbol.encode("utf-8"),
                value=json.dumps(message).encode("utf-8"),
                callback=self._delivery_callback,
            )

            # Trigger delivery callbacks
            producer.poll(0)

            logger.info(
                "produced_price",
                symbol=price.symbol,
                price=str(price.price),
            )
            return True

        except Exception as e:
            logger.error("produce_failed", symbol=price.symbol, error=str(e))
            return False

    def produce_all_prices(self) -> int:
        """
        Fetch and produce current prices for all symbols.

        Returns:
            Number of messages produced
        """
        prices = self.fetcher.fetch_all_current_prices()
        produced = 0

        for price in prices:
            if self.produce_price(price):
                produced += 1

        # Flush to ensure all messages are sent
        self.flush()

        logger.info("produced_all_prices", count=produced, total=len(self.symbols))
        return produced

    def flush(self, timeout: float = 10.0) -> int:
        """
        Flush all pending messages.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of messages still in queue (0 if all flushed)
        """
        if self._producer:
            return self._producer.flush(timeout)
        return 0

    def close(self) -> None:
        """Close the producer connection."""
        if self._producer:
            self._producer.flush()
            self._producer = None
            logger.info("producer_closed")

    def run_continuous(
        self,
        interval_seconds: int | None = None,
        max_iterations: int | None = None,
        on_produce: Callable[[list[StockPrice]], None] | None = None,
    ) -> None:
        """
        Run continuous price streaming.

        Args:
            interval_seconds: Seconds between fetches. Defaults to config.
            max_iterations: Maximum number of iterations (None for infinite).
            on_produce: Optional callback after each produce cycle.
        """
        settings = get_settings()
        interval = interval_seconds or settings.streaming_interval_seconds
        iteration = 0

        logger.info(
            "starting_continuous_streaming",
            interval=interval,
            symbols=self.symbols,
        )

        try:
            while max_iterations is None or iteration < max_iterations:
                iteration += 1

                prices = self.fetcher.fetch_all_current_prices()
                for price in prices:
                    self.produce_price(price)

                self.flush()

                if on_produce:
                    on_produce(prices)

                logger.info(
                    "streaming_iteration_complete",
                    iteration=iteration,
                    prices_sent=len(prices),
                )

                if max_iterations is None or iteration < max_iterations:
                    time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("streaming_interrupted")
        finally:
            self.close()


def create_producer(
    symbols: list[str] | None = None,
) -> StockPriceProducer:
    """
    Create a configured stock price producer.

    Args:
        symbols: Stock symbols to stream. Defaults to config.

    Returns:
        Configured StockPriceProducer instance
    """
    return StockPriceProducer(symbols=symbols)


def produce_once(symbols: list[str] | None = None) -> int:
    """
    Produce current prices once for all symbols.

    Args:
        symbols: Stock symbols to stream. Defaults to config.

    Returns:
        Number of messages produced
    """
    producer = create_producer(symbols)
    try:
        return producer.produce_all_prices()
    finally:
        producer.close()
