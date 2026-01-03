"""Streaming module for Kafka-based real-time data processing."""

from open_data_stack.streaming.consumer import (
    StockPriceConsumer,
    StockPriceProcessor,
    run_consumer_pipeline,
)
from open_data_stack.streaming.producer import (
    StockPriceProducer,
    create_producer,
    produce_once,
)

__all__ = [
    "StockPriceProducer",
    "create_producer",
    "produce_once",
    "StockPriceConsumer",
    "StockPriceProcessor",
    "run_consumer_pipeline",
]
