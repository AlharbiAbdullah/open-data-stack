"""Application configuration using Pydantic Settings."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # Project paths
    project_root: Path = Path(__file__).parent.parent.parent.parent
    data_dir: Path = Path(__file__).parent.parent.parent.parent / "data"
    duckdb_path: Path = Path(__file__).parent.parent.parent.parent / "data" / "warehouse.duckdb"

    # Stock symbols to track
    stock_symbols: list[str] = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]

    # Kafka settings
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_stock_prices: str = "stock_prices"

    # Streaming settings
    streaming_interval_seconds: int = 60

    # Batch settings
    batch_schedule: str = "0 6 * * *"  # Daily at 6 AM UTC

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
