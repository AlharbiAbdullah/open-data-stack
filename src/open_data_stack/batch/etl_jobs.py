"""Batch ETL jobs for processing stock data."""

from datetime import date, timedelta

import pandas as pd
import structlog

from open_data_stack.config import get_settings
from open_data_stack.ingestion import YahooFinanceFetcher
from open_data_stack.warehouse.models import DailyAggregate

logger = structlog.get_logger(__name__)


class StockDataETL:
    """ETL pipeline for stock data batch processing."""

    def __init__(self, symbols: list[str] | None = None) -> None:
        """
        Initialize the ETL pipeline.

        Args:
            symbols: List of stock symbols to process. Defaults to config settings.
        """
        self.symbols = symbols or get_settings().stock_symbols
        self.fetcher = YahooFinanceFetcher(self.symbols)

    def extract_daily_data(
        self,
        target_date: date | None = None,
    ) -> dict[str, DailyAggregate | None]:
        """
        Extract daily data for all symbols.

        Args:
            target_date: Date to fetch data for. Defaults to yesterday.

        Returns:
            Dictionary mapping symbol to DailyAggregate or None
        """
        if target_date is None:
            target_date = date.today() - timedelta(days=1)

        logger.info("extracting_daily_data", target_date=str(target_date))

        results: dict[str, DailyAggregate | None] = {}
        for symbol in self.symbols:
            aggregates = self.fetcher.fetch_historical_data(
                symbol=symbol,
                start_date=target_date,
                end_date=target_date + timedelta(days=1),
            )
            results[symbol] = aggregates[0] if aggregates else None
            if results[symbol]:
                logger.info("extracted_symbol", symbol=symbol, date=str(target_date))
            else:
                logger.warning("no_data_for_symbol", symbol=symbol, date=str(target_date))

        return results

    def extract_historical_data(
        self,
        period: str = "1mo",
    ) -> dict[str, list[DailyAggregate]]:
        """
        Extract historical data for all symbols.

        Args:
            period: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, etc.)

        Returns:
            Dictionary mapping symbol to list of DailyAggregate models
        """
        logger.info("extracting_historical_data", period=period)
        return self.fetcher.fetch_all_historical_data(period=period)

    def transform_to_dataframe(
        self,
        aggregates: dict[str, list[DailyAggregate]],
    ) -> pd.DataFrame:
        """
        Transform DailyAggregate models to a pandas DataFrame.

        Args:
            aggregates: Dictionary mapping symbol to list of DailyAggregate

        Returns:
            DataFrame with all stock data
        """
        records = []
        for symbol, daily_list in aggregates.items():
            for agg in daily_list:
                records.append(agg.model_dump())

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        logger.info("transformed_to_dataframe", rows=len(df))
        return df

    def calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate additional metrics on the stock data.

        Args:
            df: DataFrame with stock data

        Returns:
            DataFrame with additional calculated columns
        """
        if df.empty:
            return df

        df = df.copy()

        # Sort by symbol and date for proper calculations
        df = df.sort_values(["symbol", "date"])

        # Calculate moving averages per symbol
        df["ma_5"] = df.groupby("symbol")["close_price"].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        df["ma_20"] = df.groupby("symbol")["close_price"].transform(
            lambda x: x.rolling(window=20, min_periods=1).mean()
        )

        # Calculate volatility (standard deviation of daily returns)
        df["volatility_5"] = df.groupby("symbol")["daily_change_percent"].transform(
            lambda x: x.rolling(window=5, min_periods=1).std()
        )

        logger.info("calculated_metrics", rows=len(df))
        return df

    def run_daily_etl(self, target_date: date | None = None) -> list[DailyAggregate]:
        """
        Run the complete daily ETL pipeline.

        Args:
            target_date: Date to process. Defaults to yesterday.

        Returns:
            List of DailyAggregate models that were processed
        """
        if target_date is None:
            target_date = date.today() - timedelta(days=1)

        logger.info("starting_daily_etl", target_date=str(target_date))

        # Extract
        data = self.extract_daily_data(target_date)

        # Filter out None values
        aggregates = [agg for agg in data.values() if agg is not None]

        logger.info(
            "daily_etl_complete",
            target_date=str(target_date),
            symbols_processed=len(aggregates),
        )

        return aggregates

    def run_historical_etl(self, period: str = "1mo") -> pd.DataFrame:
        """
        Run the complete historical ETL pipeline with metrics.

        Args:
            period: Time period to fetch

        Returns:
            DataFrame with processed stock data and metrics
        """
        logger.info("starting_historical_etl", period=period)

        # Extract
        data = self.extract_historical_data(period)

        # Transform to DataFrame
        df = self.transform_to_dataframe(data)

        if df.empty:
            logger.warning("no_data_extracted")
            return df

        # Calculate metrics
        df = self.calculate_metrics(df)

        logger.info(
            "historical_etl_complete",
            period=period,
            rows=len(df),
            symbols=df["symbol"].nunique(),
        )

        return df


def run_daily_job(symbols: list[str] | None = None, target_date: date | None = None) -> int:
    """
    Run the daily ETL job.

    Args:
        symbols: List of stock symbols. Defaults to config settings.
        target_date: Date to process. Defaults to yesterday.

    Returns:
        Number of records processed
    """
    etl = StockDataETL(symbols)
    aggregates = etl.run_daily_etl(target_date)
    return len(aggregates)


def run_historical_job(
    symbols: list[str] | None = None,
    period: str = "1mo",
) -> pd.DataFrame:
    """
    Run the historical ETL job.

    Args:
        symbols: List of stock symbols. Defaults to config settings.
        period: Time period to fetch

    Returns:
        DataFrame with processed data
    """
    etl = StockDataETL(symbols)
    return etl.run_historical_etl(period)
