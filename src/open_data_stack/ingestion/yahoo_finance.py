"""Yahoo Finance data fetcher using yfinance library."""

from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog
import yfinance as yf

from open_data_stack.config import get_settings
from open_data_stack.warehouse.models import DailyAggregate, StockPrice

logger = structlog.get_logger(__name__)


class YahooFinanceFetcher:
    """Fetches stock data from Yahoo Finance API."""

    def __init__(self, symbols: list[str] | None = None) -> None:
        """
        Initialize the fetcher with stock symbols.

        Args:
            symbols: List of stock symbols to track. Defaults to config settings.
        """
        self.symbols = symbols or get_settings().stock_symbols
        self._tickers: dict[str, yf.Ticker] = {}

    def _get_ticker(self, symbol: str) -> yf.Ticker:
        """Get or create a ticker instance for a symbol."""
        if symbol not in self._tickers:
            self._tickers[symbol] = yf.Ticker(symbol)
        return self._tickers[symbol]

    def fetch_current_price(self, symbol: str) -> StockPrice | None:
        """
        Fetch the current price for a single stock.

        Args:
            symbol: Stock ticker symbol (e.g., "AAPL")

        Returns:
            StockPrice model or None if fetch fails
        """
        try:
            ticker = self._get_ticker(symbol)
            info = ticker.fast_info

            price = getattr(info, "last_price", None)
            if price is None:
                logger.warning("no_price_data", symbol=symbol)
                return None

            return StockPrice(
                symbol=symbol,
                price=round(Decimal(str(price)), 4),
                volume=getattr(info, "last_volume", None),
                bid=_to_decimal(getattr(info, "bid", None), max_places=4),
                ask=_to_decimal(getattr(info, "ask", None), max_places=4),
                timestamp=datetime.now(),
                source="yahoo_finance",
            )
        except Exception as e:
            logger.error("fetch_current_price_failed", symbol=symbol, error=str(e))
            return None

    def fetch_all_current_prices(self) -> list[StockPrice]:
        """
        Fetch current prices for all configured symbols.

        Returns:
            List of StockPrice models (excludes failed fetches)
        """
        prices = []
        for symbol in self.symbols:
            price = self.fetch_current_price(symbol)
            if price:
                prices.append(price)
                logger.info("fetched_price", symbol=symbol, price=float(price.price))
        return prices

    def fetch_historical_data(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
        period: str = "1mo",
    ) -> list[DailyAggregate]:
        """
        Fetch historical OHLCV data for a stock.

        Args:
            symbol: Stock ticker symbol
            start_date: Start date for historical data
            end_date: End date for historical data
            period: Period string if dates not provided (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)

        Returns:
            List of DailyAggregate models
        """
        try:
            ticker = self._get_ticker(symbol)

            if start_date and end_date:
                df = ticker.history(start=start_date, end=end_date)
            else:
                df = ticker.history(period=period)

            if df.empty:
                logger.warning("no_historical_data", symbol=symbol)
                return []

            aggregates = []
            for idx, row in df.iterrows():
                row_date = idx.date() if hasattr(idx, "date") else idx

                # Calculate daily change (round all prices to 4 decimal places)
                open_price = _to_decimal(row.get("Open"), max_places=4)
                close_price = _to_decimal(row.get("Close"), max_places=4)
                daily_change = None
                daily_change_percent = None

                if open_price and close_price and open_price > 0:
                    daily_change = round(close_price - open_price, 4)
                    daily_change_percent = round((daily_change / open_price) * 100, 4)

                aggregate = DailyAggregate(
                    symbol=symbol,
                    date=row_date,
                    open_price=open_price,
                    close_price=close_price,
                    high_price=_to_decimal(row.get("High"), max_places=4),
                    low_price=_to_decimal(row.get("Low"), max_places=4),
                    adj_close=_to_decimal(row.get("Close"), max_places=4),
                    volume=int(row.get("Volume", 0)) if row.get("Volume") else None,
                    daily_change=daily_change,
                    daily_change_percent=daily_change_percent,
                )
                aggregates.append(aggregate)

            logger.info(
                "fetched_historical_data",
                symbol=symbol,
                records=len(aggregates),
            )
            return aggregates

        except Exception as e:
            logger.error("fetch_historical_failed", symbol=symbol, error=str(e))
            return []

    def fetch_all_historical_data(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        period: str = "1mo",
    ) -> dict[str, list[DailyAggregate]]:
        """
        Fetch historical data for all configured symbols.

        Args:
            start_date: Start date for historical data
            end_date: End date for historical data
            period: Period string if dates not provided

        Returns:
            Dictionary mapping symbol to list of DailyAggregate models
        """
        results: dict[str, list[DailyAggregate]] = {}
        for symbol in self.symbols:
            results[symbol] = self.fetch_historical_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                period=period,
            )
        return results

    def fetch_yesterday_data(self) -> dict[str, DailyAggregate | None]:
        """
        Fetch yesterday's data for all symbols (common batch use case).

        Returns:
            Dictionary mapping symbol to DailyAggregate or None
        """
        yesterday = date.today() - timedelta(days=1)
        results: dict[str, DailyAggregate | None] = {}

        for symbol in self.symbols:
            aggregates = self.fetch_historical_data(
                symbol=symbol,
                start_date=yesterday,
                end_date=date.today(),
            )
            results[symbol] = aggregates[0] if aggregates else None

        return results

    def get_stock_info(self, symbol: str) -> dict[str, Any]:
        """
        Get detailed stock information.

        Args:
            symbol: Stock ticker symbol

        Returns:
            Dictionary with stock metadata
        """
        try:
            ticker = self._get_ticker(symbol)
            info = ticker.info
            return {
                "symbol": symbol,
                "company_name": info.get("longName") or info.get("shortName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("marketCap"),
                "currency": info.get("currency"),
            }
        except Exception as e:
            logger.error("get_stock_info_failed", symbol=symbol, error=str(e))
            return {"symbol": symbol}


def _to_decimal(value: Any, max_places: int | None = None) -> Decimal | None:
    """Convert a value to Decimal, handling None and invalid values."""
    if value is None:
        return None
    try:
        result = Decimal(str(value))
        if max_places is not None:
            result = round(result, max_places)
        return result
    except (ValueError, TypeError, InvalidOperation):
        return None


# Convenience function for simple usage
def fetch_stock_prices(symbols: list[str] | None = None) -> list[StockPrice]:
    """
    Fetch current prices for given symbols.

    Args:
        symbols: List of stock symbols. Defaults to config settings.

    Returns:
        List of StockPrice models
    """
    fetcher = YahooFinanceFetcher(symbols)
    return fetcher.fetch_all_current_prices()


def fetch_historical(
    symbols: list[str] | None = None,
    period: str = "1mo",
) -> dict[str, list[DailyAggregate]]:
    """
    Fetch historical data for given symbols.

    Args:
        symbols: List of stock symbols. Defaults to config settings.
        period: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, etc.)

    Returns:
        Dictionary mapping symbol to list of DailyAggregate models
    """
    fetcher = YahooFinanceFetcher(symbols)
    return fetcher.fetch_all_historical_data(period=period)
