"""Price data API-like handlers for ingestion and retrieval."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from app.data_pipeline.ingestion import InMemoryTimeSeriesStore, MarketDataIngestor, MarketDataProvider


class DummyMarketDataProvider:
    """Deterministic fallback market data provider for local development."""

    def fetch_realtime(self, ticker: str) -> list[dict]:
        now = datetime.now(tz=timezone.utc).replace(microsecond=0)
        return [
            {
                "timestamp": now.isoformat(),
                "ticker": ticker,
                "open": 100.0,
                "high": 101.0,
                "low": 99.5,
                "close": 100.5,
                "volume": 12000,
            }
        ]

    def fetch_historical(self, ticker: str, start: datetime, end: datetime) -> list[dict]:
        cursor = start.replace(microsecond=0)
        bars: list[dict] = []
        price = 100.0
        while cursor <= end:
            bars.append(
                {
                    "timestamp": cursor.isoformat(),
                    "ticker": ticker,
                    "open": price,
                    "high": price + 0.75,
                    "low": price - 0.5,
                    "close": price + 0.25,
                    "volume": 10000,
                }
            )
            cursor += timedelta(minutes=1)
            price += 0.05
        return bars


class PriceDataAPI:
    """Service object exposing endpoint-equivalent methods for price data."""

    ROUTES = {
        "realtime": "POST /api/data/price/realtime/{ticker}",
        "historical": "POST /api/data/price/historical/{ticker}",
        "prices": "GET /api/data/price/{ticker}",
    }

    def __init__(self, ingestor: MarketDataIngestor) -> None:
        self.ingestor = ingestor

    def ingest_realtime(self, ticker: str) -> dict:
        """Equivalent handler for ``POST /api/data/price/realtime/{ticker}``."""
        records = self.ingestor.fetch_realtime(ticker)
        return {"ticker": ticker.upper(), "ingested": len(records)}

    def ingest_historical(self, ticker: str, hours: int = 24) -> dict:
        """Equivalent handler for ``POST /api/data/price/historical/{ticker}``."""
        if hours < 1 or hours > 24 * 30:
            raise ValueError("hours must be between 1 and 720")
        records = self.ingestor.fetch_historical(ticker, hours=hours)
        return {"ticker": ticker.upper(), "ingested": len(records), "hours": hours}

    def get_prices(self, ticker: str, hours: int = 1) -> dict:
        """Equivalent handler for ``GET /api/data/price/{ticker}``."""
        if hours < 1 or hours > 24 * 30:
            raise ValueError("hours must be between 1 and 720")
        end = datetime.now(tz=timezone.utc)
        start = end - timedelta(hours=hours)
        records = self.ingestor.get_prices(ticker, start=start, end=end)
        return {
            "ticker": ticker.upper(),
            "count": len(records),
            "prices": [asdict(record) for record in records],
        }


def build_default_price_api(provider: MarketDataProvider | None = None) -> PriceDataAPI:
    """Build the default price API with in-memory storage."""
    market_provider = provider or DummyMarketDataProvider()
    ingestor = MarketDataIngestor(provider=market_provider, store=InMemoryTimeSeriesStore())
    return PriceDataAPI(ingestor)
