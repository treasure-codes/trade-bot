"""Market data ingestion service for real-time and historical prices."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

from app.data_pipeline.cleaning import DataCleaner


@dataclass(slots=True)
class PricePoint:
    """Canonical market data record."""

    timestamp: str
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class MarketDataProvider(Protocol):
    """External market data provider contract."""

    def fetch_realtime(self, ticker: str) -> list[dict]:
        """Return recent real-time bars for a ticker."""

    def fetch_historical(self, ticker: str, start: datetime, end: datetime) -> list[dict]:
        """Return historical bars for a ticker and time range."""


class TimeSeriesStore(Protocol):
    """Time-series persistence contract."""

    def write_prices(self, ticker: str, records: list[PricePoint]) -> None:
        """Persist price records."""

    def read_prices(self, ticker: str, start: datetime, end: datetime) -> list[PricePoint]:
        """Read price records for a ticker and time range."""


class InMemoryTimeSeriesStore:
    """Simple in-memory time-series store for local/dev and tests."""

    def __init__(self) -> None:
        self._data: dict[str, list[PricePoint]] = {}

    def write_prices(self, ticker: str, records: list[PricePoint]) -> None:
        self._data.setdefault(ticker.upper(), []).extend(records)

    def read_prices(self, ticker: str, start: datetime, end: datetime) -> list[PricePoint]:
        start_s = start.astimezone(timezone.utc).isoformat()
        end_s = end.astimezone(timezone.utc).isoformat()
        return [
            point
            for point in self._data.get(ticker.upper(), [])
            if start_s <= point.timestamp <= end_s
        ]


class MarketDataIngestor:
    """Coordinates ingestion, cleaning, validation, and persistence."""

    def __init__(self, provider: MarketDataProvider, store: TimeSeriesStore, cleaner: DataCleaner | None = None) -> None:
        self.provider = provider
        self.store = store
        self.cleaner = cleaner or DataCleaner()

    def fetch_realtime(self, ticker: str) -> list[PricePoint]:
        """Ingest real-time market bars for a ticker."""
        raw = self.provider.fetch_realtime(ticker)
        return self._clean_and_store(ticker, raw)

    def fetch_historical(self, ticker: str, hours: int = 24) -> list[PricePoint]:
        """Ingest historical market bars for a ticker over the previous ``hours``."""
        end = datetime.now(tz=timezone.utc)
        start = end - timedelta(hours=hours)
        raw = self.provider.fetch_historical(ticker, start, end)
        return self._clean_and_store(ticker, raw)

    def get_prices(self, ticker: str, start: datetime, end: datetime) -> list[PricePoint]:
        """Read persisted price records."""
        return self.store.read_prices(ticker, start, end)

    def _clean_and_store(self, ticker: str, raw_records: list[dict]) -> list[PricePoint]:
        cleaned = self.cleaner.handle_missing(raw_records)
        filtered = self.cleaner.outlier_filter(cleaned)
        validated = self.cleaner.validate_schema(filtered)
        records = [PricePoint(**record) for record in validated]
        self.store.write_prices(ticker, records)
        return records
