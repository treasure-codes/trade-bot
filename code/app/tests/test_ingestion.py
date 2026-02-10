"""Unit tests for MarketDataIngestor."""

from datetime import datetime, timedelta, timezone

from app.data_pipeline.cleaning import DataCleaner
from app.data_pipeline.ingestion import InMemoryTimeSeriesStore, MarketDataIngestor


class StubProvider:
    def fetch_realtime(self, ticker: str) -> list[dict]:
        return [
            {
                "timestamp": datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat(),
                "ticker": ticker,
                "open": 100,
                "high": 100.2,
                "low": 99.8,
                "close": 100.1,
                "volume": 1000,
            }
        ]

    def fetch_historical(self, ticker: str, start: datetime, end: datetime) -> list[dict]:
        return [
            {
                "timestamp": start.replace(microsecond=0).isoformat(),
                "ticker": ticker,
                "open": 99,
                "high": 100,
                "low": 98.9,
                "close": 99.5,
                "volume": 2000,
            }
        ]


def test_fetch_realtime_stores_records() -> None:
    ingestor = MarketDataIngestor(provider=StubProvider(), store=InMemoryTimeSeriesStore(), cleaner=DataCleaner())

    records = ingestor.fetch_realtime("AAPL")

    assert len(records) == 1
    assert records[0].ticker == "AAPL"


def test_get_prices_reads_back_stored_records() -> None:
    ingestor = MarketDataIngestor(provider=StubProvider(), store=InMemoryTimeSeriesStore(), cleaner=DataCleaner())
    ingestor.fetch_historical("AAPL", hours=1)
    end = datetime.now(tz=timezone.utc)
    start = end - timedelta(hours=2)

    rows = ingestor.get_prices("AAPL", start, end)

    assert len(rows) == 1
    assert rows[0].close == 99.5
