"""Unit tests for price data API handlers."""

import pytest

from app.api.data_price import build_default_price_api


def test_realtime_ingestion_endpoint_handler() -> None:
    api = build_default_price_api()

    response = api.ingest_realtime("AAPL")

    assert response["ticker"] == "AAPL"
    assert response["ingested"] >= 1


def test_historical_then_get_prices_handler() -> None:
    api = build_default_price_api()

    ingest_response = api.ingest_historical("MSFT", hours=1)
    data_response = api.get_prices("MSFT", hours=1)

    assert ingest_response["ingested"] >= 1
    assert data_response["ticker"] == "MSFT"
    assert data_response["count"] >= 1


def test_hours_range_is_validated() -> None:
    api = build_default_price_api()

    with pytest.raises(ValueError):
        api.get_prices("AAPL", hours=0)
