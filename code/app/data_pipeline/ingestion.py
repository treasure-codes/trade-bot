"""Unit tests for DataCleaner."""

from app.data_pipeline.cleaning import DataCleaner


def test_handle_missing_drops_invalid_records() -> None:
    cleaner = DataCleaner()
    records = [
        {
            "timestamp": "2024-01-01T00:00:00+00:00",
            "ticker": "AAPL",
            "open": 1,
            "high": 2,
            "low": 1,
            "close": 1.5,
            "volume": 10,
        },
        {
            "timestamp": "2024-01-01T00:01:00+00:00",
            "ticker": "AAPL",
            "open": None,
            "high": 2,
            "low": 1,
            "close": 1.5,
            "volume": 10,
        },
    ]

    cleaned = cleaner.handle_missing(records)

    assert len(cleaned) == 1


def test_outlier_filter_removes_large_intrabar_moves() -> None:
    cleaner = DataCleaner()
    records = [
        {"timestamp": "t", "ticker": "AAPL", "open": 1, "high": 1.2, "low": 1.0, "close": 1.1, "volume": 10},
        {"timestamp": "t", "ticker": "AAPL", "open": 1, "high": 2.0, "low": 1.0, "close": 1.1, "volume": 10},
    ]

    filtered = cleaner.outlier_filter(records, max_intrabar_move=0.25)

    assert len(filtered) == 1
    assert filtered[0]["high"] == 1.2


def test_validate_schema_normalizes_fields() -> None:
    cleaner = DataCleaner()

    normalized = cleaner.validate_schema(
        [
            {
                "timestamp": "2024-01-01T00:00:00+00:00",
                "ticker": "aapl",
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100.5",
                "volume": "200",
            }
        ]
    )

    assert normalized[0]["ticker"] == "AAPL"
    assert normalized[0]["close"] == 100.5
