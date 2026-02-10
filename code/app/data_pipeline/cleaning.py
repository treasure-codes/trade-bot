"""Data cleaning utilities for market price ingestion."""

from __future__ import annotations

from collections.abc import Iterable


class DataCleaner:
    """Cleans and validates raw market price data records."""

    REQUIRED_FIELDS = {"timestamp", "ticker", "open", "high", "low", "close", "volume"}

    def handle_missing(self, records: Iterable[dict]) -> list[dict]:
        """Drop records with missing required fields or ``None`` values."""
        cleaned: list[dict] = []
        for record in records:
            if not self.REQUIRED_FIELDS.issubset(record):
                continue
            if any(record[field] is None for field in self.REQUIRED_FIELDS):
                continue
            cleaned.append(record)
        return cleaned

    def outlier_filter(self, records: Iterable[dict], max_intrabar_move: float = 0.25) -> list[dict]:
        """Remove records that exhibit unrealistic intrabar price movements.

        ``max_intrabar_move`` is the maximum allowed percentage range between low and high.
        """
        filtered: list[dict] = []
        for record in records:
            low = float(record["low"])
            high = float(record["high"])
            if low <= 0:
                continue
            intrabar_move = (high - low) / low
            if intrabar_move <= max_intrabar_move:
                filtered.append(record)
        return filtered

    def validate_schema(self, records: Iterable[dict]) -> list[dict]:
        """Validate and normalize schema for downstream ingestion."""
        validated: list[dict] = []
        for record in records:
            if not self.REQUIRED_FIELDS.issubset(record):
                raise ValueError(f"Missing required fields in record: {record}")
            validated.append(
                {
                    "timestamp": str(record["timestamp"]),
                    "ticker": str(record["ticker"]).upper(),
                    "open": float(record["open"]),
                    "high": float(record["high"]),
                    "low": float(record["low"]),
                    "close": float(record["close"]),
                    "volume": float(record["volume"]),
                }
            )
        return validated
