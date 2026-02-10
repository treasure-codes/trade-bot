"""Unit tests for basic data viewer HTML."""

from app.ui.backend.data_viewer import data_viewer_page


def test_data_viewer_contains_expected_sections() -> None:
    html = data_viewer_page()

    assert "Market Data Viewer" in html
    assert "/api/data/price/historical/${ticker}" in html
    assert "<table>" in html
