"""Basic HTML UI for viewing ingested market data."""

from __future__ import annotations


def data_viewer_page() -> str:
    """Render a minimal data viewer page backed by the price API endpoints."""
    return """
<!doctype html>
<html>
  <head>
    <title>Market Data Viewer</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 2rem; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #ddd; padding: 0.5rem; }
      th { background: #f4f4f4; text-align: left; }
    </style>
  </head>
  <body>
    <h1>Market Data Viewer</h1>
    <label for=\"ticker\">Ticker:</label>
    <input id=\"ticker\" value=\"AAPL\" />
    <button id=\"load\">Load</button>
    <p id=\"status\"></p>
    <table>
      <thead>
        <tr>
          <th>Timestamp</th>
          <th>Open</th>
          <th>High</th>
          <th>Low</th>
          <th>Close</th>
          <th>Volume</th>
        </tr>
      </thead>
      <tbody id=\"rows\"></tbody>
    </table>
    <script>
      async function loadData() {
        const ticker = document.getElementById('ticker').value;
        await fetch(`/api/data/price/historical/${ticker}?hours=1`, { method: 'POST' });
        const response = await fetch(`/api/data/price/${ticker}?hours=1`);
        const payload = await response.json();
        document.getElementById('status').innerText = `Loaded ${payload.count} row(s) for ${payload.ticker}`;
        const rows = document.getElementById('rows');
        rows.innerHTML = '';
        payload.prices.forEach((price) => {
          const row = document.createElement('tr');
          row.innerHTML = `
            <td>${price.timestamp}</td>
            <td>${price.open.toFixed(2)}</td>
            <td>${price.high.toFixed(2)}</td>
            <td>${price.low.toFixed(2)}</td>
            <td>${price.close.toFixed(2)}</td>
            <td>${price.volume.toFixed(0)}</td>
          `;
          rows.appendChild(row);
        });
      }
      document.getElementById('load').addEventListener('click', loadData);
      loadData();
    </script>
  </body>
</html>
"""
