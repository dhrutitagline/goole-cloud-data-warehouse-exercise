import requests
from datetime import datetime as dt
from datetime import UTC

def fetch_crypto_data():
    """
    Fetch detailed cryptocurrency data from CoinGecko.
    Returns a list of dicts ready for BigQuery insertion.
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    # Prepare data for BigQuery
    rows = []
    fetched_at = dt.now(UTC).isoformat()
    for item in data:
        rows.append({
            "id": item["id"],
            "Symbol": item["symbol"],
            "name": item["name"],
            "current_price": item.get("current_price"),
            "market_cap": item.get("market_cap"),
            "price_change_percentage_24h": item.get("price_change_percentage_24h"),
            "last_updated": item.get("last_updated"),
            "fetched_at": fetched_at
        })
    return rows

    
