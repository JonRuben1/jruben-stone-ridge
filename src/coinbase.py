"""Fetch recent price observations from Coinbase Exchange.

Public market data does not require auth. The endpoint returns trades but we
only project down to (time, price) because that's all the spec asks for.
"""
from typing import Iterator

import requests

BASE_URL = "https://api.exchange.coinbase.com"
TIMEOUT = 10


def fetch_prices(product_id: str, pages: int = 1) -> Iterator[dict]:
    """Yield {"time": str, "price": str} dicts for `product_id`.

    Pages backwards in time via the `after` cursor, up to `pages` pages.
    Coinbase returns at most 1000 trades per page.
    """
    cursor = None
    for _ in range(pages):
        params = {}
        if cursor is not None:
            params["after"] = cursor
        resp = requests.get(
            f"{BASE_URL}/products/{product_id}/trades",
            params=params,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        trades = resp.json()
        if not trades:
            return
        for t in trades:
            yield {"time": t["time"], "price": t["price"]}
        cursor = trades[-1]["trade_id"]
