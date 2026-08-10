"""Optional post-scan price filter for the RedScan backend.

Kept separate from app.py so the existing three-red-candle detection
algorithm is not modified. The filter removes matched stocks whose latest
price is below MIN_STOCK_PRICE.
"""

import os

DEFAULT_MIN_STOCK_PRICE = 1000.0


def get_min_stock_price():
    """Return the configured minimum stock price, defaulting to 1000."""
    raw = os.getenv("MIN_STOCK_PRICE", str(DEFAULT_MIN_STOCK_PRICE)).strip()
    try:
        value = float(raw)
        if value < 0:
            raise ValueError
        return value
    except (TypeError, ValueError):
        return DEFAULT_MIN_STOCK_PRICE


def filter_matches_by_min_price(matches, min_price=None):
    """Keep only scan matches whose latest price is >= min_price."""
    threshold = get_min_stock_price() if min_price is None else float(min_price)
    filtered = []
    for match in matches:
        try:
            price = float(match.get("price"))
        except (TypeError, ValueError):
            continue
        if price >= threshold:
            filtered.append(match)
    return filtered
