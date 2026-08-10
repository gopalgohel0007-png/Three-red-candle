"""Render entrypoint that adds the minimum-price filter without changing app.py."""

import app as scanner_app
from price_filter import filter_matches_by_min_price


_original_scan_stocks = scanner_app.scan_stocks


def scan_stocks_with_price_filter(*args, **kwargs):
    """Run the existing scanner unchanged, then filter its matches by price."""
    matched, failed, scanned = _original_scan_stocks(*args, **kwargs)
    return filter_matches_by_min_price(matched), failed, scanned


# app.scan() resolves scan_stocks from the app module's global namespace.
# Replacing that reference here keeps the existing scanner algorithm intact.
scanner_app.scan_stocks = scan_stocks_with_price_filter

app = scanner_app.app
