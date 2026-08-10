"""Render entrypoint for RedScan.

Keeps the existing scanner algorithm in app.py unchanged while adding:
1. a minimum-price filter, and
2. explicit handling of today's live daily candle during the NSE session.
"""

from datetime import datetime, time
from zoneinfo import ZoneInfo

import app as scanner_app
from price_filter import filter_matches_by_min_price


_original_scan_stocks = scanner_app.scan_stocks
_original_fetch_candles = scanner_app.fetch_candles
_IST = ZoneInfo("Asia/Kolkata")
_MARKET_OPEN = time(9, 15)
_MARKET_CLOSE = time(15, 30)


def _nse_session_is_open_now():
    """Return True only during NSE weekday trading hours in IST."""
    now = datetime.now(_IST)
    return now.weekday() < 5 and _MARKET_OPEN <= now.time() <= _MARKET_CLOSE


def _today_label():
    """Yahoo candle date format used by app.py, based on the NSE calendar day."""
    return datetime.now(_IST).strftime("%d %b")


def fetch_candles_with_today(*args, **kwargs):
    """Use the existing Yahoo daily candles, but make today's live price the
    current candle close while the NSE session is running.

    This does not alter the staircase algorithm in app.py. It only ensures
    the most recent candle is explicitly evaluated using the live market
    price for today's session. Outside market hours the original daily
    candle data is left untouched.
    """
    data, err = _original_fetch_candles(*args, **kwargs)
    if err or not data or not _nse_session_is_open_now():
        return data, err

    candles = data.get("candles") or []
    current_price = data.get("price")
    if not candles or current_price is None:
        return data, err

    today = _today_label()
    if candles[-1].get("date") == today:
        latest = dict(candles[-1])
        latest["close"] = round(float(current_price), 2)
        latest["red"] = latest["close"] < latest["open"]
        candles[-1] = latest
        data["candles"] = candles

    return data, err


# app.scan() resolves scan_stocks from the app module's global namespace.
# Replace fetch_candles first so the existing scan_stocks implementation uses
# today's live daily close without changing the scanner's pattern algorithm.
scanner_app.fetch_candles = fetch_candles_with_today


def scan_stocks_with_price_filter(*args, **kwargs):
    """Run the existing scanner unchanged, then filter its matches by price."""
    matched, failed, scanned = _original_scan_stocks(*args, **kwargs)
    return filter_matches_by_min_price(matched), failed, scanned


scanner_app.scan_stocks = scan_stocks_with_price_filter

app = scanner_app.app
