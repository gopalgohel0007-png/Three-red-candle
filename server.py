"""Render entrypoint for RedScan.

Keeps the existing scanner algorithm in app.py unchanged while adding:
1. a minimum-price filter, and
2. explicit handling of today's daily candle.

Today's candle is included during the NSE session and after the session has
closed. If Yahoo's 1-day chart does not yet include today's daily row, we
recover today's opening/current candle from Yahoo's intraday chart instead of
silently falling back to the previous trading day.
"""

from datetime import datetime, time
from zoneinfo import ZoneInfo

import requests

import app as scanner_app
from price_filter import filter_matches_by_min_price


_original_scan_stocks = scanner_app.scan_stocks
_original_fetch_candles = scanner_app.fetch_candles
_IST = ZoneInfo("Asia/Kolkata")
_MARKET_OPEN = time(9, 15)
_MARKET_CLOSE = time(15, 30)
_YAHOO_HEADERS = getattr(scanner_app, "YAHOO_HEADERS", {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})


def _today_label():
    """Yahoo candle date label based on the NSE calendar day in IST."""
    return datetime.now(_IST).strftime("%d %b")


def _today_intraday_candle(sym, suffix, current_price):
    """Recover today's OHLC from Yahoo's intraday feed when its daily row is missing.

    Only the daily fields needed by the existing scanner (open/close and the
    derived red flag) are required here. The first regular-session 5-minute
    bar supplies today's open; the latest regular-session close supplies the
    fallback current price; the server's existing market price is preferred
    when available.
    """
    if not sym:
        return None

    ticker = scanner_app.yahoo_symbol(sym, suffix)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "5m", "range": "1d", "includePrePost": "false"}

    try:
        resp = scanner_app.SESSION.get(
            url, params=params, headers=_YAHOO_HEADERS, timeout=8
        )
        resp.raise_for_status()
        payload = resp.json()
        result = (payload.get("chart", {}).get("result") or [None])[0]
        if not result:
            return None

        timestamps = result.get("timestamp") or []
        quote = ((result.get("indicators") or {}).get("quote") or [None])[0]
        if not quote:
            return None

        today = datetime.now(_IST).date()
        regular_rows = []
        for i, ts in enumerate(timestamps):
            local_dt = datetime.fromtimestamp(ts, _IST)
            if local_dt.date() != today or local_dt.time() < _MARKET_OPEN:
                continue
            if local_dt.time() > _MARKET_CLOSE:
                continue

            opens = quote.get("open") or []
            closes = quote.get("close") or []
            highs = quote.get("high") or []
            lows = quote.get("low") or []
            if i >= len(opens) or opens[i] is None:
                continue
            regular_rows.append({
                "timestamp": ts,
                "open": float(opens[i]),
                "close": float(closes[i]) if i < len(closes) and closes[i] is not None else None,
                "high": float(highs[i]) if i < len(highs) and highs[i] is not None else None,
                "low": float(lows[i]) if i < len(lows) and lows[i] is not None else None,
            })

        if not regular_rows:
            return None

        first = regular_rows[0]
        valid_closes = [r["close"] for r in regular_rows if r["close"] is not None]
        valid_highs = [r["high"] for r in regular_rows if r["high"] is not None]
        valid_lows = [r["low"] for r in regular_rows if r["low"] is not None]
        close = float(current_price) if current_price is not None else (valid_closes[-1] if valid_closes else None)
        if close is None:
            return None

        return {
            "date": _today_label(),
            "open": round(first["open"], 2),
            "close": round(close, 2),
            "high": round(max(valid_highs), 2) if valid_highs else None,
            "low": round(min(valid_lows), 2) if valid_lows else None,
            "red": close < first["open"],
        }
    except (requests.RequestException, ValueError, KeyError, IndexError, TypeError):
        return None


def fetch_candles_with_today(*args, **kwargs):
    """Use today's candle whenever the scan is run on today's date.

    If the normal Yahoo daily response contains today's row, its close is
    refreshed from regularMarketPrice. If Yahoo has not published today's
    daily row yet, recover today's open/close from the intraday 5-minute feed.
    Outside market hours this still uses today's completed candle, rather than
    dropping back to the previous trading day.
    """
    data, err = _original_fetch_candles(*args, **kwargs)
    if err or not data:
        return data, err

    candles = list(data.get("candles") or [])
    current_price = data.get("price")
    if not candles:
        return data, err

    today = _today_label()
    if candles[-1].get("date") == today:
        if current_price is not None:
            latest = dict(candles[-1])
            latest["close"] = round(float(current_price), 2)
            latest["red"] = latest["close"] < latest["open"]
            candles[-1] = latest
        data["candles"] = candles
        return data, err

    # Today's daily row is missing. Recover it from the intraday feed so the
    # scanner never silently evaluates only through the previous trading day.
    sym = args[0] if args else kwargs.get("sym")
    suffix = kwargs.get("suffix", ".NS")
    if len(args) >= 3:
        suffix = args[2]
    elif len(args) >= 2 and isinstance(args[1], str):
        suffix = args[1]

    today_candle = _today_intraday_candle(sym, suffix, current_price)
    if today_candle:
        candles.append(today_candle)
        data["candles"] = candles
        data["price"] = today_candle["close"]

    return data, err


# app.scan() resolves scan_stocks/fetch_candles from the app module's globals.
# Replace fetch_candles first so the existing scan_stocks implementation uses
# today's candle without changing the scanner's pattern algorithm.
scanner_app.fetch_candles = fetch_candles_with_today


def scan_stocks_with_price_filter(*args, **kwargs):
    """Run the existing scanner unchanged, then filter its matches by price."""
    matched, failed, scanned = _original_scan_stocks(*args, **kwargs)
    return filter_matches_by_min_price(matched), failed, scanned


scanner_app.scan_stocks = scan_stocks_with_price_filter

app = scanner_app.app
