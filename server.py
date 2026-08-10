"""Render entrypoint for RedScan.

Keeps the existing scanner algorithm in app.py unchanged while adding:
1. a minimum-price filter, and
2. explicit inclusion of the latest NSE/BSE trading-session candle.

The latest session is determined from Yahoo's intraday feed rather than from
calendar date alone. This matters after market close: at 00:30 IST on Tuesday,
for example, the latest trading candle is Monday, not Tuesday (which has not
opened yet).
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
    """Calendar-day label in IST, retained for compatibility/tests."""
    return datetime.now(_IST).strftime("%d %b")


def _latest_intraday_candle(sym, suffix, current_price):
    """Recover the latest regular NSE/BSE trading-session candle.

    Do not assume that the calendar date is the trading date. When the market
    is closed after today's session, today's session is eligible. Before the
    market opens, the latest completed session is eligible. We therefore scan
    several days of 5-minute data and select the most recent regular-session
    date at or before the current IST time.
    """
    if not sym:
        return None

    ticker = scanner_app.yahoo_symbol(sym, suffix)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "5m", "range": "5d", "includePrePost": "false"}

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

        now = datetime.now(_IST)
        opens = quote.get("open") or []
        closes = quote.get("close") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []

        sessions = {}
        for i, ts in enumerate(timestamps):
            if i >= len(opens) or opens[i] is None:
                continue
            local_dt = datetime.fromtimestamp(ts, _IST)
            if local_dt.time() < _MARKET_OPEN or local_dt.time() > _MARKET_CLOSE:
                continue
            # Never use a future intraday bar. This is especially important
            # shortly after midnight IST, when the calendar date has changed
            # but the next NSE session has not started.
            if local_dt > now:
                continue

            session_date = local_dt.date()
            sessions.setdefault(session_date, []).append({
                "timestamp": ts,
                "open": float(opens[i]),
                "close": float(closes[i]) if i < len(closes) and closes[i] is not None else None,
                "high": float(highs[i]) if i < len(highs) and highs[i] is not None else None,
                "low": float(lows[i]) if i < len(lows) and lows[i] is not None else None,
            })

        if not sessions:
            return None

        session_date = max(sessions)
        rows = sessions[session_date]
        first = rows[0]
        valid_closes = [r["close"] for r in rows if r["close"] is not None]
        valid_highs = [r["high"] for r in rows if r["high"] is not None]
        valid_lows = [r["low"] for r in rows if r["low"] is not None]
        if not valid_closes and current_price is None:
            return None

        # During the live session, use the supplied regular-market price when
        # available. After close, Yahoo's latest intraday close is the proper
        # completed-session close.
        market_is_open = _MARKET_OPEN <= now.time() <= _MARKET_CLOSE
        if market_is_open and current_price is not None and session_date == now.date():
            close = float(current_price)
        else:
            close = valid_closes[-1] if valid_closes else float(current_price)

        return {
            "date": session_date.strftime("%d %b"),
            "open": round(first["open"], 2),
            "close": round(close, 2),
            "high": round(max(valid_highs), 2) if valid_highs else None,
            "low": round(min(valid_lows), 2) if valid_lows else None,
            "red": close < first["open"],
        }
    except (requests.RequestException, ValueError, KeyError, IndexError, TypeError):
        return None


def fetch_candles_with_today(*args, **kwargs):
    """Ensure the latest trading-session candle is present in scan data."""
    data, err = _original_fetch_candles(*args, **kwargs)
    if err or not data:
        return data, err

    candles = list(data.get("candles") or [])
    current_price = data.get("price")
    if not candles:
        return data, err

    sym = args[0] if args else kwargs.get("sym")
    suffix = kwargs.get("suffix", ".NS")
    if len(args) >= 3:
        suffix = args[2]
    elif len(args) >= 2 and isinstance(args[1], str):
        suffix = args[1]

    latest = _latest_intraday_candle(sym, suffix, current_price)
    if latest:
        latest_date = latest["date"]
        if candles[-1].get("date") == latest_date:
            # Intraday data is the authoritative current-session value. This
            # also repairs the daily Yahoo row when its close is stale.
            candles[-1] = latest
        else:
            # The daily feed may lag behind the latest trading session. Remove
            # any accidental duplicate of that date, then append the session.
            candles = [c for c in candles if c.get("date") != latest_date]
            candles.append(latest)
        data["candles"] = candles
        data["price"] = latest["close"]

    return data, err


# app.scan() resolves scan_stocks/fetch_candles from the app module's globals.
# Replace fetch_candles first so the existing scan_stocks implementation uses
# the latest trading-session candle without changing the scanner algorithm.
scanner_app.fetch_candles = fetch_candles_with_today


def scan_stocks_with_price_filter(*args, **kwargs):
    """Run the existing scanner unchanged, then filter its matches by price."""
    matched, failed, scanned = _original_scan_stocks(*args, **kwargs)
    return filter_matches_by_min_price(matched), failed, scanned


scanner_app.scan_stocks = scan_stocks_with_price_filter

app = scanner_app.app
