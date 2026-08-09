from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as clock_time, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests

try:
    import upstox_client
except ImportError:
    upstox_client = None

IST = ZoneInfo("Asia/Kolkata")
MARKET_OPEN = clock_time(9, 15)
MARKET_CLOSE = clock_time(15, 30)
INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
log = logging.getLogger("nifty_drop_monitor")


@dataclass
class CandleState:
    start: datetime
    open: float
    alerted: bool = False


def is_market_open(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(IST)
    return now.weekday() < 5 and MARKET_OPEN <= now.time() < MARKET_CLOSE


def candle_start_for(ts: datetime) -> datetime:
    return ts.replace(minute=ts.minute - ts.minute % 5, second=0, microsecond=0)


def _dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        n = int(value)
        return datetime.fromtimestamp(n / (1000 if n > 10_000_000_000 else 1), tz=IST)
    except (TypeError, ValueError, OSError):
        try:
            parsed = datetime.fromisoformat(str(value))
            return parsed.astimezone(IST) if parsed.tzinfo else parsed.replace(tzinfo=IST)
        except (TypeError, ValueError):
            return None


def _dict(message: Any) -> dict:
    if isinstance(message, dict):
        return message
    if isinstance(message, str):
        try:
            value = json.loads(message)
            return value if isinstance(value, dict) else {}
        except json.JSONDecodeError:
            return {}
    if hasattr(message, "to_dict"):
        value = message.to_dict()
        return value if isinstance(value, dict) else {}
    return {}


def _walk(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def extract_ltp(message: Any) -> Optional[float]:
    for obj in _walk(_dict(message)):
        ltpc = obj.get("ltpc")
        if isinstance(ltpc, dict) and ltpc.get("ltp") is not None:
            try:
                price = float(ltpc["ltp"])
                if price > 0:
                    return price
            except (TypeError, ValueError):
                pass
    return None


def extract_tick_time(message: Any) -> datetime:
    data = _dict(message)
    for obj in _walk(data):
        for key in ("currentTs", "ltt"):
            if key in obj:
                value = _dt(obj[key])
                if value:
                    return value
    return datetime.now(IST)


def extract_one_minute_open(message: Any, target: datetime) -> Optional[float]:
    candidates = []
    for obj in _walk(_dict(message)):
        for candle in obj.get("ohlc", []) if isinstance(obj.get("ohlc"), list) else []:
            if not isinstance(candle, dict) or candle.get("interval") != "I1":
                continue
            ts = _dt(candle.get("ts"))
            try:
                opening = float(candle["open"])
            except (KeyError, TypeError, ValueError):
                continue
            if ts and candle_start_for(ts) == target and opening > 0:
                candidates.append((ts, opening))
    return min(candidates)[1] if candidates else None


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id

    def send(self, text: str) -> None:
        response = requests.post(
            f"https://api.telegram.org/bot{self.token}/sendMessage",
            json={"chat_id": self.chat_id, "text": text},
            timeout=10,
        )
        response.raise_for_status()
        if not response.json().get("ok"):
            raise RuntimeError("Telegram API returned ok=false")


class NiftyDropMonitor:
    """Isolated live NIFTY 50 5-minute drop monitor."""

    def __init__(self, state_file: Optional[str] = None):
        self.access_token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.threshold = float(os.getenv("NIFTY_DROP_THRESHOLD", "50"))
        self.max_drop = float(os.getenv("NIFTY_DROP_MAX", "60"))
        self.state_file = Path(state_file or os.getenv("NIFTY_ALERT_STATE_FILE", ".nifty_alert_state.json"))
        self.candle: Optional[CandleState] = None
        self.current_price: Optional[float] = None
        self.market_status = "UNKNOWN"
        self.connected = False
        self.last_error: Optional[str] = None
        self.last_alert: Optional[dict] = None
        self._streamer = None
        self._started = False
        self._lock = threading.Lock()
        self._load_state()

    @property
    def configured(self) -> bool:
        return bool(self.access_token and self.telegram_token and self.telegram_chat_id and upstox_client)

    @property
    def status(self) -> dict:
        with self._lock:
            candle = self.candle
            drop = candle.open - self.current_price if candle and self.current_price is not None else None
            return {
                "enabled": self.configured,
                "connected": self.connected,
                "monitoring": self.connected and self.market_status == "NORMAL_OPEN" and is_market_open(),
                "marketStatus": self.market_status,
                "currentPrice": self.current_price,
                "candleOpen": candle.open if candle else None,
                "candleStart": candle.start.isoformat() if candle else None,
                "candleEnd": (candle.start + timedelta(minutes=5)).isoformat() if candle else None,
                "dropPoints": round(drop, 2) if drop is not None else None,
                "alertedThisCandle": candle.alerted if candle else False,
                "lastAlert": self.last_alert,
                "lastError": self.last_error,
                "threshold": self.threshold,
                "maxDrop": self.max_drop,
            }

    def _load_state(self):
        try:
            if self.state_file.exists():
                self.last_alert = json.loads(self.state_file.read_text()).get("lastAlert")
        except Exception as exc:
            log.warning("Could not load NIFTY alert state: %s", exc)

    def _save_state(self):
        if not self.last_alert:
            return
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_file.with_suffix(".tmp")
            tmp.write_text(json.dumps({"lastAlert": self.last_alert}, indent=2))
            tmp.replace(self.state_file)
        except Exception as exc:
            log.warning("Could not save NIFTY alert state: %s", exc)

    def _set_candle(self, start: datetime, opening: float):
        alerted = bool(self.last_alert and self.last_alert.get("candleStart") == start.isoformat())
        with self._lock:
            self.candle = CandleState(start, float(opening), alerted)

    def _recover_current_candle(self, now: Optional[datetime] = None):
        now = now or datetime.now(IST)
        if not is_market_open(now):
            return
        url = "https://api.upstox.com/v3/historical-candle/intraday/NSE_INDEX%7CNifty%2050/minutes/5"
        try:
            response = requests.get(
                url,
                headers={"Accept": "application/json", "Authorization": f"Bearer {self.access_token}"},
                timeout=10,
            )
            response.raise_for_status()
            candles = response.json().get("data", {}).get("candles", [])
            if candles:
                start = _dt(candles[0][0])
                opening = float(candles[0][1])
                if start and start <= now < start + timedelta(minutes=5):
                    self._set_candle(start, opening)
        except Exception as exc:
            self.last_error = f"Candle recovery failed: {exc}"
            log.warning(self.last_error)

    def _on_open(self, *_):
        self.connected = True
        self.last_error = None
        try:
            self._streamer.subscribe([INSTRUMENT_KEY], "full")
        except Exception as exc:
            self.last_error = f"Subscription failed: {exc}"
            log.exception(self.last_error)
        self._recover_current_candle()

    def _on_close(self, *_):
        self.connected = False
        log.warning("NIFTY WebSocket disconnected")

    def _on_error(self, error):
        self.connected = False
        self.last_error = str(error)
        log.error("NIFTY WebSocket error: %s", error)

    def _on_reconnecting(self, *_):
        self.connected = False
        log.warning("NIFTY WebSocket reconnecting")

    def _on_message(self, message):
        data = _dict(message)
        if data.get("type") == "market_info":
            statuses = data.get("marketInfo", {}).get("segmentStatus", {})
            self.market_status = statuses.get("NSE_INDEX", "UNKNOWN")
            return
        price = extract_ltp(message)
        if price is None:
            return
        tick_time = extract_tick_time(message)
        if not is_market_open(tick_time):
            return
        start = candle_start_for(tick_time)
        with self._lock:
            self.current_price = price
            existing = self.candle
        if existing is None or existing.start != start:
            opening = extract_one_minute_open(message, start)
            if opening is None:
                self._recover_current_candle(tick_time)
                with self._lock:
                    existing = self.candle
                if existing is None or existing.start != start:
                    opening = price
            self._set_candle(start, opening)
        self._evaluate(price, start)

    def _evaluate(self, price: float, start: datetime):
        with self._lock:
            candle = self.candle
            if not candle or candle.start != start or candle.alerted:
                return
            drop = candle.open - price
            if drop < self.threshold:
                return
            # 50-60 is the preferred band. A gap through 60 still alerts because
            # the hard requirement is >= threshold and there may be no tick in-band.
            text = self._alert_text(candle, price, drop)
        try:
            TelegramNotifier(self.telegram_token, self.telegram_chat_id).send(text)
        except Exception as exc:
            self.last_error = f"Telegram alert failed: {exc}"
            log.error(self.last_error)
            return
        with self._lock:
            if self.candle and self.candle.start == start:
                self.candle.alerted = True
                self.last_alert = {
                    "candleStart": start.isoformat(),
                    "candleEnd": (start + timedelta(minutes=5)).isoformat(),
                    "open": candle.open,
                    "current": price,
                    "drop": round(drop, 2),
                    "sentAt": datetime.now(IST).isoformat(),
                }
        self._save_state()

    @staticmethod
    def _alert_text(candle: CandleState, price: float, drop: float) -> str:
        end = candle.start + timedelta(minutes=5)
        return (
            "🚨 NIFTY 50 FAST DROP\n\n"
            f"5-Min Candle: {candle.start:%H:%M} – {end:%H:%M}\n"
            f"Open: {candle.open:,.2f}\n"
            f"Current: {price:,.2f}\n"
            f"Drop: {drop:,.2f} points\n"
            "Status: Candle still running"
        )

    def start(self) -> bool:
        if self._started:
            return self.configured
        self._started = True
        if not self.configured:
            missing = []
            for name, value in (("UPSTOX_ACCESS_TOKEN", self.access_token), ("TELEGRAM_BOT_TOKEN", self.telegram_token), ("TELEGRAM_CHAT_ID", self.telegram_chat_id)):
                if not value:
                    missing.append(name)
            if upstox_client is None:
                missing.append("upstox-python-sdk")
            log.warning("NIFTY monitor disabled; missing: %s", ", ".join(missing))
            return False
        threading.Thread(target=self._run, name="nifty-drop-monitor", daemon=True).start()
        return True

    def _run(self):
        configuration = upstox_client.Configuration()
        configuration.access_token = self.access_token
        self._streamer = upstox_client.MarketDataStreamerV3(upstox_client.ApiClient(configuration), [INSTRUMENT_KEY], "full")
        self._streamer.auto_reconnect(True, 10, 0)
        self._streamer.on("open", self._on_open)
        self._streamer.on("message", self._on_message)
        self._streamer.on("error", self._on_error)
        self._streamer.on("close", self._on_close)
        self._streamer.on("reconnecting", self._on_reconnecting)
        while True:
            try:
                self._streamer.connect()
                return
            except Exception as exc:
                self.connected = False
                self.last_error = str(exc)
                log.exception("NIFTY WebSocket connection failed: %s", exc)
                time.sleep(10)
