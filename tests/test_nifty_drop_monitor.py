import json
from datetime import datetime
from zoneinfo import ZoneInfo

import nifty_drop_monitor as mod

IST = ZoneInfo("Asia/Kolkata")


def test_five_minute_candle_alignment():
    ts = datetime(2026, 8, 10, 10, 17, 43, tzinfo=IST)
    assert mod.candle_start_for(ts) == datetime(2026, 8, 10, 10, 15, tzinfo=IST)


def test_threshold_sends_once(monkeypatch, tmp_path):
    sent = []

    class FakeNotifier:
        def __init__(self, token, chat_id):
            pass
        def send(self, text):
            sent.append(text)

    monkeypatch.setattr(mod, "TelegramNotifier", FakeNotifier)
    monitor = mod.NiftyDropMonitor(state_file=str(tmp_path / "state.json"))
    monitor.access_token = "token"
    monitor.telegram_token = "bot"
    monitor.telegram_chat_id = "chat"
    start = datetime(2026, 8, 10, 10, 15, tzinfo=IST)
    monitor._set_candle(start, 25520)

    monitor._evaluate(25490, start)
    assert len(sent) == 0
    monitor._evaluate(25470, start)
    assert len(sent) == 1
    monitor._evaluate(25465, start)
    assert len(sent) == 1
    assert json.loads((tmp_path / "state.json").read_text())["lastAlert"]["drop"] == 50


def test_new_candle_resets_alert_state(tmp_path):
    monitor = mod.NiftyDropMonitor(state_file=str(tmp_path / "state.json"))
    first = datetime(2026, 8, 10, 10, 15, tzinfo=IST)
    second = datetime(2026, 8, 10, 10, 20, tzinfo=IST)
    monitor.last_alert = {"candleStart": first.isoformat()}
    monitor._set_candle(first, 25520)
    assert monitor.candle.alerted is True
    monitor._set_candle(second, 25500)
    assert monitor.candle.alerted is False


def test_missing_credentials_are_graceful(monkeypatch):
    monkeypatch.setattr(mod, "upstox_client", object())
    monitor = mod.NiftyDropMonitor()
    monitor.access_token = monitor.telegram_token = monitor.telegram_chat_id = ""
    assert monitor.start() is False
