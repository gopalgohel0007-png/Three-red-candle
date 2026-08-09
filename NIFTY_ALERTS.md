# NIFTY 50 5-minute Telegram alerts

The backend now includes an isolated live NIFTY 50 monitor. The existing `app.py` scanner code and `/api/scan` implementation are unchanged.

## Market data

The monitor uses the Upstox Market Data Feed V3 WebSocket for live NIFTY 50 ticks. It subscribes to `NSE_INDEX|Nifty 50` in `full` mode and derives each five-minute candle from the live feed. On startup/reconnect it uses Upstox's V3 intraday 5-minute endpoint once to recover the current candle open when needed.

## Environment variables

Required for alerts:

```text
UPSTOX_ACCESS_TOKEN=your_upstox_access_token
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
```

Optional:

```text
NIFTY_DROP_THRESHOLD=50
NIFTY_DROP_MAX=60
NIFTY_ALERT_STATE_FILE=.nifty_alert_state.json
```

The hard alert condition is `drop >= NIFTY_DROP_THRESHOLD`. `NIFTY_DROP_MAX` documents the preferred 50-60 point band; a fast move through 60 still alerts rather than being missed.

Do not commit tokens, `.env` files, or the state file. Configure secrets in the deployment platform's environment-variable/secret settings.

## Status endpoint

`GET /api/nifty-status` returns the current monitoring state, including connection status, market status, candle open/start/end, current price, drop points, and the last Telegram alert.

## Alert behavior

- NSE cash-market session: 09:15-15:30 IST, weekdays.
- Alert threshold is evaluated on every live tick while the five-minute candle is forming.
- Only one successful Telegram alert is sent per candle.
- The alert state resets when the next five-minute candle begins.
- Telegram failures are logged and retried on later ticks instead of marking the candle as alerted.
- The monitor uses the Upstox WebSocket rather than polling REST every few seconds.
- Upstox WebSocket reconnect handling is enabled.
- A small local state file prevents the same candle from being re-alerted after an application restart when the file is on persistent storage.
- If credentials are missing, the monitor disables itself cleanly and the scanner remains available.
