"""
RedScan Backend — NSE F&O Consecutive Red Candle Screener
-----------------------------------------------------------
This Flask server fetches daily OHLC data from Yahoo Finance
(server-to-server, so no CORS issues) and returns stocks that
have N+ consecutive red candles (Close < Open) on the daily
timeframe, filtered to F&O-eligible index constituents.

Endpoints:
  GET /                -> health check
  GET /api/indices     -> list of available indices
  GET /api/scan        -> run the scan
      query params:
        index   = nifty50 | banknifty | nifty100 | sensex   (default nifty50)
        minRed  = integer, minimum consecutive red candles   (default 3)
"""

from flask import Flask, jsonify, request
import requests
from datetime import datetime
import time

app = Flask(__name__)


@app.after_request
def add_cors_headers(response):
    """Allow requests from any frontend (GitHub Pages, etc.) without needing flask-cors."""
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response

# ──────────────────────────────────────────────────────────
# Static index constituent lists (F&O eligible NSE stocks)
# ──────────────────────────────────────────────────────────
INDICES = {
    "nifty50": {
        "label": "Nifty 50",
        "stocks": [
            {"s": "RELIANCE", "n": "Reliance Industries"},
            {"s": "TCS", "n": "Tata Consultancy Services"},
            {"s": "HDFCBANK", "n": "HDFC Bank"},
            {"s": "ICICIBANK", "n": "ICICI Bank"},
            {"s": "INFY", "n": "Infosys"},
            {"s": "HINDUNILVR", "n": "Hindustan Unilever"},
            {"s": "ITC", "n": "ITC"},
            {"s": "SBIN", "n": "State Bank of India"},
            {"s": "BHARTIARTL", "n": "Bharti Airtel"},
            {"s": "KOTAKBANK", "n": "Kotak Mahindra Bank"},
            {"s": "AXISBANK", "n": "Axis Bank"},
            {"s": "LT", "n": "Larsen & Toubro"},
            {"s": "HCLTECH", "n": "HCL Technologies"},
            {"s": "WIPRO", "n": "Wipro"},
            {"s": "ASIANPAINT", "n": "Asian Paints"},
            {"s": "MARUTI", "n": "Maruti Suzuki"},
            {"s": "NTPC", "n": "NTPC"},
            {"s": "POWERGRID", "n": "Power Grid Corp"},
            {"s": "SUNPHARMA", "n": "Sun Pharma"},
            {"s": "ULTRACEMCO", "n": "UltraTech Cement"},
            {"s": "TATAMOTORS", "n": "Tata Motors"},
            {"s": "ONGC", "n": "ONGC"},
            {"s": "BAJFINANCE", "n": "Bajaj Finance"},
            {"s": "BAJAJFINSV", "n": "Bajaj Finserv"},
            {"s": "TECHM", "n": "Tech Mahindra"},
            {"s": "TITAN", "n": "Titan Company"},
            {"s": "NESTLEIND", "n": "Nestle India"},
            {"s": "DIVISLAB", "n": "Divi's Laboratories"},
            {"s": "DRREDDY", "n": "Dr Reddy's Labs"},
            {"s": "CIPLA", "n": "Cipla"},
            {"s": "APOLLOHOSP", "n": "Apollo Hospitals"},
            {"s": "ADANIPORTS", "n": "Adani Ports"},
            {"s": "ADANIENT", "n": "Adani Enterprises"},
            {"s": "JSWSTEEL", "n": "JSW Steel"},
            {"s": "TATASTEEL", "n": "Tata Steel"},
            {"s": "HINDALCO", "n": "Hindalco Industries"},
            {"s": "COALINDIA", "n": "Coal India"},
            {"s": "BPCL", "n": "Bharat Petroleum"},
            {"s": "INDIGO", "n": "InterGlobe Aviation"},
            {"s": "HDFCLIFE", "n": "HDFC Life Insurance"},
            {"s": "SBILIFE", "n": "SBI Life Insurance"},
            {"s": "BAJAJ-AUTO", "n": "Bajaj Auto"},
            {"s": "HEROMOTOCO", "n": "Hero MotoCorp"},
            {"s": "EICHERMOT", "n": "Eicher Motors"},
            {"s": "M&M", "n": "Mahindra & Mahindra"},
            {"s": "TATAPOWER", "n": "Tata Power"},
            {"s": "GRASIM", "n": "Grasim Industries"},
            {"s": "SHREECEM", "n": "Shree Cement"},
            {"s": "BRITANNIA", "n": "Britannia Industries"},
            {"s": "IOC", "n": "Indian Oil Corp"},
        ],
    },
    "banknifty": {
        "label": "Nifty Bank",
        "stocks": [
            {"s": "HDFCBANK", "n": "HDFC Bank"},
            {"s": "ICICIBANK", "n": "ICICI Bank"},
            {"s": "KOTAKBANK", "n": "Kotak Mahindra Bank"},
            {"s": "AXISBANK", "n": "Axis Bank"},
            {"s": "SBIN", "n": "State Bank of India"},
            {"s": "BANKBARODA", "n": "Bank of Baroda"},
            {"s": "PNB", "n": "Punjab National Bank"},
            {"s": "INDUSINDBK", "n": "IndusInd Bank"},
            {"s": "FEDERALBNK", "n": "Federal Bank"},
            {"s": "IDFCFIRSTB", "n": "IDFC First Bank"},
            {"s": "BANDHANBNK", "n": "Bandhan Bank"},
            {"s": "AUBANK", "n": "AU Small Finance Bank"},
        ],
    },
    "nifty100": {
        "label": "Nifty 100",
        "stocks": [
            {"s": "RELIANCE", "n": "Reliance Industries"},
            {"s": "TCS", "n": "TCS"},
            {"s": "HDFCBANK", "n": "HDFC Bank"},
            {"s": "ICICIBANK", "n": "ICICI Bank"},
            {"s": "INFY", "n": "Infosys"},
            {"s": "HINDUNILVR", "n": "HUL"},
            {"s": "ITC", "n": "ITC"},
            {"s": "SBIN", "n": "SBI"},
            {"s": "BHARTIARTL", "n": "Bharti Airtel"},
            {"s": "KOTAKBANK", "n": "Kotak Bank"},
            {"s": "AXISBANK", "n": "Axis Bank"},
            {"s": "LT", "n": "L&T"},
            {"s": "HCLTECH", "n": "HCL Tech"},
            {"s": "WIPRO", "n": "Wipro"},
            {"s": "ASIANPAINT", "n": "Asian Paints"},
            {"s": "MARUTI", "n": "Maruti"},
            {"s": "NTPC", "n": "NTPC"},
            {"s": "POWERGRID", "n": "Power Grid"},
            {"s": "SUNPHARMA", "n": "Sun Pharma"},
            {"s": "ULTRACEMCO", "n": "UltraTech"},
            {"s": "TATAMOTORS", "n": "Tata Motors"},
            {"s": "ONGC", "n": "ONGC"},
            {"s": "BAJFINANCE", "n": "Bajaj Finance"},
            {"s": "BAJAJFINSV", "n": "Bajaj Finserv"},
            {"s": "TECHM", "n": "Tech Mahindra"},
            {"s": "TITAN", "n": "Titan"},
            {"s": "NESTLEIND", "n": "Nestle"},
            {"s": "DIVISLAB", "n": "Divi's Labs"},
            {"s": "DRREDDY", "n": "Dr Reddy's"},
            {"s": "CIPLA", "n": "Cipla"},
            {"s": "APOLLOHOSP", "n": "Apollo Hospitals"},
            {"s": "ADANIPORTS", "n": "Adani Ports"},
            {"s": "ADANIENT", "n": "Adani Ent"},
            {"s": "JSWSTEEL", "n": "JSW Steel"},
            {"s": "TATASTEEL", "n": "Tata Steel"},
            {"s": "HINDALCO", "n": "Hindalco"},
            {"s": "COALINDIA", "n": "Coal India"},
            {"s": "BPCL", "n": "BPCL"},
            {"s": "INDIGO", "n": "IndiGo"},
            {"s": "HDFCLIFE", "n": "HDFC Life"},
            {"s": "SBILIFE", "n": "SBI Life"},
            {"s": "BAJAJ-AUTO", "n": "Bajaj Auto"},
            {"s": "HEROMOTOCO", "n": "Hero Moto"},
            {"s": "EICHERMOT", "n": "Eicher Motors"},
            {"s": "M&M", "n": "M&M"},
            {"s": "TATAPOWER", "n": "Tata Power"},
            {"s": "GRASIM", "n": "Grasim"},
            {"s": "SHREECEM", "n": "Shree Cement"},
            {"s": "BRITANNIA", "n": "Britannia"},
            {"s": "IOC", "n": "Indian Oil"},
            {"s": "PIDILITIND", "n": "Pidilite Industries"},
            {"s": "SIEMENS", "n": "Siemens"},
            {"s": "HAVELLS", "n": "Havells India"},
            {"s": "VOLTAS", "n": "Voltas"},
            {"s": "DABUR", "n": "Dabur India"},
            {"s": "MARICO", "n": "Marico"},
            {"s": "GODREJCP", "n": "Godrej Consumer"},
            {"s": "LUPIN", "n": "Lupin"},
            {"s": "AUROPHARMA", "n": "Aurobindo Pharma"},
            {"s": "TORNTPHARM", "n": "Torrent Pharma"},
            {"s": "ZOMATO", "n": "Zomato (Eternal)"},
            {"s": "NAUKRI", "n": "Info Edge (Naukri)"},
            {"s": "IRCTC", "n": "IRCTC"},
            {"s": "CHOLAFIN", "n": "Cholamandalam Finance"},
            {"s": "FEDERALBNK", "n": "Federal Bank"},
            {"s": "IDFCFIRSTB", "n": "IDFC First Bank"},
            {"s": "INDUSINDBK", "n": "IndusInd Bank"},
            {"s": "PFC", "n": "Power Finance Corp"},
            {"s": "RECLTD", "n": "REC Limited"},
            {"s": "HAL", "n": "Hindustan Aeronautics"},
            {"s": "BEL", "n": "Bharat Electronics"},
            {"s": "TATAELXSI", "n": "Tata Elxsi"},
            {"s": "BERGEPAINT", "n": "Berger Paints"},
            {"s": "COLPAL", "n": "Colgate-Palmolive"},
            {"s": "DLF", "n": "DLF"},
            {"s": "BANKBARODA", "n": "Bank of Baroda"},
            {"s": "PNB", "n": "Punjab National Bank"},
            {"s": "MUTHOOTFIN", "n": "Muthoot Finance"},
            {"s": "NMDC", "n": "NMDC"},
            {"s": "VEDL", "n": "Vedanta"},
            {"s": "SAIL", "n": "SAIL"},
            {"s": "GAIL", "n": "GAIL India"},
        ],
    },
    "sensex": {
        "label": "Sensex 30",
        "stocks": [
            {"s": "RELIANCE", "n": "Reliance Industries"},
            {"s": "TCS", "n": "TCS"},
            {"s": "HDFCBANK", "n": "HDFC Bank"},
            {"s": "ICICIBANK", "n": "ICICI Bank"},
            {"s": "INFY", "n": "Infosys"},
            {"s": "HINDUNILVR", "n": "HUL"},
            {"s": "ITC", "n": "ITC"},
            {"s": "SBIN", "n": "SBI"},
            {"s": "BHARTIARTL", "n": "Bharti Airtel"},
            {"s": "KOTAKBANK", "n": "Kotak Bank"},
            {"s": "AXISBANK", "n": "Axis Bank"},
            {"s": "LT", "n": "L&T"},
            {"s": "HCLTECH", "n": "HCL Tech"},
            {"s": "WIPRO", "n": "Wipro"},
            {"s": "ASIANPAINT", "n": "Asian Paints"},
            {"s": "MARUTI", "n": "Maruti"},
            {"s": "NTPC", "n": "NTPC"},
            {"s": "SUNPHARMA", "n": "Sun Pharma"},
            {"s": "ULTRACEMCO", "n": "UltraTech"},
            {"s": "TATAMOTORS", "n": "Tata Motors"},
            {"s": "BAJFINANCE", "n": "Bajaj Finance"},
            {"s": "BAJAJFINSV", "n": "Bajaj Finserv"},
            {"s": "TITAN", "n": "Titan"},
            {"s": "NESTLEIND", "n": "Nestle"},
            {"s": "DRREDDY", "n": "Dr Reddy's"},
            {"s": "ADANIENT", "n": "Adani Ent"},
            {"s": "JSWSTEEL", "n": "JSW Steel"},
            {"s": "TATASTEEL", "n": "Tata Steel"},
            {"s": "POWERGRID", "n": "Power Grid"},
            {"s": "INDIGO", "n": "IndiGo"},
        ],
    },
}

YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def yahoo_symbol(sym: str) -> str:
    """Convert internal symbol to Yahoo Finance NSE ticker."""
    # Yahoo uses no special chars; M&M -> M%26M doesn't work, Yahoo's actual ticker is M&M.NS
    return sym.replace("&", "%26") + ".NS"


def fetch_candles(sym: str, days: int = 15):
    """Fetch daily OHLC candles for a symbol from Yahoo Finance."""
    ticker = yahoo_symbol(sym)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1d", "range": f"{days}d"}

    try:
        resp = requests.get(url, params=params, headers=YAHOO_HEADERS, timeout=8)
        resp.raise_for_status()
        data = resp.json()

        result = data.get("chart", {}).get("result")
        if not result:
            err = data.get("chart", {}).get("error", {})
            return None, err.get("description", "No data returned")

        r = result[0]
        meta = r.get("meta", {})
        quote = r["indicators"]["quote"][0]
        timestamps = r.get("timestamp", [])

        candles = []
        for i, ts in enumerate(timestamps):
            o = quote["open"][i]
            c = quote["close"][i]
            if o is None or c is None:
                continue
            candles.append({
                "date": datetime.utcfromtimestamp(ts).strftime("%d %b"),
                "open": round(o, 2),
                "close": round(c, 2),
                "red": c < o,
            })

        return {
            "price": meta.get("regularMarketPrice"),
            "prevClose": meta.get("previousClose") or meta.get("chartPreviousClose"),
            "candles": candles,
        }, None

    except requests.exceptions.RequestException as e:
        return None, str(e)
    except (KeyError, IndexError, ValueError) as e:
        return None, f"Parse error: {e}"


def trailing_red_streak(candles):
    """Count consecutive red candles ending at the most recent candle."""
    count = 0
    for c in reversed(candles):
        if c["red"]:
            count += 1
        else:
            break
    return count


@app.route("/")
def health():
    return jsonify({
        "status": "ok",
        "service": "RedScan Backend",
        "endpoints": ["/api/indices", "/api/scan?index=nifty50&minRed=3"],
    })


@app.route("/api/indices")
def list_indices():
    return jsonify({
        "indices": [
            {"key": k, "label": v["label"], "count": len(v["stocks"])}
            for k, v in INDICES.items()
        ]
    })


@app.route("/api/scan")
def scan():
    index_key = request.args.get("index", "nifty50")
    try:
        min_red = int(request.args.get("minRed", 3))
    except ValueError:
        min_red = 3

    if index_key not in INDICES:
        return jsonify({"error": f"Unknown index '{index_key}'", "available": list(INDICES.keys())}), 400

    stocks = INDICES[index_key]["stocks"]
    matched = []
    failed = []
    scanned = 0

    for stock in stocks:
        data, err = fetch_candles(stock["s"])
        scanned += 1

        if err:
            failed.append({"symbol": stock["s"], "reason": err})
            continue

        streak = trailing_red_streak(data["candles"])
        if streak >= min_red:
            price = data["price"]
            prev = data["prevClose"]
            change_pct = ((price - prev) / prev * 100) if (price and prev) else None
            matched.append({
                "symbol": stock["s"],
                "name": stock["n"],
                "price": price,
                "changePct": round(change_pct, 2) if change_pct is not None else None,
                "redStreak": streak,
                "candles": data["candles"][-5:],
                "fnoEligible": True,
                "indexMember": INDICES[index_key]["label"],
            })

        # Be polite to Yahoo — tiny delay between requests
        time.sleep(0.05)

    matched.sort(key=lambda x: (-x["redStreak"], x["changePct"] or 0))

    return jsonify({
        "index": INDICES[index_key]["label"],
        "minRedCandles": min_red,
        "scanTime": datetime.utcnow().strftime("%d %b %Y, %H:%M UTC"),
        "totalScanned": scanned,
        "totalFailed": len(failed),
        "totalMatched": len(matched),
        "results": matched,
        "failed": failed,
    })


if __name__ == "__main__":
    app.run(debug=True, port=5000)
