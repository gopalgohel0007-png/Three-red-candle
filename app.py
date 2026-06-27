"""
RedScan Backend — NSE F&O Consecutive Red Candle Screener
-----------------------------------------------------------
This Flask server fetches daily OHLC data from Yahoo Finance
(server-to-server, so no CORS issues) and returns stocks that
have N+ candles forming a DESCENDING STAIRCASE pattern on the
daily timeframe (each red candle's body lower than the previous
one's), filtered to F&O-eligible index constituents.

Pattern definition (matches the reference screenshot):
  - Each candle is RED (close < open)
  - Each subsequent candle's open AND close are both lower than
    the previous candle's open AND close (a "lower body" — like
    descending stairs), not just a same-direction red streak.

Endpoints:
  GET /                -> health check
  GET /api/indices     -> list of available indices
  GET /api/scan         -> run the scan
      query params:
        index   = nifty50 | banknifty | nifty100 | sensex | fno | allstocks   (default nifty50)
        minRed  = integer, minimum candles in the staircase pattern (default 3)

"allstocks" scans every NSE-listed equity (~2000 symbols) instead of
just an index/F&O list. Each result is tagged with fnoEligible: true/false
so the trader can see which staircase matches are also tradeable via options
(NSE F&O currently covers ~180-220 of those ~2000 names — options trading is
only available on F&O-eligible stocks, there's no separate larger "options
list"). Because this scans a much bigger universe, it uses a small thread
pool to fetch candles concurrently rather than one-by-one.
"""

from flask import Flask, jsonify, request
import requests
import certifi
from datetime import datetime
import time
import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

# Force requests to use certifi's CA bundle explicitly instead of relying
# on the system/Python default CA path resolution. This avoids SSLError /
# load_verify_locations failures that can show up after a Python version
# bump or on minimal container images (seen on Render with Python 3.14).
SESSION = requests.Session()
SESSION.verify = certifi.where()


@app.after_request
def add_cors_headers(response):
    """Allow requests from any frontend (GitHub Pages, etc.) without needing flask-cors."""
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response


# ──────────────────────────────────────────────────────────
# F&O eligible stock list — fetched live from NSE, cached
# ──────────────────────────────────────────────────────────
NSE_FO_CSV_URL = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"

_fno_cache = {"data": None, "fetched_at": 0}
FNO_CACHE_TTL = 6 * 60 * 60  # 6 hours

# Fallback list used only if NSE CSV fetch fails (kept reasonably current)
FALLBACK_FNO_STOCKS = [
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
    {"s": "BANDHANBNK", "n": "Bandhan Bank"},
    {"s": "AUBANK", "n": "AU Small Finance Bank"},
    {"s": "ZYDUSLIFE", "n": "Zydus Lifesciences"},
    {"s": "ABCAPITAL", "n": "Aditya Birla Capital"},
    {"s": "ABFRL", "n": "Aditya Birla Fashion"},
    {"s": "ALKEM", "n": "Alkem Laboratories"},
    {"s": "AMBUJACEM", "n": "Ambuja Cements"},
    {"s": "APOLLOTYRE", "n": "Apollo Tyres"},
    {"s": "ASHOKLEY", "n": "Ashok Leyland"},
    {"s": "ASTRAL", "n": "Astral Limited"},
    {"s": "ATGL", "n": "Adani Total Gas"},
    {"s": "BALKRISIND", "n": "Balkrishna Industries"},
    {"s": "BHEL", "n": "BHEL"},
    {"s": "BIOCON", "n": "Biocon"},
    {"s": "BSOFT", "n": "Birlasoft"},
    {"s": "CANBK", "n": "Canara Bank"},
    {"s": "CHAMBLFERT", "n": "Chambal Fertilizers"},
    {"s": "COFORGE", "n": "Coforge"},
    {"s": "CONCOR", "n": "Container Corp"},
    {"s": "CROMPTON", "n": "Crompton Greaves"},
    {"s": "CUMMINSIND", "n": "Cummins India"},
    {"s": "DALBHARAT", "n": "Dalmia Bharat"},
    {"s": "DEEPAKNTR", "n": "Deepak Nitrite"},
    {"s": "DIXON", "n": "Dixon Technologies"},
    {"s": "ESCORTS", "n": "Escorts Kubota"},
    {"s": "EXIDEIND", "n": "Exide Industries"},
    {"s": "GMRAIRPORT", "n": "GMR Airports"},
    {"s": "GODREJPROP", "n": "Godrej Properties"},
    {"s": "GRANULES", "n": "Granules India"},
    {"s": "HFCL", "n": "HFCL"},
    {"s": "HINDCOPPER", "n": "Hindustan Copper"},
    {"s": "HINDPETRO", "n": "Hindustan Petroleum"},
    {"s": "IDEA", "n": "Vodafone Idea"},
    {"s": "IEX", "n": "Indian Energy Exchange"},
    {"s": "IGL", "n": "Indraprastha Gas"},
    {"s": "INDHOTEL", "n": "Indian Hotels"},
    {"s": "INDUSTOWER", "n": "Indus Towers"},
    {"s": "JINDALSTEL", "n": "Jindal Steel & Power"},
    {"s": "JIOFIN", "n": "Jio Financial Services"},
    {"s": "JUBLFOOD", "n": "Jubilant FoodWorks"},
    {"s": "KPITTECH", "n": "KPIT Technologies"},
    {"s": "LAURUSLABS", "n": "Laurus Labs"},
    {"s": "LICHSGFIN", "n": "LIC Housing Finance"},
    {"s": "LODHA", "n": "Macrotech Developers"},
    {"s": "LTF", "n": "L&T Finance"},
    {"s": "LTIM", "n": "LTIMindtree"},
    {"s": "LUPIN", "n": "Lupin"},
    {"s": "M&MFIN", "n": "M&M Financial Services"},
    {"s": "MANAPPURAM", "n": "Manappuram Finance"},
    {"s": "MFSL", "n": "Max Financial Services"},
    {"s": "MGL", "n": "Mahanagar Gas"},
    {"s": "MOTHERSON", "n": "Samvardhana Motherson"},
    {"s": "MPHASIS", "n": "Mphasis"},
    {"s": "NATIONALUM", "n": "National Aluminium"},
    {"s": "NAVINFLUOR", "n": "Navin Fluorine"},
    {"s": "NBCC", "n": "NBCC India"},
    {"s": "NCC", "n": "NCC Limited"},
    {"s": "NMDC", "n": "NMDC"},
    {"s": "OBEROIRLTY", "n": "Oberoi Realty"},
    {"s": "OFSS", "n": "Oracle Financial Services"},
    {"s": "PAGEIND", "n": "Page Industries"},
    {"s": "PATANJALI", "n": "Patanjali Foods"},
    {"s": "PEL", "n": "Piramal Enterprises"},
    {"s": "PERSISTENT", "n": "Persistent Systems"},
    {"s": "PETRONET", "n": "Petronet LNG"},
    {"s": "POLICYBZR", "n": "PB Fintech (PolicyBazaar)"},
    {"s": "POLYCAB", "n": "Polycab India"},
    {"s": "PRESTIGE", "n": "Prestige Estates"},
    {"s": "RBLBANK", "n": "RBL Bank"},
    {"s": "RVNL", "n": "Rail Vikas Nigam"},
    {"s": "SAIL", "n": "SAIL"},
    {"s": "SBICARD", "n": "SBI Cards"},
    {"s": "SHRIRAMFIN", "n": "Shriram Finance"},
    {"s": "SRF", "n": "SRF Limited"},
    {"s": "SUPREMEIND", "n": "Supreme Industries"},
    {"s": "SUZLON", "n": "Suzlon Energy"},
    {"s": "TATACHEM", "n": "Tata Chemicals"},
    {"s": "TATACOMM", "n": "Tata Communications"},
    {"s": "TATATECH", "n": "Tata Technologies"},
    {"s": "TIINDIA", "n": "Tube Investments"},
    {"s": "TORNTPOWER", "n": "Torrent Power"},
    {"s": "TRENT", "n": "Trent Limited"},
    {"s": "TVSMOTOR", "n": "TVS Motor"},
    {"s": "UNIONBANK", "n": "Union Bank of India"},
    {"s": "UNITDSPR", "n": "United Spirits"},
    {"s": "UPL", "n": "UPL Limited"},
    {"s": "VBL", "n": "Varun Beverages"},
    {"s": "VEDL", "n": "Vedanta"},
    {"s": "YESBANK", "n": "Yes Bank"},
]


def get_fno_stock_list():
    """
    Fetch the current F&O-eligible stock list from NSE's official CSV.
    Cached for FNO_CACHE_TTL seconds. Falls back to a static list if
    NSE is unreachable.
    Returns a list of {"s": SYMBOL, "n": NAME} dicts.
    """
    now = time.time()
    if _fno_cache["data"] and (now - _fno_cache["fetched_at"] < FNO_CACHE_TTL):
        return _fno_cache["data"], "cache"

    try:
        resp = SESSION.get(NSE_FO_CSV_URL, headers=YAHOO_HEADERS, timeout=10)
        resp.raise_for_status()
        content = resp.content.decode("utf-8", errors="ignore")

        reader = csv.reader(io.StringIO(content))
        rows = list(reader)

        stocks = []
        seen = set()
        for row in rows:
            if len(row) < 3:
                continue
            name = row[1].strip() if len(row) > 1 else ""
            symbol = row[2].strip().upper() if len(row) > 2 else ""
            # Skip header rows, blanks, and index futures (NIFTY, BANKNIFTY etc.)
            if not symbol or symbol in ("SYMBOL", "UNDERLYING"):
                continue
            if symbol in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTY50"):
                continue
            if symbol in seen:
                continue
            seen.add(symbol)
            stocks.append({"s": symbol, "n": name or symbol})

        if len(stocks) < 50:
            # Sanity check failed — CSV format may have changed
            raise ValueError(f"Parsed only {len(stocks)} stocks, expected 150+")

        _fno_cache["data"] = stocks
        _fno_cache["fetched_at"] = now
        return stocks, "live"

    except Exception:
        return FALLBACK_FNO_STOCKS, "fallback"


# ──────────────────────────────────────────────────────────
# ALL NSE-listed equities — fetched live from NSE, cached
# Used by the "All Stocks" scan option (~2000 symbols)
# ──────────────────────────────────────────────────────────
NSE_ALL_EQUITY_CSV_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

_all_stocks_cache = {"data": None, "fetched_at": 0}
ALL_STOCKS_CACHE_TTL = 6 * 60 * 60  # 6 hours

# Small fallback used only if NSE's full equity list is unreachable.
# (Not meant to be exhaustive — just keeps the "All Stocks" option from
# returning nothing if NSE is briefly down. The live fetch is what
# actually delivers ~2000 names.)
FALLBACK_ALL_STOCKS = FALLBACK_FNO_STOCKS


def get_all_nse_stocks():
    """
    Fetch every NSE-listed equity (EQ/BE series) from NSE's official
    EQUITY_L.csv. Cached for ALL_STOCKS_CACHE_TTL seconds. Falls back
    to a small static list if NSE is unreachable.
    Returns a list of {"s": SYMBOL, "n": NAME} dicts (~2000 entries).
    """
    now = time.time()
    if _all_stocks_cache["data"] and (now - _all_stocks_cache["fetched_at"] < ALL_STOCKS_CACHE_TTL):
        return _all_stocks_cache["data"], "cache"

    try:
        resp = SESSION.get(NSE_ALL_EQUITY_CSV_URL, headers=YAHOO_HEADERS, timeout=15)
        resp.raise_for_status()
        content = resp.content.decode("utf-8", errors="ignore")

        reader = csv.reader(io.StringIO(content))
        rows = list(reader)

        stocks = []
        seen = set()
        for row in rows:
            if len(row) < 2:
                continue
            symbol = row[0].strip().upper()
            name = row[1].strip() if len(row) > 1 else ""
            if not symbol or symbol == "SYMBOL":
                continue
            if symbol in seen:
                continue
            seen.add(symbol)
            stocks.append({"s": symbol, "n": name or symbol})

        if len(stocks) < 500:
            # Sanity check failed — CSV format may have changed
            raise ValueError(f"Parsed only {len(stocks)} stocks, expected 1500+")

        _all_stocks_cache["data"] = stocks
        _all_stocks_cache["fetched_at"] = now
        return stocks, "live"

    except Exception:
        return FALLBACK_ALL_STOCKS, "fallback"


# ──────────────────────────────────────────────────────────
# Static index constituent lists (still available for index-based scans)
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
        resp = SESSION.get(url, params=params, headers=YAHOO_HEADERS, timeout=8)
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
            h = quote["high"][i] if i < len(quote.get("high", [])) else None
            l = quote["low"][i] if i < len(quote.get("low", [])) else None
            if o is None or c is None:
                continue
            candles.append({
                "date": datetime.utcfromtimestamp(ts).strftime("%d %b"),
                "open": round(o, 2),
                "close": round(c, 2),
                "high": round(h, 2) if h is not None else None,
                "low": round(l, 2) if l is not None else None,
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
    """Count consecutive red candles ending at the most recent candle.
    (Kept for reference / backward compatibility — no longer used to decide matches.)
    """
    count = 0
    for c in reversed(candles):
        if c["red"]:
            count += 1
        else:
            break
    return count


def trailing_staircase_down(candles):
    """
    Count how many of the most recent candles form a DESCENDING STAIRCASE
    of red candles, like the reference pattern:

        ▮
         ▮
          ▮

    Rules, checked walking backwards from the most recent candle:
      1. The candle must be RED (close < open).
      2. Compared to the candle right after it (i.e. the next, more
         recent step in the staircase), BOTH its open and its close
         must be lower (a fully lower body — not just a lower close).

    Returns the length of the staircase ending at the latest candle
    (0 if the latest candle isn't even red).
    """
    if not candles:
        return 0

    count = 0
    prev = None  # the candle one step closer to "now" (i.e. came after this one)

    for c in reversed(candles):
        if not c["red"]:
            break

        if prev is not None:
            # This candle (earlier in time) must have a HIGHER body than `prev`
            # i.e. prev's body must be fully lower than this candle's body.
            if not (prev["open"] < c["open"] and prev["close"] < c["close"]):
                break

        count += 1
        prev = c

    return count


@app.route("/")
def health():
    return jsonify({
        "status": "ok",
        "service": "RedScan Backend",
        "pattern": "Descending staircase of red candles (each red body lower than the previous)",
        "endpoints": ["/api/indices", "/api/scan?index=nifty50&minRed=3", "/api/scan?index=allstocks&minRed=3"],
    })


@app.route("/api/indices")
def list_indices():
    indices_list = [
        {"key": k, "label": v["label"], "count": len(v["stocks"])}
        for k, v in INDICES.items()
    ]
    fno_stocks, fno_source = get_fno_stock_list()
    indices_list.insert(0, {"key": "fno", "label": "All F&O Stocks", "count": len(fno_stocks), "source": fno_source})

    all_stocks, all_source = get_all_nse_stocks()
    indices_list.insert(0, {"key": "allstocks", "label": "All Stocks (NSE)", "count": len(all_stocks), "source": all_source})

    return jsonify({"indices": indices_list})


@app.route("/api/fno-list")
def fno_list():
    """Debug endpoint: shows the current F&O stock list and its source."""
    stocks, source = get_fno_stock_list()
    return jsonify({
        "source": source,  # "live" (from NSE), "cache", or "fallback"
        "count": len(stocks),
        "stocks": stocks,
    })


@app.route("/api/all-stocks-list")
def all_stocks_list():
    """Debug endpoint: shows the current full NSE equity list and its source."""
    stocks, source = get_all_nse_stocks()
    return jsonify({
        "source": source,  # "live" (from NSE), "cache", or "fallback"
        "count": len(stocks),
        "stocks": stocks,
    })


# How many stocks to fetch in parallel. NSE's "All Stocks" universe is
# ~2000 symbols — fetching those one-by-one (as the index/F&O scans do)
# would take many minutes and risks request timeouts. A small thread
# pool keeps Yahoo Finance load reasonable while finishing in a sane time.
# Kept modest (rather than higher) since free-tier hosts like Render have
# limited CPU/RAM, and too many concurrent threads risks the worker
# getting OOM-killed or hitting gunicorn's timeout.
ALLSTOCKS_MAX_WORKERS = 8


def scan_stocks(stocks, min_red, fno_symbols=None, concurrent=False, max_workers=ALLSTOCKS_MAX_WORKERS):
    """
    Run the staircase-down scan over a list of {"s","n"} stocks.

    fno_symbols: optional set of symbols that are F&O-eligible, used to
    tag each match with an accurate fnoEligible flag instead of assuming
    everything scanned is options-tradeable.

    concurrent: if True, fetch candles for all stocks in a thread pool
    instead of one at a time (used for the large "All Stocks" universe).

    Returns (matched, failed, scanned_count).
    """
    matched = []
    failed = []
    scanned = 0

    def handle_result(stock, data, err):
        nonlocal scanned
        scanned += 1
        if err:
            failed.append({"symbol": stock["s"], "reason": err})
            return
        streak = trailing_staircase_down(data["candles"])
        if streak >= min_red:
            price = data["price"]
            prev = data["prevClose"]
            change_pct = ((price - prev) / prev * 100) if (price and prev) else None
            is_fno = (stock["s"] in fno_symbols) if fno_symbols is not None else True
            matched.append({
                "symbol": stock["s"],
                "name": stock["n"],
                "price": price,
                "changePct": round(change_pct, 2) if change_pct is not None else None,
                "redStreak": streak,
                "candles": data["candles"][-5:],
                "fnoEligible": is_fno,
            })

    if concurrent:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_stock = {pool.submit(fetch_candles, stock["s"]): stock for stock in stocks}
            for future in as_completed(future_to_stock):
                stock = future_to_stock[future]
                try:
                    data, err = future.result()
                except Exception as e:
                    data, err = None, str(e)
                handle_result(stock, data, err)
    else:
        for stock in stocks:
            data, err = fetch_candles(stock["s"])
            handle_result(stock, data, err)
            # Be polite to Yahoo — tiny delay between requests
            time.sleep(0.05)

    return matched, failed, scanned


@app.route("/api/scan")
def scan():
    index_key = request.args.get("index", "nifty50")
    try:
        min_red = int(request.args.get("minRed", 3))
    except ValueError:
        min_red = 3

    fno_source = None
    all_stocks_source = None

    if index_key == "allstocks":
        stocks, all_stocks_source = get_all_nse_stocks()
        fno_stocks, fno_source = get_fno_stock_list()
        fno_symbols = {s["s"] for s in fno_stocks}
        index_label = "All Stocks (NSE)"

        matched, failed, scanned = scan_stocks(
            stocks, min_red, fno_symbols=fno_symbols, concurrent=True
        )
        for m in matched:
            m["indexMember"] = index_label

    elif index_key == "fno":
        stocks, fno_source = get_fno_stock_list()
        index_label = "All F&O Stocks"
        fno_symbols = {s["s"] for s in stocks}

        matched, failed, scanned = scan_stocks(
            stocks, min_red, fno_symbols=fno_symbols, concurrent=False
        )
        for m in matched:
            m["indexMember"] = index_label

    elif index_key in INDICES:
        stocks = INDICES[index_key]["stocks"]
        index_label = INDICES[index_key]["label"]
        fno_stocks, fno_source = get_fno_stock_list()
        fno_symbols = {s["s"] for s in fno_stocks}

        matched, failed, scanned = scan_stocks(
            stocks, min_red, fno_symbols=fno_symbols, concurrent=False
        )
        for m in matched:
            m["indexMember"] = index_label

    else:
        return jsonify({
            "error": f"Unknown index '{index_key}'",
            "available": ["fno", "allstocks"] + list(INDICES.keys())
        }), 400

    matched.sort(key=lambda x: (-x["redStreak"], x["changePct"] or 0))

    response_data = {
        "index": index_label,
        "minRedCandles": min_red,
        "pattern": "staircase_down",
        "scanTime": datetime.utcnow().strftime("%d %b %Y, %H:%M UTC"),
        "totalScanned": scanned,
        "totalFailed": len(failed),
        "totalMatched": len(matched),
        "results": matched,
        "failed": failed,
    }
    if fno_source:
        response_data["fnoListSource"] = fno_source  # "live", "cache", or "fallback"
    if all_stocks_source:
        response_data["allStocksListSource"] = all_stocks_source  # "live", "cache", or "fallback"

    return jsonify(response_data)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
