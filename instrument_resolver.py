"""
instrument_resolver.py - resolves & caches Upstox instrument keys for the
full F&O universe feed_listener.py subscribes to.

RECONSTRUCTED, not an original vendored file (it wasn't part of what was
uploaded) -- written to match instrument_keys_cache.json's actual on-disk
shape exactly ({symbol: instrument_key}, 222 entries = the 220 EQUITY_SYMBOLS
+ 2 FUTURES_SYMBOLS from app.py/paper_trader_daemon.py) and the identical
resolve_equity_instrument_key/resolve_futures_instrument_key logic already
duplicated in both of those files -- same instrument search calls, same
"exact trading_symbol match" / "nearest expiry" selection rules, just
factored out here and cached to disk so feed_listener.py doesn't re-hit the
search API on every restart.

Safe to re-run any time: resolve_all() only re-resolves symbols missing
from the cache, so a restart after the cache already exists is instant.
Equity keys are cached forever (they don't change); futures keys are
re-resolved every call's first miss and then cached like everything else --
if a contract has rolled to a new expiry, delete this symbol's entry (or
the whole file) to force a fresh lookup, same as you'd do for the other
repos' instrument caches.

USAGE:
    from instrument_resolver import resolve_all, get_token
    key_by_symbol = resolve_all(token)   # {symbol: instrument_key}
"""
import json
import os
import time

import requests

INSTRUMENT_SEARCH_URL = "https://api.upstox.com/v2/instruments/search"
CACHE_PATH = os.path.join(os.path.dirname(__file__), "instrument_keys_cache.json")

# Same universe app.py / paper_trader_daemon.py scan -- duplicated here
# deliberately (per this repo's own convention: "duplicated here so this
# repo has no dependency on that one", per README.md's Files section).
EQUITY_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "SBIN", "AXISBANK",
    "KOTAKBANK", "BAJFINANCE", "BHARTIARTL", "ITC", "LT", "HINDUNILVR",
    "MARUTI", "TMPV", "TATASTEEL", "SUNPHARMA", "TITAN", "ULTRACEMCO",
    "ASIANPAINT", "WIPRO", "NTPC", "POWERGRID", "M&M", "ADANIENT",
    "ADANIPORTS", "BAJAJFINSV", "HCLTECH", "JSWSTEEL", "ONGC", "COALINDIA",
    "TECHM", "GRASIM", "DIVISLAB", "DRREDDY", "CIPLA", "EICHERMOT",
    "HEROMOTOCO", "HINDALCO", "BPCL", "BRITANNIA", "APOLLOHOSP", "SBILIFE",
    "HDFCLIFE", "INDUSINDBK", "BAJAJ-AUTO", "TATACONSUM", "UPL", "SHREECEM",
    "NESTLEIND", "VEDANTA", "GAIL", "PIDILITIND", "DLF", "GODREJCP",
    "SIEMENS", "AMBUJACEM", "BANDHANBNK", "BANKBARODA", "PNB", "CANBK",
    "IDFCFIRSTB", "FEDERALBNK", "AUROPHARMA", "BEL", "BIOCON", "CHOLAFIN",
    "COLPAL", "CONCOR", "CUMMINSIND", "DABUR", "DEEPAKNTR", "ESCORTS",
    "EXIDEIND", "GODREJPROP", "HAVELLS", "HDFCAMC", "ICICIGI", "ICICIPRULI",
    "IEX", "INDIGO", "INDUSTOWER", "IOC", "IRCTC", "JINDALSTEL", "JUBLFOOD",
    "LICHSGFIN", "LTIM", "LUPIN", "MANAPPURAM", "MARICO", "MCDOWELL-N",
    "MFSL", "MOTHERSON", "MPHASIS", "MRF", "MUTHOOTFIN", "NAUKRI",
    "NMDC", "OBEROIRLTY", "OFSS", "PAGEIND", "PEL", "PERSISTENT",
    "PETRONET", "PFC", "PIIND", "POLYCAB", "RECLTD", "SAIL", "SBICARD",
    "SRF", "SYNGENE", "TATACOMM", "TATAPOWER", "TORNTPHARM", "TRENT",
    "TVSMOTOR", "UBL", "VOLTAS", "ZEEL", "ZYDUSLIFE", "CDSL", "IRFC",
    "IDEA", "YESBANK", "SUZLON", "ETERNAL", "DMART", "JIOFIN", "PAYTM",
    "NYKAA", "POLICYBZR", "DELHIVERY", "LODHA", "PATANJALI", "ABCAPITAL",
    "ALKEM", "APLAPOLLO", "ASHOKLEY", "ASTRAL", "ATUL", "BALKRISNIND",
    "BATAINDIA", "BHARATFORG", "BHEL", "BSOFT", "CANFINHOME", "CROMPTON",
    "CUB", "DALBHARAT", "GLENMARK", "GMRINFRA", "GNFC", "GRANULES",
    "GUJGASLTD", "HAL", "HINDCOPPER", "HINDPETRO", "IBULHSGFIN", "IGL",
    "INDHOTEL", "INDIAMART", "IPCALAB", "JKCEMENT", "L&TFH", "LALPATHLAB",
    "LAURUSLABS", "M&MFIN", "METROPOLIS", "NATIONALUM", "NAVINFLUOR",
    "OIL", "PVRINOX", "RAIN", "RBLBANK", "SUNTV", "TATACHEM",
    "TATAELXSI", "TORNTPOWER", "UNIONBANK", "VBL", "WHIRLPOOL",
    "AARTIIND", "ABFRL", "ANGELONE", "APOLLOTYRE", "AUBANK", "BANKINDIA",
    "BSE", "CGPOWER", "CHAMBLFERT", "COFORGE", "COROMANDEL", "DIXON",
    "FORTIS", "GICRE", "GODFRYPHLP", "GRAPHITE", "GSPL", "HFCL",
    "HUDCO", "IIFL", "INDIACEM", "IRB", "ITI", "KALYANKJIL",
    "KEI", "LTF", "MANKIND", "MAXHEALTH", "MGL", "MOTILALOFS",
    "NBCC", "NCC", "NHPC", "PFIZER", "PGEL", "POWERINDIA",
    "PRESTIGE", "RVNL", "SJVN", "SOLARINDS", "SONACOMS", "STARHEALTH",
    "SUPREMEIND", "TIINDIA", "TITAGARH", "VEDL", "ZFCVINDIA",
    "SHRIRAMFIN",
]
FUTURES_SYMBOLS = ["NIFTY", "BANKNIFTY"]


def get_token():
    token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN not set.")
    return token.strip()


def _load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_cache(cache):
    tmp = CACHE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f, indent=2, sort_keys=True)
    os.replace(tmp, CACHE_PATH)


def resolve_equity_instrument_key(symbol, token):
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    params = {"query": symbol, "exchanges": "NSE", "segments": "EQ",
              "instrument_types": "EQ", "page_number": 1, "records": 10}
    resp = requests.get(INSTRUMENT_SEARCH_URL, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    candidates = [inst for inst in resp.json().get("data", [])
                  if inst.get("trading_symbol", "").upper() == symbol.upper()]
    return candidates[0]["instrument_key"] if candidates else None


def resolve_futures_instrument_key(name, token):
    """No expiry filter - sorts client-side by nearest expiry (same
    pattern app.py/paper_trader_daemon.py already use)."""
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    params = {"query": name, "exchanges": "NSE", "segments": "FO",
              "instrument_types": "FUT", "page_number": 1, "records": 30}
    resp = requests.get(INSTRUMENT_SEARCH_URL, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    candidates = [inst for inst in resp.json().get("data", [])
                  if inst.get("instrument_type") == "FUT"
                  and inst.get("underlying_symbol", "").upper() == name.upper()]
    if not candidates:
        return None
    candidates.sort(key=lambda x: x["expiry"])
    return candidates[0]["instrument_key"]


def resolve_all(token, force_refresh=False):
    """Returns {symbol: instrument_key} for every symbol in EQUITY_SYMBOLS
    + FUTURES_SYMBOLS, resolving only whatever isn't already cached (unless
    force_refresh=True). Symbols that fail to resolve are simply left out
    (feed_listener.py just won't subscribe to them) rather than raising --
    one bad symbol shouldn't block startup for the other ~220."""
    cache = {} if force_refresh else _load_cache()
    changed = False

    for symbol in EQUITY_SYMBOLS:
        if symbol in cache:
            continue
        try:
            key = resolve_equity_instrument_key(symbol, token)
            if key:
                cache[symbol] = key
                changed = True
        except requests.RequestException as e:
            print(f"  ! could not resolve {symbol} (equity): {e}")
        time.sleep(0.1)

    for symbol in FUTURES_SYMBOLS:
        if symbol in cache:
            continue
        try:
            key = resolve_futures_instrument_key(symbol, token)
            if key:
                cache[symbol] = key
                changed = True
        except requests.RequestException as e:
            print(f"  ! could not resolve {symbol} (futures): {e}")
        time.sleep(0.1)

    if changed:
        _save_cache(cache)
    return cache


if __name__ == "__main__":
    tok = get_token()
    resolved = resolve_all(tok)
    print(f"Resolved {len(resolved)} / {len(EQUITY_SYMBOLS) + len(FUTURES_SYMBOLS)} symbols "
          f"-> {CACHE_PATH}")
