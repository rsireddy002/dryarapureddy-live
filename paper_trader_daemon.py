"""
paper_trader_daemon.py - Standalone, browser-independent Precompute +
Zone Refresh for the paper trading system.

WHY THIS EXISTS: the interactive app's zone computation only updates
while someone has a browser tab open with auto-refresh on -- that
refresh timer runs in the BROWSER, not the server, so closing your
laptop or phone stops it completely even though the Streamlit process on
AWS is still technically alive. This script has no such dependency: it's
a plain Python loop that runs on the server itself, refreshing zones on
its own schedule regardless of whether anyone is watching.

CHANGED: entries and exits used to live in this file too (a REST-quote
scan cycle, roughly every 5 minutes -- aligned to candle closes, despite
an earlier version of this docstring claiming "~60s"). That logic has
MOVED to tick_paper_trader.py, hooked directly into feed_listener.py's
WebSocket tick handler, so paper trades now react the instant a tick
crosses a stop-loss/target or an entry condition, instead of waiting for
this daemon's next cycle. This file now does ONLY the things that
inherently require periodic REST candle downloads -- Precompute and Zone
Refresh -- which tick_paper_trader.py reads from sahi_zones_cache.json
but does not compute itself.

Shares the SAME sahi_zones_cache.json as the interactive app
(dryarapureddy-ML) and tick_paper_trader.py -- run this from that same
folder. paper_trades.json is no longer touched by this file at all (see
tick_paper_trader.py, which now owns it).

SCHEDULE:
  - Once per day, before market open (or on first run if the cache is
    missing/stale): runs a full Precompute.
  - Every ZONE_REFRESH_INTERVAL_SECONDS (~5 min) during market hours:
    refreshes intraday zones.
  - Outside market hours: sleeps, waking up periodically to check if the
    next session has started.

USAGE:
    $env:UPSTOX_ACCESS_TOKEN = "your_token_here"   (or set as a systemd
    Environment= var, same as the interactive app's service)
    python3 paper_trader_daemon.py

    Run tick_paper_trader.py's host, feed_listener.py, ALONGSIDE this --
    entries/exits won't happen without it, and this daemon has nothing
    to do with them anymore either way.
"""
import json
import os
import time
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone, time as dtime

import requests
import pandas as pd

from sahi_style_key_levels import sahi_style_key_levels

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("paper_trader_daemon.log"), logging.StreamHandler()],
)
log = logging.getLogger("paper_trader_daemon")

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():
    return datetime.now(IST)

# ---------------- Config (matches app.py exactly) ----------------
INSTRUMENT_SEARCH_URL = "https://api.upstox.com/v2/instruments/search"
CACHE_PATH = "sahi_zones_cache.json"
HEARTBEAT_PATH = "daemon_heartbeat.json"

DAILY_LOOKBACK_DAYS = 30
COMPOSITE_LOOKBACK_DAYS = 18
RVOL_BASELINE_DAYS = 20
COMPOSITE_N_BINS = 50
INTRADAY_N_BINS = 45
MIN_PROMINENCE_PCT = 0.08
MIN_BIN_DISTANCE = 2
MAX_ZONES = 6
MIN_DISPLAY_PCT = 2.0

MARKET_OPEN_TIME = dtime(9, 15)
MARKET_CLOSE_TIME = dtime(15, 30)

ZONE_REFRESH_INTERVAL_SECONDS = 300   # ~5 min
IDLE_CHECK_INTERVAL_SECONDS = 120     # how often to check "has the market opened yet" outside hours

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


def fetch_candles(instrument_key, token, unit, interval, lookback_days):
    to_date = now_ist().strftime("%Y-%m-%d")
    from_date = (now_ist() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    candles = resp.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["date"] = df["timestamp"].dt.date
    return df


def fetch_intraday_candles(instrument_key, token, unit="minutes", interval="5"):
    url = f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/{unit}/{interval}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    candles = resp.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["date"] = df["timestamp"].dt.date
    return df


def fetch_today_candles(instrument_key, token):
    df = fetch_intraday_candles(instrument_key, token, "minutes", "5")
    if not df.empty:
        return df
    df = fetch_candles(instrument_key, token, "minutes", "5", lookback_days=1)
    if df.empty:
        return df
    latest = df["date"].max()
    return df[df["date"] == latest]


def compute_composite_zones(intraday_df):
    if intraday_df.empty:
        return []
    try:
        _, shown = sahi_style_key_levels(
            intraday_df, n_bins=COMPOSITE_N_BINS, max_zones=MAX_ZONES,
            min_display_pct=MIN_DISPLAY_PCT, min_prominence_pct=MIN_PROMINENCE_PCT,
            min_bin_distance=MIN_BIN_DISTANCE,
        )
        return [asdict(z) for z in shown]
    except Exception:
        return []


def compute_ema_200(closes, period=200):
    """Latest 200-period EMA from a 5-min closing-price series -- see
    app.py's identical function for the full reasoning (computed once
    per Precompute from 18 days of composite history, held static
    through the day rather than recomputed live each cycle)."""
    if len(closes) < period:
        return None
    return float(pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1])


def compute_intraday_zones(today_only_df):
    if today_only_df.empty:
        return []
    today = today_only_df["date"].max()
    today_df = today_only_df[today_only_df["date"] == today]
    try:
        _, shown = sahi_style_key_levels(
            today_df, n_bins=INTRADAY_N_BINS, max_zones=MAX_ZONES,
            min_display_pct=MIN_DISPLAY_PCT, min_prominence_pct=MIN_PROMINENCE_PCT,
            min_bin_distance=MIN_BIN_DISTANCE,
        )
        return [asdict(z) for z in shown]
    except Exception:
        return []


# ---------------- Precompute / zone refresh ----------------

def run_precompute(token):
    log.info("Starting Precompute (this takes a while)...")
    cache = {}
    all_symbols = [(s, "equity") for s in EQUITY_SYMBOLS] + [(s, "futures") for s in FUTURES_SYMBOLS]
    for i, (symbol, kind) in enumerate(all_symbols):
        try:
            key = (resolve_equity_instrument_key(symbol, token) if kind == "equity"
                   else resolve_futures_instrument_key(symbol, token))
            if key is None:
                continue
            daily_df = fetch_candles(key, token, "days", "1", DAILY_LOOKBACK_DAYS)
            intraday_df = fetch_candles(key, token, "minutes", "5", COMPOSITE_LOOKBACK_DAYS)
            prev_close = float(daily_df["close"].iloc[-1]) if not daily_df.empty else None
            avg_daily_volume = (float(daily_df["volume"].tail(RVOL_BASELINE_DAYS).mean())
                                 if len(daily_df) >= RVOL_BASELINE_DAYS else None)
            composite_zones = compute_composite_zones(intraday_df)
            intraday_zones = compute_intraday_zones(intraday_df)
            ema_200 = compute_ema_200(intraday_df["close"].tolist()) if not intraday_df.empty else None
            cache[symbol] = {
                "instrument_key": key, "prev_close": prev_close,
                "avg_daily_volume": avg_daily_volume,
                "composite_zones": composite_zones, "intraday_zones": intraday_zones,
                "ema_200": ema_200,
                "zones_updated_at": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
            }
        except Exception as e:
            log.warning(f"{symbol}: precompute failed ({e}), skipping.")
        if (i + 1) % 25 == 0:
            log.info(f"Precompute progress: {i+1}/{len(all_symbols)}")
        time.sleep(0.15)
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f)
    log.info(f"Precompute done. {len(cache)} symbols cached.")
    return cache


def run_zone_refresh(cache, token):
    for symbol in list(cache.keys()):
        try:
            key = cache[symbol]["instrument_key"]
            today_df = fetch_today_candles(key, token)
            cache[symbol]["intraday_zones"] = compute_intraday_zones(today_df)
            cache[symbol]["zones_updated_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            log.warning(f"{symbol}: zone refresh failed ({e}), keeping previous zones.")
        time.sleep(0.1)
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f)
    return cache


def cache_is_stale(cache):
    """True if the cache is missing, empty, or has no zone data from
    today. Checks zones_updated_at's date rather than a dedicated
    "precompute_date" field, since this cache file is SHARED with the
    interactive app.py and tick_paper_trader.py, which only ever read
    (or, for app.py, also set) zones_updated_at -- checking that field
    keeps this daemon from treating a cache another process already
    refreshed today as stale, and vice versa."""
    if not cache:
        return True
    today_str = now_ist().strftime("%Y-%m-%d")
    sample = next(iter(cache.values()))
    updated_at = sample.get("zones_updated_at", "")
    return not updated_at.startswith(today_str)


def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "r") as f:
            return json.load(f)
    return {}


def write_heartbeat(last_successful_scan, last_error, consecutive_errors):
    """Written every loop iteration -- proves the daemon PROCESS is alive
    and looping (last_loop_time), separately from whether its most
    recent CYCLE (Precompute or Zone Refresh) actually succeeded
    (last_successful_scan / last_error)."""
    heartbeat = {
        "last_loop_time": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
        "last_successful_scan": last_successful_scan,
        "last_error": last_error,
        "consecutive_errors": consecutive_errors,
    }
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            json.dump(heartbeat, f, indent=2)
    except Exception as e:
        log.warning(f"Could not write heartbeat file: {e}")


def main():
    token = get_token()
    cache = load_cache()
    last_zone_refresh = 0.0
    last_successful_scan = None
    consecutive_errors = 0

    log.info("paper_trader_daemon starting (Precompute + Zone Refresh only -- "
             "entries/exits now handled tick-by-tick by tick_paper_trader.py, "
             "hooked into feed_listener.py).")

    while True:
        now = now_ist()
        last_error = None
        market_open_now = MARKET_OPEN_TIME <= now.time() < MARKET_CLOSE_TIME

        try:
            if cache_is_stale(cache):
                cache = run_precompute(token)
                last_zone_refresh = time.time()
                last_successful_scan = now_ist().strftime("%Y-%m-%d %H:%M:%S")

            elif market_open_now and time.time() - last_zone_refresh >= ZONE_REFRESH_INTERVAL_SECONDS:
                log.info("Refreshing intraday zones...")
                cache = run_zone_refresh(cache, token)
                last_zone_refresh = time.time()
                last_successful_scan = now_ist().strftime("%Y-%m-%d %H:%M:%S")
            consecutive_errors = 0
        except Exception as e:
            last_error = str(e)
            consecutive_errors += 1
            log.error(f"Cycle failed ({consecutive_errors} in a row): {e}")

        write_heartbeat(last_successful_scan, last_error, consecutive_errors)
        time.sleep(ZONE_REFRESH_INTERVAL_SECONDS if market_open_now else IDLE_CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
