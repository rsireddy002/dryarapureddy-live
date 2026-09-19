"""
feed_listener.py - Connects to Upstox's V3 market-data WebSocket feed,
subscribes to the full F&O universe in tiers (same tiered pattern as
upstox-feed-listener: Tier 1 gets full_d30 depth, Tier 2 gets full),
feeds every tick into CandleAggregator, and periodically dumps all
symbols' rolling 5-min candles to a shared JSON file that
fno-liquid-scanner-live's Streamlit app can read instead of hitting the
REST historical-candle endpoint ~230 times per refresh.

Decoding is handled by proto_decoder.py, built directly from your actual
MarketDataFeedV3.proto schema and tested end-to-end against real
serialized protobuf messages (marketFF, indexFF, and plain ltpc cases) --
see this repo's commit history for the test.

TICK-DRIVEN PAPER TRADING (added): every tick is also handed to
tick_paper_trader.on_tick(), which checks stop-loss/target exits and
level+VWAP+CVD entry conditions immediately, instead of the old
paper_trader_daemon.py's REST-polling scan cycle (~5 min cadence). See
tick_paper_trader.py's module docstring for the full design and its
known approximations (VWAP and CVD, since the WebSocket feed doesn't
carry either directly). Wrapped in try/except below so a bug in the
trading logic can never take down the live feed/chart pipeline this
file's other consumers (the dashboard, the interactive scanner) depend
on. paper_trader_daemon.py should still run alongside this for
Precompute + Zone Refresh -- tick_paper_trader.py only reads zones,
it doesn't compute them.

SETUP:
    pip install -r requirements.txt
    $env:UPSTOX_ACCESS_TOKEN = "your_token_here"
    python instrument_resolver.py     # one-time: builds instrument_keys_cache.json
    python feed_listener.py

OUTPUT:
    fno_live_candles.json -- updated every DUMP_INTERVAL_SECONDS, holding
    {symbol: {interval: [{"timestamp": iso, "open":, "high":, "low":,
                           "close":, "volume":}, ...]}}
    for every symbol currently subscribed, where interval is "1"/"5"/"15"
    (see candle_aggregator.INTERVALS_SECONDS) -- built from the SAME tick
    stream at three granularities at once, so app.py's Dashboard interval
    switcher can read live candles at any of the three, not just 5-min.
    (Format changed from a flat {symbol: [bars...]} list to this nested
    dict when 1m/15m aggregation was added -- restarting this script
    overwrites the old-format file within one DUMP_INTERVAL_SECONDS.)

    paper_trades.json -- updated in real time by tick_paper_trader.py
    whenever a paper trade opens or closes.
"""
import json
import os
import time
import threading
from datetime import datetime, timezone, timedelta

import requests
import websocket  # pip install websocket-client

from candle_aggregator import CandleAggregator, INTERVALS_SECONDS
from instrument_resolver import resolve_all, get_token
from proto_decoder import decode_feed_message
import tick_paper_trader

IST = timezone(timedelta(hours=5, minutes=30))
AUTHORIZE_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
OUTPUT_PATH = os.environ.get("FNO_LIVE_CANDLES_OUTPUT", "fno_live_candles.json")
# ^ override to write directly into fno-liquid-scanner-live's folder, e.g.:
#   $env:FNO_LIVE_CANDLES_OUTPUT = "C:\Users\MY-PC\Desktop\fno-liquid-scanner-live\fno_live_candles.json"
# so that repo can read it without any copy step, while keeping both repos
# independent (this one has no dependency on the other's folder structure
# beyond this one optional env var).
DUMP_INTERVAL_SECONDS = 5

# Same tiering idea as upstox-feed-listener: a small "Tier 1" set gets
# richer depth (full_d30), everything else gets the lighter "full" mode.
# Adjust TIER1_SYMBOLS to whatever you actively trade/watch most closely.
TIER1_SYMBOLS = ["NIFTY", "BANKNIFTY"]
TIER1_MODE = "full_d30"
TIER2_MODE = "full"

aggregator = CandleAggregator(bar_seconds_list=list(INTERVALS_SECONDS.values()))
SECONDS_TO_LABEL = {v: k for k, v in INTERVALS_SECONDS.items()}
symbol_by_key = {}  # instrument_key -> symbol, built from instrument_resolver's output
_stop_event = threading.Event()


def get_authorized_ws_url(token):
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    resp = requests.get(AUTHORIZE_URL, headers=headers, timeout=20)
    resp.raise_for_status()
    return resp.json()["data"]["authorized_redirect_uri"]


def build_subscribe_message(instrument_keys, mode):
    return json.dumps({
        "guid": f"sub-{mode}-{int(time.time())}",
        "method": "sub",
        "data": {"mode": mode, "instrumentKeys": instrument_keys},
    })


def dump_loop():
    """Runs in its own thread: every DUMP_INTERVAL_SECONDS, writes the
    aggregator's current state to OUTPUT_PATH. Writes to a temp file then
    renames, so the Streamlit app never reads a half-written file. Also
    a convenient, already-existing periodic tick for tick_paper_trader's
    own lightweight housekeeping (re-ranking the wide-range universe,
    re-checking which symbols have an open paper trade) -- no need for a
    second timer thread just for that."""
    tmp_path = OUTPUT_PATH + ".tmp"
    while not _stop_event.is_set():
        # snapshot() returns {symbol: {bar_seconds: [bars...]}} -- translate
        # the bar_seconds keys to the "1"/"5"/"15" labels app.py's
        # live_feed_reader.py looks up by, and ISO-format each bar's
        # timestamp for JSON.
        snapshot = aggregator.snapshot()
        serializable = {
            sym: {
                SECONDS_TO_LABEL[bar_seconds]: [
                    {**bar, "timestamp": bar["timestamp"].isoformat()}
                    for bar in bars
                ]
                for bar_seconds, bars in by_interval.items()
            }
            for sym, by_interval in snapshot.items()
        }
        with open(tmp_path, "w") as f:
            json.dump(serializable, f)
        os.replace(tmp_path, OUTPUT_PATH)

        try:
            tick_paper_trader.maybe_periodic_refresh()
        except Exception as e:
            print(f"tick_paper_trader periodic refresh error: {e}")

        _stop_event.wait(DUMP_INTERVAL_SECONDS)


def on_message(ws, message):
    """message is raw bytes (protobuf-encoded). decode_feed_message must
    return a dict like:
        {instrument_key: {"ltp": float, "ltt": int_ms_epoch_or_None, "volume": float_or_None}, ...}
    for whichever instruments had an update in this message -- see
    proto_decoder.py's docstring for the exact contract."""
    try:
        updates = decode_feed_message(message)
    except Exception as e:
        print(f"decode error: {e}")
        return

    for instrument_key, tick in updates.items():
        symbol = symbol_by_key.get(instrument_key)
        if not symbol:
            continue
        ltp = tick.get("ltp")
        if ltp is None:
            continue
        ltt_ms = tick.get("ltt")
        ts = datetime.fromtimestamp(ltt_ms / 1000, tz=IST) if ltt_ms else datetime.now(IST)
        aggregator.on_tick(symbol, ltp, tick.get("volume"), timestamp=ts)

        # Tick-driven paper trading -- see tick_paper_trader.py. Wrapped
        # defensively: a bug in trading logic must never break the live
        # feed / chart pipeline that other apps depend on this file for.
        try:
            tick_paper_trader.on_tick(symbol, ltp, tick.get("volume"), timestamp=ts)
        except Exception as e:
            print(f"tick_paper_trader error for {symbol}: {e}")


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} {close_msg}")


def on_open(ws, tier1_keys, tier2_keys):
    print(f"Connected. Subscribing {len(tier1_keys)} Tier-1 ({TIER1_MODE}) "
          f"and {len(tier2_keys)} Tier-2 ({TIER2_MODE}) symbols...")
    if tier1_keys:
        ws.send(build_subscribe_message(tier1_keys, TIER1_MODE))
    if tier2_keys:
        # Upstox recommends chunking large subscribe lists rather than
        # one giant message -- 100 per message is a safe conservative size.
        chunk_size = 100
        for i in range(0, len(tier2_keys), chunk_size):
            ws.send(build_subscribe_message(tier2_keys[i:i + chunk_size], TIER2_MODE))
            time.sleep(0.2)
    print("Subscription messages sent.")


TIER1_KEYS_FILE = "tier1_keys.txt"
TIER2_KEYS_FILE = "tier2_keys.txt"


def load_keys_file(path):
    """Reads a comma-separated instrument-key list, same format your
    build_tier1_keys.py already writes. Returns [] if the file doesn't
    exist -- caller falls back to auto-splitting via TIER1_SYMBOLS."""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()
    return [k.strip() for k in content.split(",") if k.strip()]


def run():
    global symbol_by_key
    token = get_token()

    print("Resolving instrument keys (cached after first run)...")
    key_by_symbol = resolve_all(token)
    symbol_by_key = {v: k for k, v in key_by_symbol.items()}

    # Prefer your own pre-built tier1_keys.txt/tier2_keys.txt (from
    # build_tier1_keys.py) if present -- keeps this repo's tiering in
    # sync with whatever fixed anchors/RVOL ranking you've already set up,
    # rather than falling back to the simple TIER1_SYMBOLS constant here.
    tier1_keys = load_keys_file(TIER1_KEYS_FILE)
    tier2_keys = load_keys_file(TIER2_KEYS_FILE)

    if tier1_keys or tier2_keys:
        print(f"Using {TIER1_KEYS_FILE} ({len(tier1_keys)} keys) and "
              f"{TIER2_KEYS_FILE} ({len(tier2_keys)} keys).")
        # symbol_by_key only knows about EQUITY_SYMBOLS/FUTURES_SYMBOLS from
        # instrument_resolver.py -- keys from tier1/tier2 files not found
        # there (e.g. NSE_INDEX|... index keys, which aren't equities or
        # futures) won't map to a symbol name and so won't be aggregated
        # into named candles, but they'll still be subscribed/received.
        unmapped = [k for k in tier1_keys + tier2_keys if k not in symbol_by_key]
        if unmapped:
            print(f"  Note: {len(unmapped)} key(s) from these files have no symbol "
                  f"name in instrument_keys_cache.json (e.g. raw index keys) -- "
                  f"they'll be subscribed but won't appear in fno_live_candles.json "
                  f"under a friendly symbol name.")
    else:
        print(f"No {TIER1_KEYS_FILE}/{TIER2_KEYS_FILE} found -- auto-splitting "
              f"by TIER1_SYMBOLS instead.")
        tier1_keys = [key_by_symbol[s] for s in TIER1_SYMBOLS if s in key_by_symbol]
        tier2_keys = [key_by_symbol[s] for s in key_by_symbol if s not in TIER1_SYMBOLS]

    dump_thread = threading.Thread(target=dump_loop, daemon=True)
    dump_thread.start()

    while not _stop_event.is_set():
        try:
            ws_url = get_authorized_ws_url(token)
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.on_open = lambda ws_: on_open(ws_, tier1_keys, tier2_keys)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"Connection failed ({e}), retrying in 5s...")

        if not _stop_event.is_set():
            print("Reconnecting in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\nStopping...")
        _stop_event.set()
