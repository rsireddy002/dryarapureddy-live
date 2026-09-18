"""
live_feed_reader.py - reads fno_live_candles.json (written every
DUMP_INTERVAL_SECONDS by feed_listener.py in this same folder) instead of
hitting Upstox's REST historical-candle endpoint.

RECONSTRUCTED, not an original vendored file (it wasn't part of what was
uploaded) -- written straight from README_fno_websocket_feed.md's
documented output format:

    {
      "RELIANCE": [
        {"timestamp": "2026-09-05T09:15:00+05:30", "open":, "high":,
         "low":, "close":, "volume":},
        ...
      ],
      "NIFTY": [...],
      ...
    }

This is a DIFFERENT file/data source from upstox-live-feed's own
live_feed_reader.py (which reads data/live_feed.sqlite3, written by
poll_listener.py) -- deliberately kept separate rather than reusing that
one, since app.py in this folder is meant to read whatever feed_listener.py
in THIS folder is currently dumping, not the other pipeline's database.
Nothing in the existing upstox-live-feed/viewer.py setup is touched by
this file.

app.py's get_today_candles() calls get_live_candles(symbol) with no other
args and treats an empty DataFrame as "the live feed doesn't have this
symbol (yet)" -- falling back to its own REST fetch in that case. So this
reader is deliberately forgiving: a missing/stale/malformed file, or a
symbol not yet present, all just return an empty DataFrame rather than
raising.
"""
import json
import os
import time

import pandas as pd

OUTPUT_PATH = os.environ.get(
    "FNO_LIVE_CANDLES_PATH",
    os.path.join(os.path.dirname(__file__), "fno_live_candles.json"),
)

# If feed_listener.py's last write is older than this, treat the file as
# stale (the process probably isn't running) and let the caller fall back
# to REST instead of serving a frozen snapshot from hours ago.
MAX_STALENESS_SECONDS = 120


def _load_raw():
    if not os.path.exists(OUTPUT_PATH):
        return None
    try:
        mtime = os.path.getmtime(OUTPUT_PATH)
        if time.time() - mtime > MAX_STALENESS_SECONDS:
            return None
        with open(OUTPUT_PATH, "r") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        # feed_listener.py writes temp-file-then-rename, so a genuinely
        # torn read shouldn't happen, but don't take the caller down over
        # a transient race either way.
        return None


def get_live_candles(symbol: str) -> pd.DataFrame:
    """Returns today's rolling 5-min candles for `symbol` from the live
    WebSocket feed's snapshot file, oldest first. Columns: timestamp
    (tz-aware), open, high, low, close, volume. Empty DataFrame if the
    feed isn't running, the file is stale, or this symbol has no bars
    yet -- callers (see app.py's get_today_candles) treat that as "fall
    back to REST", not an error."""
    raw = _load_raw()
    if not raw:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    bars = raw.get(symbol.strip().upper()) or raw.get(symbol)
    if not bars:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(bars)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df[["timestamp", "open", "high", "low", "close", "volume"]]


def is_feed_alive() -> bool:
    """True if feed_listener.py has written a snapshot recently -- handy
    for a status indicator, same idea as upstox-live-feed's own
    is_feed_alive() but checking this file's mtime instead of a DB
    heartbeat row."""
    if not os.path.exists(OUTPUT_PATH):
        return False
    return (time.time() - os.path.getmtime(OUTPUT_PATH)) <= MAX_STALENESS_SECONDS
