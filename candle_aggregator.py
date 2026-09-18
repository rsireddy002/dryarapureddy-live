"""
candle_aggregator.py - Turns a stream of individual ticks (one per
instrument, arriving in real time off the WebSocket) into rolling 5-minute
OHLCV candles, per symbol, held in memory.

This is the piece that replaces ~230 REST calls to the historical-candle
endpoint with local aggregation of a single live feed -- the REST version
re-downloads a symbol's whole day every time you want fresh candles; this
version just keeps building on bars it already has.

Pure logic, no network/websocket code here (see feed_listener.py for
that) so it's easy to unit-test with fake ticks.
"""
from datetime import datetime, timedelta, timezone
from threading import Lock

IST = timezone(timedelta(hours=5, minutes=30))
BAR_SECONDS = 5 * 60  # 5-minute bars, matching the REST candle interval used elsewhere


def bucket_start(ts, bar_seconds=BAR_SECONDS):
    """Floors a timestamp to its containing bar's start time, e.g.
    09:17:42 -> 09:15:00 for 5-min bars."""
    epoch = ts.timestamp()
    floored = epoch - (epoch % bar_seconds)
    return datetime.fromtimestamp(floored, tz=ts.tzinfo)


class CandleAggregator:
    """
    Thread-safe. Call on_tick() from the WebSocket receive loop (which
    runs continuously); call get_candles() from anywhere else (e.g. a
    periodic "dump to file" timer, or directly from Streamlit if you
    later run the aggregator in-process instead of as a separate service).

    Internal state per symbol:
        {"bars": {bar_start_iso: {"open","high","low","close","volume"}},
         "last_cum_volume": <float, to compute per-tick volume deltas
                             since Upstox's LTP feed gives CUMULATIVE
                             day volume, not per-tick volume>}
    """

    def __init__(self, bar_seconds=BAR_SECONDS):
        self.bar_seconds = bar_seconds
        self._data = {}  # symbol -> {"bars": {...}, "last_cum_volume": float}
        self._lock = Lock()

    def on_tick(self, symbol, ltp, cum_volume, timestamp=None):
        """
        symbol: your own symbol string (e.g. "RELIANCE", "NIFTY") --
            map from instrument_key to symbol before calling this.
        ltp: last traded price, float.
        cum_volume: TOTAL volume traded so far today for this instrument
            (this is what Upstox's feed reports -- not a per-tick delta).
            Pass None if unavailable; volume will just stay 0 for bars
            built from ticks that never carried a volume figure.
        timestamp: tz-aware datetime; defaults to now in IST.
        """
        if timestamp is None:
            timestamp = datetime.now(IST)
        elif timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=IST)

        bstart = bucket_start(timestamp, self.bar_seconds)
        bkey = bstart.isoformat()

        with self._lock:
            state = self._data.setdefault(symbol, {"bars": {}, "last_cum_volume": None})
            bars = state["bars"]

            # per-tick volume delta from the cumulative day-volume figure
            vol_delta = 0.0
            if cum_volume is not None:
                prev_cum = state["last_cum_volume"]
                if prev_cum is not None and cum_volume >= prev_cum:
                    vol_delta = cum_volume - prev_cum
                state["last_cum_volume"] = cum_volume

            if bkey not in bars:
                bars[bkey] = {
                    "timestamp": bstart, "open": ltp, "high": ltp,
                    "low": ltp, "close": ltp, "volume": vol_delta,
                }
            else:
                bar = bars[bkey]
                bar["high"] = max(bar["high"], ltp)
                bar["low"] = min(bar["low"], ltp)
                bar["close"] = ltp
                bar["volume"] += vol_delta

    def get_candles(self, symbol):
        """Returns this symbol's bars so far today, sorted oldest-first,
        as a list of dicts -- easy to hand straight to pandas.DataFrame()
        on the reading side."""
        with self._lock:
            state = self._data.get(symbol)
            if not state:
                return []
            return sorted(state["bars"].values(), key=lambda b: b["timestamp"])

    def get_all_symbols(self):
        with self._lock:
            return list(self._data.keys())

    def snapshot(self):
        """Returns {symbol: [bars...]} for every symbol currently tracked
        -- what feed_listener.py periodically dumps to the shared file."""
        with self._lock:
            return {sym: self.get_candles(sym) for sym in self._data}
