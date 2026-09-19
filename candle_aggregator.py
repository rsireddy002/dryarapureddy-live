"""
candle_aggregator.py - Turns a stream of individual ticks (one per
instrument, arriving in real time off the WebSocket) into rolling OHLCV
candles, per symbol, held in memory -- at MULTIPLE granularities at once
(1-min, 5-min, 15-min by default), so the Dashboard's interval switcher
can be fed by the live feed regardless of which interval is selected,
not just the 5-min default.

This is the piece that replaces ~230 REST calls to the historical-candle
endpoint with local aggregation of a single live feed -- the REST version
re-downloads a symbol's whole day every time you want fresh candles; this
version just keeps building on bars it already has.

Pure logic, no network/websocket code here (see feed_listener.py for
that) so it's easy to unit-test with fake ticks.
"""
from datetime import datetime, timedelta, timezone
from threading import RLock

IST = timezone(timedelta(hours=5, minutes=30))
BAR_SECONDS = 5 * 60  # kept as the historical default -- tick_paper_trader.py
                       # imports this directly for its own 5-min CVD bucketing,
                       # independent of CandleAggregator's multi-interval bars.

# Interval label (matches the strings app.py's Dashboard interval radio
# already uses: "1"/"5"/"15") -> bar length in seconds. Single source of
# truth for which granularities the live feed aggregates -- feed_listener.py
# builds a CandleAggregator over exactly these, and app.py imports this same
# dict to know which intervals it can ask the live feed for before falling
# back to REST.
INTERVALS_SECONDS = {"1": 60, "5": 300, "15": 900}


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

    Builds bars at every granularity in bar_seconds_list simultaneously --
    each incoming tick is cheap to bucket at 1/5/15-min all at once, so
    there's no need to run three separate aggregators over the same tick
    stream.

    Internal state per symbol:
        {"bars": {bar_seconds: {bar_start_iso: {"open","high","low",
                                                  "close","volume"}}},
         "last_cum_volume": <float, to compute per-tick volume deltas
                             since Upstox's LTP feed gives CUMULATIVE
                             day volume, not per-tick volume -- this is
                             granularity-independent, one counter per
                             symbol, not one per bar_seconds>}
    """

    def __init__(self, bar_seconds_list=None):
        self.bar_seconds_list = list(bar_seconds_list) if bar_seconds_list else list(INTERVALS_SECONDS.values())
        self._data = {}  # symbol -> {"bars": {bar_seconds: {...}}, "last_cum_volume": float}
        # RLock, not Lock -- snapshot() below calls get_candles() while
        # already holding this lock (same thread), which self-deadlocks on
        # a plain Lock. This was a latent bug in the pre-multi-interval
        # version of this file too (snapshot() -> get_candles() under the
        # same non-reentrant lock); fixed here rather than carried forward.
        self._lock = RLock()

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

        with self._lock:
            state = self._data.setdefault(
                symbol, {"bars": {bs: {} for bs in self.bar_seconds_list}, "last_cum_volume": None},
            )

            # per-tick volume delta from the cumulative day-volume figure --
            # computed ONCE per tick and applied to every granularity's bar,
            # not re-derived per granularity (the cumulative counter itself
            # only advances once per tick regardless of how many bar sizes
            # are being built from it).
            vol_delta = 0.0
            if cum_volume is not None:
                prev_cum = state["last_cum_volume"]
                if prev_cum is not None and cum_volume >= prev_cum:
                    vol_delta = cum_volume - prev_cum
                state["last_cum_volume"] = cum_volume

            for bar_seconds in self.bar_seconds_list:
                bars = state["bars"][bar_seconds]
                bstart = bucket_start(timestamp, bar_seconds)
                bkey = bstart.isoformat()

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

    def get_candles(self, symbol, bar_seconds=BAR_SECONDS):
        """Returns this symbol's bars so far today at the given bar
        length, sorted oldest-first, as a list of dicts -- easy to hand
        straight to pandas.DataFrame() on the reading side. Defaults to
        the historical 5-min granularity if bar_seconds isn't specified."""
        with self._lock:
            state = self._data.get(symbol)
            if not state or bar_seconds not in state["bars"]:
                return []
            return sorted(state["bars"][bar_seconds].values(), key=lambda b: b["timestamp"])

    def get_all_symbols(self):
        with self._lock:
            return list(self._data.keys())

    def snapshot(self):
        """Returns {symbol: {bar_seconds: [bars...]}} for every symbol
        currently tracked, at every granularity this aggregator builds --
        what feed_listener.py periodically dumps to the shared file (after
        translating bar_seconds -> the "1"/"5"/"15" labels app.py uses)."""
        with self._lock:
            return {
                sym: {bs: self.get_candles(sym, bs) for bs in self.bar_seconds_list}
                for sym in self._data
            }
