"""
tick_paper_trader.py - Tick-driven entries AND exits for paper trading,
replacing paper_trader_daemon.py's REST-polling scan cycle.

Hooked directly into feed_listener.py's WebSocket tick handler (see
on_tick() below, called from feed_listener.py's on_message for EVERY
tick that arrives) so paper trades react the instant a stop-loss/target
is hit, or an entry condition fires -- not up to 5 minutes later, which
was the old REST-polling daemon's real cadence (seconds_until_next_
candle_close aligned it to 5-min candle boundaries; the docstring's
claimed "~60s" was stale relative to the actual code).

WHAT MOVED HERE FROM paper_trader_daemon.py
--------------------------------------------
- Entry detection: level-cross + VWAP-cross + CVD confirm + ML risk
  filter -- now evaluated on every tick instead of once per REST poll.
- Exit checking: stop-loss/target -- now checked on every tick for that
  symbol, the instant a tick crosses either level.
- VWAP: Upstox's REST quotes conveniently include average_price (their
  own VWAP). The WebSocket tick feed does NOT include this, so this
  module computes a running VWAP itself from cumulative-day-volume
  deltas -- the SAME delta technique candle_aggregator.py already uses
  to turn Upstox's cumulative day-volume ticks into per-tick volume (see
  _on_tick_volume below). VWAP = sum(ltp * vol_delta) / sum(vol_delta),
  reset at the start of each trading day.

WHAT STAYED IN paper_trader_daemon.py
---------------------------------------
Precompute (once/day) and Zone Refresh (~5 min) -- both need 18-day/
today REST candle history to compute HVN/LVN volume-profile zones; that
is inherently a periodic batch computation, not something individual
ticks can drive. This module only READS whatever zones that process
last wrote to sahi_zones_cache.json, reloaded every ZONE_RELOAD_SECONDS
(not on every tick -- re-parsing that file on every one of potentially
hundreds of ticks/sec across 200+ symbols would be wasteful I/O for no
benefit, since zones themselves only change every 5 minutes anyway).

KNOWN APPROXIMATIONS -- read before trusting entries blindly
--------------------------------------------------------------
1. CVD (cumulative volume delta, used to confirm an entry has real
   buying/selling pressure behind it): matches candles_with_levels.py's
   compute_cumulative_volume_delta() exactly for every COMPLETED 5-min
   bar -- an up-close bar's full volume counts as buying, a down-close
   bar's as selling, summed across the session. The one adaptation:
   that function only ever saw fully-formed candles (from a REST fetch),
   so for the bar CURRENTLY forming, this module estimates its
   contribution live as (volume so far in this bar) x (sign of current
   ltp vs this bar's own open) -- i.e. exactly what the real formula
   would produce if this partial bar were finalized right now. This
   estimate updates every tick and settles onto the exact real value
   the instant the bar actually closes.
2. day_open (fed into predict_break_probability's day_change_pct_so_far
   feature): approximated as this symbol's FIRST tick price of the day,
   captured live, rather than an exchange-reported opening print -- close
   in practice, but not guaranteed identical to the official open.

CONCURRENCY NOTE
------------------
This module and the interactive app.py (manual Buy/Sell clicks on the
Chart tab) both read-modify-write the same paper_trades.json, without
file locking -- the same pre-existing risk as before this change (the
old daemon and the interactive app already both wrote to it). In the
rare case a manual click and an automatic tick-driven exit land in the
same instant, one write could overwrite the other. Not solved here --
would need real file locking or a message queue to close entirely.

SETUP
-------
No separate process to run -- this is imported and called directly by
feed_listener.py. Just make sure paper_trader_daemon.py is ALSO still
running (for Precompute + Zone Refresh) so sahi_zones_cache.json
actually has zones in it; this module has nothing to trade against
without that.
"""
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone, time as dtime

from zone_validation import cross_validated_zones
from ml_predict import predict_break_probability
from candle_aggregator import bucket_start, BAR_SECONDS

IST = timezone(timedelta(hours=5, minutes=30))


def now_ist():
    return datetime.now(IST)


CACHE_PATH = "sahi_zones_cache.json"
PAPER_TRADE_LOG_PATH = "paper_trades.json"
TRIPLE_CROSS_LOG_PATH = "triple_cross_log.json"
MAX_TRIPLE_CROSS_LOG_ENTRIES = 300  # keep the file bounded; oldest entries drop off

MARKET_OPEN_TIME = dtime(9, 15)
MARKET_CLOSE_TIME = dtime(15, 30)

PAPER_TRADE_SIZE_RUPEES = 25000
PAPER_TRADE_ML_RISK_THRESHOLD = 15.0   # entry only if the crossed level's ML break-risk is BELOW this %
PAPER_TRADE_UNIVERSE_TOP_N = 10        # only the top-N Wide Range symbols are eligible for entries

ZONE_RELOAD_SECONDS = 5          # how often to re-read sahi_zones_cache.json from disk
RANKING_RELOAD_SECONDS = 5       # how often to recompute the top-wide-range universe
OPEN_SYMBOLS_RELOAD_SECONDS = 3  # how often to re-check which symbols have an open paper trade

_zone_cache = {}
_zone_cache_loaded_at = 0.0

# Wide-Range eligibility is stored as a CUTOFF gap% (the Nth-largest gap
# across the universe), not a cached set of symbol names. A cached name
# set would lag behind the very tick that crosses a level -- gap-based
# eligibility inherently requires price to already sit BETWEEN a
# symbol's support and resistance zones, so by the time a periodic
# snapshot picks up a newly-qualifying symbol, its crossing tick has
# already passed. Storing a cutoff instead lets on_tick() compute THIS
# symbol's own gap% fresh, from data it already has for the crossing
# check, and compare it against a threshold that's only periodically
# stale for the other ~200 symbols -- not for the one that just crossed.
_wide_range_cutoff_pct = None
_wide_range_cutoff_at = 0.0

_open_symbols = set()          # symbols with a currently-open paper trade, kept in sync incrementally
_open_symbols_loaded_at = 0.0

_last_price_lookup = {}        # symbol -> latest ltp, updated on every tick; used for wide-range ranking
_state = {}                    # symbol -> per-tick running state, see _get_state()
_current_day = None


# ---------------------------------------------------------------------------
# Small helpers -- ported unchanged from paper_trader_daemon.py / app.py
# ---------------------------------------------------------------------------
def _market_open_now(now_dt):
    return MARKET_OPEN_TIME <= now_dt.time() < MARKET_CLOSE_TIME


def _pct_from_label_safe(label):
    m = re.search(r"[\d.]+", str(label))
    return float(m.group()) if m else 0.0


def _nearest_zones(ltp, validated_zones):
    support, support_dist = None, None
    resistance, resistance_dist = None, None
    for z in validated_zones:
        if ltp is None:
            break
        if z["price_mode"] <= ltp:
            dist = abs(ltp - z["price_high"]) / ltp * 100
            if support_dist is None or dist < support_dist:
                support, support_dist = z, dist
        else:
            dist = abs(z["price_low"] - ltp) / ltp * 100
            if resistance_dist is None or dist < resistance_dist:
                resistance, resistance_dist = z, dist
    return support, support_dist, resistance, resistance_dist


def _crossed_zones(prev_ltp, ltp, validated_zones):
    breakdowns, reclaims = [], []
    if prev_ltp is None or ltp is None:
        return breakdowns, reclaims
    for z in validated_zones:
        level = z["price_mode"]
        if prev_ltp >= level > ltp:
            breakdowns.append(z)
        elif prev_ltp < level <= ltp:
            reclaims.append(z)
    return breakdowns, reclaims


# ---------------------------------------------------------------------------
# Zone cache (read-only from here -- paper_trader_daemon.py is the writer)
# ---------------------------------------------------------------------------
def _load_zone_cache(force=False):
    global _zone_cache, _zone_cache_loaded_at
    if not force and (time.time() - _zone_cache_loaded_at) < ZONE_RELOAD_SECONDS:
        return
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r") as f:
                _zone_cache = json.load(f)
        except Exception:
            pass  # keep serving the previous in-memory copy on a bad/mid-write read
    _zone_cache_loaded_at = time.time()


def _refresh_wide_range_cutoff(force=False):
    """Computes the Nth-largest support/resistance gap% across the whole
    scanned universe (same ranking the original REST version used), and
    stores just that CUTOFF value -- see the module-level comment above
    for why a cutoff, not a cached symbol set."""
    global _wide_range_cutoff_pct, _wide_range_cutoff_at
    if not force and (time.time() - _wide_range_cutoff_at) < RANKING_RELOAD_SECONDS:
        return
    gaps = []
    for symbol, c in _zone_cache.items():
        ltp = _last_price_lookup.get(symbol)
        if ltp is None:
            continue
        val_comp, _, _ = cross_validated_zones(c.get("composite_zones", []), c.get("intraday_zones", []))
        support, _, resistance, _ = _nearest_zones(ltp, val_comp)
        if support is None or resistance is None:
            continue
        gap_price = resistance["price_mode"] - support["price_mode"]
        if gap_price <= 0:
            continue
        gaps.append(gap_price / ltp * 100)
    gaps.sort(reverse=True)
    if len(gaps) >= PAPER_TRADE_UNIVERSE_TOP_N:
        _wide_range_cutoff_pct = gaps[PAPER_TRADE_UNIVERSE_TOP_N - 1]
    elif gaps:
        _wide_range_cutoff_pct = gaps[-1]  # fewer than N eligible candidates -- everyone with a valid gap qualifies
    else:
        _wide_range_cutoff_pct = None
    _wide_range_cutoff_at = time.time()


# ---------------------------------------------------------------------------
# paper_trades.json -- this module is now the ONLY automated writer
# (manual chart-click trades from app.py can still write too -- see the
# concurrency note in the module docstring)
# ---------------------------------------------------------------------------
def _load_paper_trades():
    if os.path.exists(PAPER_TRADE_LOG_PATH):
        try:
            with open(PAPER_TRADE_LOG_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"trades": []}


def _save_paper_trades(log):
    # temp-file-then-rename, same pattern feed_listener.py uses for
    # fno_live_candles.json, so a reader (the dashboard, the interactive
    # app) never sees a half-written file.
    tmp = PAPER_TRADE_LOG_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(log, f, indent=2)
    os.replace(tmp, PAPER_TRADE_LOG_PATH)


def _log_triple_cross(entry):
    """Records every instant a zone level, the 200-EMA, and VWAP are all
    crossed (in the confirming direction) on the SAME tick -- regardless
    of whether an actual paper trade results. A real entry additionally
    requires CVD confirmation and passing the wide-range/already-open
    gates (see on_tick below), so this log will show more rows than
    paper_trades.json -- that gap IS the point: it shows how often the
    strict same-tick triple-cross condition fires versus how often it
    actually converts into a trade."""
    log = []
    if os.path.exists(TRIPLE_CROSS_LOG_PATH):
        try:
            with open(TRIPLE_CROSS_LOG_PATH, "r") as f:
                log = json.load(f)
        except Exception:
            log = []
    log.append(entry)
    log = log[-MAX_TRIPLE_CROSS_LOG_ENTRIES:]
    tmp = TRIPLE_CROSS_LOG_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(log, f, indent=2)
    os.replace(tmp, TRIPLE_CROSS_LOG_PATH)


def _refresh_open_symbols(force=False):
    global _open_symbols, _open_symbols_loaded_at
    if not force and (time.time() - _open_symbols_loaded_at) < OPEN_SYMBOLS_RELOAD_SECONDS:
        return
    paper_log = _load_paper_trades()
    _open_symbols = {t["symbol"] for t in paper_log["trades"] if t["status"] == "open"}
    _open_symbols_loaded_at = time.time()


def _open_paper_trade(paper_log, candidate):
    qty = int(PAPER_TRADE_SIZE_RUPEES // candidate["entry_price"])
    if qty < 1:
        return False
    paper_log["trades"].append({
        "symbol": candidate["symbol"], "direction": candidate["direction"],
        "entry_price": candidate["entry_price"],
        "entry_time": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
        "stop_loss": candidate["stop_loss"], "target": candidate["target"], "qty": qty,
        "ml_risk_pct": candidate["ml_risk_pct"], "zone_pct": candidate["zone_pct"],
        "source": "tick_algo",
        "status": "open", "exit_price": None, "exit_time": None,
        "exit_reason": None, "pnl": None,
    })
    return True


def _check_exit_for_symbol(symbol, ltp, force_eod=False):
    """Checks ONLY this symbol's open trade(s) against the current tick's
    LTP -- must stay cheap since this runs on every tick for any symbol
    with an open position."""
    paper_log = _load_paper_trades()
    changed = False
    now_str = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    for t in paper_log["trades"]:
        if t["status"] != "open" or t["symbol"] != symbol:
            continue
        exit_reason = None
        if t["direction"] == "long":
            if ltp <= t["stop_loss"]:
                exit_reason = "stop_loss"
            elif t["target"] is not None and ltp >= t["target"]:
                exit_reason = "target"
        else:
            if ltp >= t["stop_loss"]:
                exit_reason = "stop_loss"
            elif t["target"] is not None and ltp <= t["target"]:
                exit_reason = "target"
        if exit_reason is None and force_eod:
            exit_reason = "end_of_day"
        if exit_reason:
            t["status"] = "closed"
            t["exit_price"] = ltp
            t["exit_time"] = now_str
            t["exit_reason"] = exit_reason
            t["pnl"] = (round((ltp - t["entry_price"]) * t["qty"], 2) if t["direction"] == "long"
                        else round((t["entry_price"] - ltp) * t["qty"], 2))
            changed = True
            _open_symbols.discard(symbol)
    if changed:
        _save_paper_trades(paper_log)


# ---------------------------------------------------------------------------
# Per-symbol running state: VWAP, CVD, day-open, cross tracking
# ---------------------------------------------------------------------------
def _get_state(symbol):
    st = _state.get(symbol)
    if st is None:
        st = {
            "prev_ltp": None, "prev_vwap_above": None, "day_open": None,
            "last_cum_volume": None, "cum_vol": 0.0, "cum_pv": 0.0,
            # CVD, tracked per-bar to match candles_with_levels.py's
            # compute_cumulative_volume_delta() exactly on every
            # completed bar -- see module docstring.
            "completed_cvd": 0.0, "bar_start": None, "bar_open": None, "bar_vol": 0.0,
            # Signal detection (VWAP/EMA/zone crosses) is evaluated ONLY
            # at 5-min bar close, not every tick -- these hold the
            # PREVIOUS completed bar's close and its VWAP relationship,
            # so the NEXT bar-close can be compared against them.
            "prev_bar_close": None, "prev_bar_above_vwap": None,
        }
        _state[symbol] = st
    return st


def _maybe_reset_for_new_day(now_dt):
    """Resets every symbol's running VWAP/CVD/cross-state at the start of
    a new trading day. Doesn't touch paper_trades.json -- open positions
    from a previous day are the daemon/EOD-close's concern, not this."""
    global _current_day
    today = now_dt.date()
    if _current_day == today:
        return
    _current_day = today
    _state.clear()


# ---------------------------------------------------------------------------
# Public entry point -- call this from feed_listener.py's on_message,
# right after aggregator.on_tick(), for every tick.
# ---------------------------------------------------------------------------
def on_tick(symbol, ltp, cum_volume, timestamp=None):
    """
    symbol: your own symbol string (e.g. "RELIANCE", "NIFTY") -- ticks
        for symbols not present in sahi_zones_cache.json are ignored
        (not part of the scanned universe, or the cache hasn't loaded
        that symbol yet).
    ltp: last traded price, float.
    cum_volume: TOTAL volume traded so far today for this instrument --
        this is what Upstox's feed reports (confirmed in
        candle_aggregator.py), NOT a per-tick delta. Pass None if
        unavailable.
    timestamp: tz-aware datetime; defaults to now in IST.
    """
    if ltp is None:
        return
    now = timestamp or now_ist()
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)

    _maybe_reset_for_new_day(now)
    _load_zone_cache()

    c = _zone_cache.get(symbol)
    if c is None:
        return  # not in the scanned universe, or cache hasn't loaded it yet

    _last_price_lookup[symbol] = ltp
    st = _get_state(symbol)
    if st["day_open"] is None:
        st["day_open"] = ltp  # approximation -- see module docstring

    # -- running VWAP from cumulative-day-volume deltas (identical delta
    # technique to candle_aggregator.py's on_tick) --
    vol_delta = 0.0
    if cum_volume is not None:
        prev_cum = st["last_cum_volume"]
        if prev_cum is not None and cum_volume >= prev_cum:
            vol_delta = cum_volume - prev_cum
        st["last_cum_volume"] = cum_volume
    if vol_delta > 0:
        st["cum_vol"] += vol_delta
        st["cum_pv"] += ltp * vol_delta
    vwap = (st["cum_pv"] / st["cum_vol"]) if st["cum_vol"] > 0 else None

    # -- CVD, bucketed into the SAME 5-min bars candle_aggregator.py
    # uses, so this matches compute_cumulative_volume_delta()'s
    # candle-close-vs-open classification exactly for every bar that's
    # actually finished. See module docstring for how the still-forming
    # bar's contribution is estimated. --
    bstart = bucket_start(now, BAR_SECONDS)
    bar_just_completed = st["bar_start"] is not None and bstart != st["bar_start"]

    if bar_just_completed:
        # the bar we were tracking just closed -- fold its FINAL signed
        # volume into the permanent running total, using its own last
        # tick as that bar's close (exactly what a completed candle's
        # close would be)
        prev_open = st["bar_open"]
        completed_close = st["prev_ltp"]  # last tick seen before this bar rolled over
        if prev_open is not None and completed_close is not None:
            if completed_close > prev_open:
                st["completed_cvd"] += st["bar_vol"]
            elif completed_close < prev_open:
                st["completed_cvd"] -= st["bar_vol"]
        st["bar_start"], st["bar_open"], st["bar_vol"] = None, None, 0.0
    if st["bar_start"] is None:
        st["bar_start"] = bstart
        st["bar_open"] = ltp
    st["bar_vol"] += vol_delta

    # live estimate for the bar still forming -- exactly what
    # compute_cumulative_volume_delta() would give if this partial bar
    # were finalized on this tick
    if ltp > st["bar_open"]:
        live_cvd = st["completed_cvd"] + st["bar_vol"]
    elif ltp < st["bar_open"]:
        live_cvd = st["completed_cvd"] - st["bar_vol"]
    else:
        live_cvd = st["completed_cvd"]

    if not _market_open_now(now):
        st["prev_ltp"] = ltp
        return

    # -- exits: checked on every tick, for this symbol only, and only if
    # it's actually known to have an open position (cheap in-memory set,
    # refreshed periodically rather than reading the file every tick).
    # Exits stay tick-by-tick regardless of the bar-close signal change
    # below -- a stop-loss/target should react instantly, not wait for
    # a candle to close. --
    _refresh_open_symbols()
    if symbol in _open_symbols:
        _check_exit_for_symbol(symbol, ltp)

    # -- Signal detection (VWAP/EMA/zone crosses) + entries: evaluated
    # ONLY at 5-min bar close, not on every tick. This filters out
    # intra-candle noise/whipsaws -- a level is only considered "really"
    # crossed if the candle actually CLOSED beyond it, not just wicked
    # through it mid-bar. completed_close (this bar's close, captured
    # above right when the bar rolled over) is compared against
    # prev_bar_close (the PREVIOUS completed bar's close), not against
    # any raw intermediate tick. --
    if not bar_just_completed or completed_close is None:
        st["prev_ltp"] = ltp
        return

    val_comp, _, _ = cross_validated_zones(c.get("composite_zones", []), c.get("intraday_zones", []))

    # -- VWAP cross state, evaluated at bar close using the VWAP value
    # as of this bar's close (VWAP moves slowly tick-to-tick, so this is
    # an accurate-enough reference point) --
    crossed_up, crossed_down = False, False
    if vwap is not None:
        bar_above_vwap_now = completed_close > vwap
        crossed_up = st["prev_bar_above_vwap"] is False and bar_above_vwap_now
        crossed_down = st["prev_bar_above_vwap"] is True and not bar_above_vwap_now
        st["prev_bar_above_vwap"] = bar_above_vwap_now

    level_breakdowns, level_reclaims = _crossed_zones(st["prev_bar_close"], completed_close, val_comp)

    # 200-EMA crossing filter -- same reasoning as before, now applied
    # bar-close to bar-close instead of tick to tick: only counts as a
    # real signal if THIS bar's close is the moment price crosses the
    # 200-EMA, not just "currently on the right side" of it.
    ema_200 = c.get("ema_200")
    if ema_200 is None or st["prev_bar_close"] is None:
        level_breakdowns, level_reclaims = [], []
    else:
        ema_crossed_up = st["prev_bar_close"] <= ema_200 and completed_close > ema_200
        ema_crossed_down = st["prev_bar_close"] >= ema_200 and completed_close < ema_200
        if not ema_crossed_up:
            level_reclaims = []
        if not ema_crossed_down:
            level_breakdowns = []

    qualifies_wide_range = False
    if level_reclaims or level_breakdowns:
        support_now, _, resistance_now, _ = _nearest_zones(completed_close, val_comp)
        if support_now is not None and resistance_now is not None:
            gap_price_now = resistance_now["price_mode"] - support_now["price_mode"]
            if gap_price_now > 0:
                gap_pct_now = gap_price_now / completed_close * 100
                qualifies_wide_range = (_wide_range_cutoff_pct is None) or (gap_pct_now >= _wide_range_cutoff_pct)

    # -- Same-bar-close triple-cross visibility log: zone level + 200-EMA
    # + VWAP all crossed on THIS bar's close, in the confirming
    # direction. Logged regardless of CVD/wide-range/already-open
    # gating below, so this shows every instance the strict
    # candle-close triple-cross condition actually fires, not just the
    # ones that became trades. --
    triple_cross_direction = None
    if level_reclaims and crossed_up:
        triple_cross_direction = "bullish (support reclaim)"
        triple_cross_zone = level_reclaims[0]
    elif level_breakdowns and crossed_down:
        triple_cross_direction = "bearish (resistance breakdown)"
        triple_cross_zone = level_breakdowns[0]

    if triple_cross_direction is not None:
        cvd_confirms = (live_cvd > 0) if "bullish" in triple_cross_direction else (live_cvd < 0)
        _log_triple_cross({
            "time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "direction": triple_cross_direction,
            "bar_close": completed_close,
            "zone_level": triple_cross_zone["price_mode"],
            "ema_200": ema_200,
            "vwap": round(vwap, 2) if vwap is not None else None,
            "live_cvd": round(live_cvd, 1),
            "cvd_confirms": cvd_confirms,
            "qualifies_wide_range": qualifies_wide_range,
            "already_open": symbol in _open_symbols,
            # Approximate only -- doesn't check the ML risk threshold or
            # whether a next target zone exists, both checked further
            # below for an actual trade. True here means "passed every
            # check this log can see", not a guarantee a trade opened --
            # cross-reference against paper_trades.json's actual entries
            # for the definitive answer.
            "likely_became_trade": cvd_confirms and qualifies_wide_range and symbol not in _open_symbols,
        })

    if (level_reclaims or level_breakdowns) and qualifies_wide_range and symbol not in _open_symbols:
        candidate = None

        if level_reclaims and crossed_up and vwap is not None and live_cvd > 0:
            z = level_reclaims[0]
            risk = predict_break_probability(
                z, ltp=completed_close, vwap=vwap, day_open=st["day_open"],
                session_start_time=MARKET_OPEN_TIME, session_end_time=MARKET_CLOSE_TIME,
                now_time=now.time(), is_intraday_validated=True,
            )
            if risk is not None and risk * 100 < PAPER_TRADE_ML_RISK_THRESHOLD:
                _, _, next_resistance, _ = _nearest_zones(completed_close, val_comp)
                if next_resistance is not None:
                    candidate = {
                        "symbol": symbol, "direction": "long", "entry_price": completed_close,
                        "stop_loss": z["price_mode"], "target": next_resistance["price_mode"],
                        "ml_risk_pct": round(risk * 100, 1), "zone_pct": _pct_from_label_safe(z["label"]),
                    }
        elif level_breakdowns and crossed_down and vwap is not None and live_cvd < 0:
            z = level_breakdowns[0]
            risk = predict_break_probability(
                z, ltp=completed_close, vwap=vwap, day_open=st["day_open"],
                session_start_time=MARKET_OPEN_TIME, session_end_time=MARKET_CLOSE_TIME,
                now_time=now.time(), is_intraday_validated=True,
            )
            if risk is not None and risk * 100 < PAPER_TRADE_ML_RISK_THRESHOLD:
                next_support, _, _, _ = _nearest_zones(completed_close, val_comp)
                if next_support is not None:
                    candidate = {
                        "symbol": symbol, "direction": "short", "entry_price": completed_close,
                        "stop_loss": z["price_mode"], "target": next_support["price_mode"],
                        "ml_risk_pct": round(risk * 100, 1), "zone_pct": _pct_from_label_safe(z["label"]),
                    }

        if candidate is not None:
            paper_log = _load_paper_trades()
            if not any(t["symbol"] == symbol and t["status"] == "open" for t in paper_log["trades"]):
                if _open_paper_trade(paper_log, candidate):
                    _save_paper_trades(paper_log)
                    _open_symbols.add(symbol)

    st["prev_bar_close"] = completed_close
    st["prev_ltp"] = ltp


def maybe_periodic_refresh():
    """Call this from feed_listener.py's existing dump_loop (already
    runs every DUMP_INTERVAL_SECONDS) to keep the wide-range cutoff and
    open-symbols set fresh without recomputing them on every tick."""
    _refresh_wide_range_cutoff()
    _refresh_open_symbols()


def force_eod_close_all():
    """Optional: call once, right at/after MARKET_CLOSE_TIME, to close
    every still-open paper trade at its last-known LTP -- same
    force_eod behavior the old daemon applied per-symbol on its final
    scan of the day. Not wired to a timer automatically here; call it
    from wherever your process's end-of-day housekeeping already lives."""
    paper_log = _load_paper_trades()
    now_str = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    changed = False
    for t in paper_log["trades"]:
        if t["status"] != "open":
            continue
        ltp = _last_price_lookup.get(t["symbol"])
        if ltp is None:
            continue
        t["status"] = "closed"
        t["exit_price"] = ltp
        t["exit_time"] = now_str
        t["exit_reason"] = "end_of_day"
        t["pnl"] = (round((ltp - t["entry_price"]) * t["qty"], 2) if t["direction"] == "long"
                    else round((t["entry_price"] - ltp) * t["qty"], 2))
        changed = True
        _open_symbols.discard(t["symbol"])
    if changed:
        _save_paper_trades(paper_log)
