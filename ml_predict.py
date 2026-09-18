"""
ml_predict.py - Loads the trained "does this zone hold or break" model
and exposes a single function, predict_break_probability(), for the
live app to call.

TRAINED ON: 2,290 labeled zone-test events across 20 liquid symbols,
~2.5 months of 5-min history (see build_ml_dataset.py). Held-out
time-based test set: AUC 0.763, and at a 0.7 confidence threshold,
precision is 2.3x the base rate (45.8% vs 19.9%).

HONEST LIMITATIONS -- read before trusting this blindly:
  - Modest sample size, single ~2.5-month window (no earnings season,
    no crash, no major regime change represented). Needs revisiting as
    more data accumulates.
  - Two training features have no clean live equivalent, and are
    approximated here:
      * prior_touches_today: defaults to 0 (this function doesn't track
        touch history across a live session). This was the LEAST
        important feature in training (1.3% importance), so the impact
        of this approximation should be small.
      * "kind" (support vs resistance) was determined in training from
        the PRIOR candle's close (which direction price was coming
        from). Live, we approximate this with the zone's position
        relative to current price instead -- a reasonable stand-in,
        but not identical.
  - Use this as a CONFIDENCE FILTER on top of the existing rule-based
    zones, not a replacement for them, and not a blind auto-trade signal.
"""
import os
import re
import joblib
import pandas as pd

_MODEL_PATH = os.path.join(os.path.dirname(__file__), "zone_break_model.pkl")
_model = None


def _get_model():
    global _model
    if _model is None:
        _model = joblib.load(_MODEL_PATH)
    return _model


def _pct_from_label(label):
    m = re.search(r"[\d.]+", str(label))
    return float(m.group()) if m else 0.0


def predict_break_probability(zone, ltp, vwap, day_open, session_start_time, session_end_time, now_time,
                                is_intraday_validated):
    """
    zone: a zone dict with price_mode, price_low, price_high, label.
    ltp: current last traded price.
    vwap: current session VWAP (None if unavailable -- returns None).
    day_open: today's opening price (for day_change_pct_so_far).
    session_start_time / session_end_time / now_time: datetime.time
        objects (market open, market close, and current time) -- used
        to compute how far through the session we are.
    is_intraday_validated: bool -- is this composite zone ALSO confirmed
        by today's intraday profile right now.

    Returns a float probability (0-1) that this zone BREAKS rather than
    holds, or None if required inputs are missing.
    """
    if ltp is None or vwap is None or day_open is None:
        return None

    price_mode = zone["price_mode"]
    price_low, price_high = zone["price_low"], zone["price_high"]

    zone_pct = _pct_from_label(zone.get("label", ""))
    zone_width_pct = (price_high - price_low) / price_mode * 100 if price_mode else 0.0
    vwap_dist_signed = (ltp - vwap) / ltp * 100 if ltp else 0.0
    vwap_dist_pct = abs(vwap_dist_signed)
    vwap_above = 1 if vwap_dist_signed > 0 else 0
    day_change_pct_so_far = (ltp - day_open) / day_open * 100 if day_open else 0.0
    is_support = 1 if price_mode <= ltp else 0  # position-based approximation, see module docstring

    # fraction of the session elapsed, clipped to [0, 1]
    total_session_secs = (session_end_time.hour * 3600 + session_end_time.minute * 60) - \
                          (session_start_time.hour * 3600 + session_start_time.minute * 60)
    elapsed_secs = (now_time.hour * 3600 + now_time.minute * 60) - \
                   (session_start_time.hour * 3600 + session_start_time.minute * 60)
    time_of_day_frac = max(0.0, min(1.0, elapsed_secs / total_session_secs)) if total_session_secs > 0 else 0.0

    features = pd.DataFrame([{
        "zone_pct": zone_pct,
        "zone_width_pct": zone_width_pct,
        "vwap_dist_pct": vwap_dist_pct,
        "vwap_above": vwap_above,
        "day_change_pct_so_far": day_change_pct_so_far,
        "prior_touches_today": 0,  # approximation -- see module docstring
        "is_intraday_validated": int(is_intraday_validated),
        "is_support": is_support,
        "time_of_day_frac": time_of_day_frac,
    }])

    model = _get_model()
    prob = model.predict_proba(features)[0, 1]
    return float(prob)
