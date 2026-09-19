"""
zone_validation.py - cross-timeframe validation for Sahi-style Key Levels.

A zone found in only one timeframe (e.g. a brief intraday cluster with no
multi-day support, or a composite zone today's session hasn't touched at
all) is treated as noise. A zone is VALIDATED when its price range
overlaps a zone from the OTHER timeframe -- i.e. both the composite
(multi-day) profile and today's intraday profile independently agree
there's real volume concentrated around that price.

This mirrors the existing HVN/LVN "today_pool + composite_pool" confirms
pattern already used in hvn-lvn-scanner's app.py, applied here to
Sahi-style collapsed zones (dicts with price_low/price_high/price_mode)
instead of raw HVN/LVN nodes.
"""
from typing import List, Dict, Optional, Tuple


def zones_overlap(zone_a: Dict, zone_b: Dict) -> bool:
    """True if two zones' [price_low, price_high] ranges overlap at all."""
    return zone_a["price_low"] <= zone_b["price_high"] and zone_b["price_low"] <= zone_a["price_high"]


def filter_validated_zones(zones: List[Dict], other_zones: List[Dict]) -> List[Dict]:
    """Keep only the zones (from `zones`) whose range overlaps at least
    one zone in `other_zones`."""
    return [z for z in zones if any(zones_overlap(z, o) for o in other_zones)]


def cross_validated_zones(
    composite_zones: List[Dict], intraday_zones: List[Dict]
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Returns (validated_composite, validated_intraday, validated_merged):
      - validated_composite: composite zones confirmed by an overlapping intraday zone
      - validated_intraday: intraday zones confirmed by an overlapping composite zone
      - validated_merged: union of the two, deduplicated by overlap -- used
        for signal generation so there's one nearest-level check regardless
        of which timeframe originally found the zone
    """
    validated_composite = filter_validated_zones(composite_zones, intraday_zones)
    validated_intraday = filter_validated_zones(intraday_zones, composite_zones)

    merged = list(validated_composite)
    for z in validated_intraday:
        if not any(zones_overlap(z, m) for m in merged):
            merged.append(z)
    return validated_composite, validated_intraday, merged


def nearest_zone_price_above(zones: List[Dict], price: float) -> Optional[float]:
    candidates = [z["price_mode"] for z in zones if z["price_mode"] > price]
    return min(candidates) if candidates else None


def nearest_zone_price_below(zones: List[Dict], price: float) -> Optional[float]:
    candidates = [z["price_mode"] for z in zones if z["price_mode"] < price]
    return max(candidates) if candidates else None


def compute_zone_signal(
    ltp: float,
    vwap: float,
    composite_zones: List[Dict],
    intraday_zones: List[Dict],
    min_distance_pct: float = 0.5,
    min_vwap_distance_pct: float = 0.15,
) -> str:
    """
    bias = long if LTP > VWAP, short if LTP < VWAP (same convention as the
    existing HVN/LVN signal logic in hvn-lvn-scanner).

    NOISE FILTER: a razor-thin LTP/VWAP gap (e.g. 0.02%) technically has a
    "bias" but isn't a real move -- it's LTP sitting on top of VWAP and
    wobbling a few paise either side. min_vwap_distance_pct requires LTP
    to actually be away from VWAP by a meaningful amount before a bias is
    considered real at all. Found necessary in practice: a live run
    without this produced 10 alerts in 3 minutes, several on the same
    symbol flipping back and forth, almost all with LTP/VWAP gaps under
    0.05%.

    BUY if bias == long AND (no validated zone above LTP, OR the nearest
        one is at least min_distance_pct away -- i.e. room to run before
        hitting a level both timeframes agree is real).
    SELL mirrors this on the downside.
    Otherwise: no signal.
    """
    if ltp is None or vwap is None or ltp <= 0:
        return "-"

    vwap_gap_pct = abs(ltp - vwap) / ltp * 100
    if vwap_gap_pct < min_vwap_distance_pct:
        return "-"

    _, _, validated = cross_validated_zones(composite_zones, intraday_zones)

    if ltp > vwap:
        above = nearest_zone_price_above(validated, ltp)
        confirms = above is None or ((above - ltp) / ltp * 100 >= min_distance_pct)
        return "BUY" if confirms else "-"
    elif ltp < vwap:
        below = nearest_zone_price_below(validated, ltp)
        confirms = below is None or ((ltp - below) / ltp * 100 >= min_distance_pct)
        return "SELL" if confirms else "-"
    return "-"


# Default weights for compute_cvd_zone_signal's composite score -- must
# sum to 100 when every component is available (see that function's
# docstring for what each one measures). Kept as a module-level default
# rather than baked into the function so a caller can experiment with
# re-weighting without touching this file, e.g. weighting order_flow
# more heavily than ml_confidence for a more CVD-led variant.
CVD_SIGNAL_WEIGHTS = {
    "ml_confidence": 30,  # ML model's confidence the zone HOLDS (100 - break-risk%)
    "order_flow": 30,     # recent CVD-derived buying/selling pressure agreeing with the trade
    "rvol": 20,            # today's relative volume -- conviction behind the move
    "room": 20,             # % room to the next validated zone -- the reward side of R:R
}
CVD_SIGNAL_BUY_THRESHOLD = 70    # score >= this -> BUY/SELL (strong composite confirmation)
CVD_SIGNAL_WATCH_THRESHOLD = 45  # score >= this (but below BUY) -> WATCH (worth tracking, not yet firing)
CVD_SIGNAL_MIN_DATA_COVERAGE = 0.5  # fraction of total component weight that must have real data
                                     # (not just defaults) before a BUY/SELL/WATCH grade is awarded


def compute_cvd_zone_signal(
    ltp: float,
    vwap: float,
    side: str,
    distance_pct: float,
    room_pct: Optional[float] = None,
    order_flow_imbalance_pct: Optional[float] = None,
    rvol_pct: Optional[float] = None,
    ml_break_risk_pct: Optional[float] = None,
    min_vwap_distance_pct: float = 0.15,
    weights: Optional[Dict[str, float]] = None,
) -> Dict:
    """
    "Quality signal" for a zone test, layered ON TOP of compute_zone_signal's
    pass/fail bias -- this doesn't replace that gate, it scores HOW GOOD a
    candidate the current support/resistance test is, as a single weighted
    composite 0-100 score rather than a strict "every factor must align"
    filter (deliberately: a razor-perfect setup with one weak factor still
    shows up here as a strong-but-imperfect candidate instead of vanishing
    entirely, matching the preference for weighing evidence over hard gates).

    ltp / vwap: current price / session VWAP.
    side: "support" (LTP testing a floor from above -- a BUY/long
        candidate) or "resistance" (LTP testing a ceiling from below --
        a SELL/short candidate).
    distance_pct: % distance from LTP to the zone's near edge right now
        (how close the test actually is) -- informational, kept in the
        returned components for display, but not itself scored: proximity
        is already what put this zone in front of you (see NEAR_ZONE_PCT
        gating in app.py), so scoring it again would double-count it.
    room_pct: % distance from LTP to the NEXT validated zone beyond this
        one, in the trade's direction -- the reward side of risk/reward.
        None means open air (no further zone found), which is treated as
        the BEST case (max score for this component), not a penalty --
        same convention as the existing bottom_setups/top_setups "room"
        fields in app.py.
    order_flow_imbalance_pct: recent normalized CVD reading, e.g. from
        candles_with_levels.compute_recent_order_flow_imbalance_pct --
        positive = net recent buying pressure, negative = net selling.
    rvol_pct: today's relative volume vs the N-day average (same
        convention as the Scanner tab's RVOL% column) -- elevated RVOL is
        the conviction filter that a move is real, not noise.
    ml_break_risk_pct: predict_break_probability()'s output * 100 -- the
        trained model's estimate that this zone BREAKS rather than holds.
        Lower is better for this signal (a confident BUY/SELL wants the
        zone to hold).
    min_vwap_distance_pct: same noise filter as compute_zone_signal -- a
        razor-thin LTP/VWAP gap doesn't count as a real bias. This is a
        HARD GATE (score forced to 0), not a scored component: a setup
        with no real directional bias yet isn't "a weak BUY", it's not a
        BUY candidate at all.
    weights: optional override of CVD_SIGNAL_WEIGHTS.

    Returns {"score": 0.0-100.0, "grade": "BUY"/"SELL"/"WATCH"/"-",
        "components": {name: 0-100, ...}}. Any input left as None simply
        drops that component's weight from both the numerator and the
        denominator (the score is renormalized over whatever's actually
        available) rather than failing the whole call -- so this degrades
        gracefully early in the session before CVD/RVOL data has built up,
        instead of returning nothing until every input is present.

    grade: "BUY" (side="support") / "SELL" (side="resistance") at
        score >= CVD_SIGNAL_BUY_THRESHOLD, "WATCH" at
        score >= CVD_SIGNAL_WATCH_THRESHOLD, else "-". This is a display
        tier, not a new gate -- compute_zone_signal remains the actual
        BUY/SELL signal used for alerting; this score ranks/qualifies
        candidates that gate already lets through. A grade is only
        awarded when at least CVD_SIGNAL_MIN_DATA_COVERAGE of the total
        component weight had real data behind it -- e.g. if only
        room_pct is known (everything else None), the score still
        reflects that, but it can't earn BUY off room_pct's generous
        "open air" default alone.
    """
    result = {"score": 0.0, "grade": "-", "components": {"distance_pct": distance_pct}}

    if ltp is None or vwap is None or ltp <= 0 or side not in ("support", "resistance"):
        return result

    vwap_gap_signed_pct = (ltp - vwap) / ltp * 100
    bias_gap = vwap_gap_signed_pct if side == "support" else -vwap_gap_signed_pct
    if bias_gap < min_vwap_distance_pct:
        return result  # no real directional bias yet -- hard gate, score stays 0

    w = weights if weights is not None else CVD_SIGNAL_WEIGHTS
    weighted_sum = 0.0
    weight_used = 0.0

    if ml_break_risk_pct is not None:
        c = max(0.0, min(100.0, 100.0 - ml_break_risk_pct))
        result["components"]["ml_confidence"] = round(c, 1)
        weighted_sum += c * w.get("ml_confidence", 0)
        weight_used += w.get("ml_confidence", 0)

    if order_flow_imbalance_pct is not None:
        # flip sign for resistance/short: net SELLING pressure is what
        # confirms a short, so it should score the same as net BUYING
        # pressure does for a long.
        signed = order_flow_imbalance_pct if side == "support" else -order_flow_imbalance_pct
        # +/-20% of recent-window volume as net order flow is treated as
        # a maximally strong reading (100); 0% (balanced) sits at the
        # component's midpoint (50).
        c = max(0.0, min(100.0, (signed + 20.0) / 40.0 * 100.0))
        result["components"]["order_flow"] = round(c, 1)
        weighted_sum += c * w.get("order_flow", 0)
        weight_used += w.get("order_flow", 0)

    if rvol_pct is not None:
        # 50% RVOL (well below average) floors at 0; 200%+ RVOL caps at 100.
        c = max(0.0, min(100.0, (rvol_pct - 50.0) / 150.0 * 100.0))
        result["components"]["rvol"] = round(c, 1)
        weighted_sum += c * w.get("rvol", 0)
        weight_used += w.get("rvol", 0)

    room_weight = w.get("room", 0)
    if room_weight:
        c = 100.0 if room_pct is None else max(0.0, min(100.0, room_pct / 1.0 * 100.0))
        result["components"]["room"] = round(c, 1)
        weighted_sum += c * room_weight
        weight_used += room_weight

    if weight_used <= 0:
        return result

    score = weighted_sum / weight_used
    result["score"] = round(score, 1)

    # Grading requires enough of the weighted inputs to actually be
    # present -- without this, a symbol with EVERYTHING missing except
    # room_pct=None (which defaults to a generous 100, since "no next
    # zone found yet" isn't a penalty) would score a perfect 100 and
    # grade BUY off a single, mostly-uninformative component. A score
    # is still returned either way (useful context), but it only earns
    # a BUY/SELL/WATCH tier once at least MIN_DATA_COVERAGE of the total
    # possible weight actually had real data behind it.
    total_weight = sum(w.get(k, 0) for k in ("ml_confidence", "order_flow", "rvol", "room"))
    has_enough_data = total_weight > 0 and (weight_used / total_weight) >= CVD_SIGNAL_MIN_DATA_COVERAGE

    if has_enough_data:
        if score >= CVD_SIGNAL_BUY_THRESHOLD:
            result["grade"] = "BUY" if side == "support" else "SELL"
        elif score >= CVD_SIGNAL_WATCH_THRESHOLD:
            result["grade"] = "WATCH"

    return result
