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
