"""
Sahi-style "Key Levels" zone summary.

Thin layer on top of hvn_lvn.py: reuses build_volume_profile() and
find_hvn_lvn() to build the profile and find HVN/LVN nodes, then collapses
the profile into contiguous price ZONES cut at the LVN valleys -- one zone
per HVN cluster, each labeled with its % share of session volume. This is
what produces the Sahi app's "Key Levels (N, M)" style overlay.

Usage (from app.py, right after / instead of compute_hvn_lvn):

    from sahi_style_key_levels import sahi_style_key_levels

    today_df = df[df["date"] == df["date"].max()]
    all_zones, shown_zones = sahi_style_key_levels(today_df, n_bins=TODAY_N_BINS)

    for z in shown_zones:
        print(z.price_mode, z.label, z.color)
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Tuple

from hvn_lvn import build_volume_profile, find_hvn_lvn


@dataclass
class KeyLevelZone:
    price_low: float
    price_high: float
    price_mode: float      # price of peak volume inside the zone (~the HVN)
    volume: float
    pct_of_session: float
    color: str
    label: str


def _segment_at_valleys(price_bins, volumes, lvn_prices, max_zones=6):
    """
    Cuts (price_bins, volumes) into contiguous segments at the given LVN
    prices (the natural valleys find_hvn_lvn already found), then merges
    the smallest segments into a neighbor until at most max_zones remain.
    """
    n = len(price_bins)
    total_vol = volumes.sum()
    if total_vol <= 0 or n == 0:
        return []

    cut_idxs = sorted(set(
        int(np.argmin(np.abs(price_bins - p))) + 1 for p in lvn_prices
    ))
    cut_points = sorted(set([0] + [c for c in cut_idxs if 0 < c < n] + [n]))

    segments = []
    for start, end in zip(cut_points[:-1], cut_points[1:]):
        seg_vols = volumes[start:end]
        seg_bins = price_bins[start:end]
        seg_vol = seg_vols.sum()
        if seg_vol <= 0:
            continue
        peak_idx = int(np.argmax(seg_vols))
        segments.append({
            "price_low": seg_bins.min(),
            "price_high": seg_bins.max(),
            "price_mode": seg_bins[peak_idx],
            "volume": seg_vol,
        })

    while len(segments) > max_zones:
        smallest_idx = min(range(len(segments)), key=lambda i: segments[i]["volume"])
        merge_with = smallest_idx - 1 if smallest_idx > 0 else smallest_idx + 1
        a, b = sorted([smallest_idx, merge_with])
        merged = {
            "price_low": min(segments[a]["price_low"], segments[b]["price_low"]),
            "price_high": max(segments[a]["price_high"], segments[b]["price_high"]),
            "volume": segments[a]["volume"] + segments[b]["volume"],
            "price_mode": (segments[a]["price_mode"] if segments[a]["volume"] >= segments[b]["volume"]
                           else segments[b]["price_mode"]),
        }
        segments = segments[:a] + [merged] + segments[b + 1:]

    segments.sort(key=lambda s: s["price_high"], reverse=True)
    return segments


def _label_zones(segments) -> List[KeyLevelZone]:
    total_vol = sum(s["volume"] for s in segments)
    palette = ["#e05252", "#e0a336", "#3d7fd6", "#e0c93d", "#7d52d6", "#52c9a3"]
    zones = []
    for i, seg in enumerate(segments):
        pct = seg["volume"] / total_vol if total_vol else 0
        zones.append(KeyLevelZone(
            price_low=round(float(seg["price_low"]), 2),
            price_high=round(float(seg["price_high"]), 2),
            price_mode=round(float(seg["price_mode"]), 2),
            volume=float(seg["volume"]),
            pct_of_session=round(pct * 100, 1),
            color=palette[i % len(palette)],
            label=f"{round(pct * 100)}%",
        ))
    return zones


def sahi_style_key_levels(
    candle_df: pd.DataFrame,
    n_bins: int = 30,
    max_zones: int = 6,
    min_display_pct: float = 3.0,
    min_prominence_pct: float = 0.15,
    min_bin_distance: int = 2,
) -> Tuple[List[KeyLevelZone], List[KeyLevelZone]]:
    """
    OHLCV candles in -> (all_zones, displayed_zones) out.

    candle_df: already filtered to the window you want (e.g. today_df in
        app.py). Must have 'high', 'low', 'volume' columns.
    n_bins: same fixed-bin-count already used for TODAY_N_BINS /
        COMPOSITE_N_BINS in app.py -- pass the same value for consistency.
    min_prominence_pct / min_bin_distance: passed straight through to
        find_hvn_lvn() -- keep these matched to whatever app.py uses so
        zones line up with the HVN/LVN markers already on your chart.
    """
    if candle_df.empty:
        return [], []

    price_range = candle_df["high"].max() - candle_df["low"].min()
    bin_size = max(price_range / n_bins, 0.01) if price_range > 0 else 0.01

    price_bins, volumes = build_volume_profile(candle_df, bin_size=bin_size)
    result = find_hvn_lvn(
        price_bins, volumes,
        min_prominence_pct=min_prominence_pct,
        min_bin_distance=min_bin_distance,
    )
    lvn_prices = [n["price"] for n in result["lvns"]]

    segments = _segment_at_valleys(price_bins, volumes, lvn_prices, max_zones=max_zones)
    zones = _label_zones(segments)
    shown = [z for z in zones if z.pct_of_session >= min_display_pct]
    return zones, shown


def render_zones_plotly(fig, zones: List[KeyLevelZone], x0=None, x1=None):
    """Overlay zones as horizontal bands on an existing Plotly figure."""
    for z in zones:
        fig.add_hrect(
            y0=z.price_low, y1=z.price_high,
            x0=x0, x1=x1,
            fillcolor=z.color, opacity=0.15,
            line_width=1, line_color=z.color,
            annotation_text=f"{z.price_mode:g}  {z.label}",
            annotation_position="right",
        )
    return fig


if __name__ == "__main__":
    # Synthetic NIFTY-futures-like session: two high-volume clusters (HVNs)
    # separated by a thin transition zone, spanning a realistic ~230 point
    # session range (matching the real Sahi screenshot: 24065-24296).
    rng = np.random.default_rng(42)
    n = 75  # ~6.25 hours of 5-min bars
    base = 24150 + np.cumsum(rng.normal(0, 15, n))
    highs = base + rng.uniform(5, 25, n)
    lows = base - rng.uniform(5, 25, n)
    vols = rng.uniform(5000, 50000, n)
    vols[10:20] *= 4
    vols[45:55] *= 3

    test_df = pd.DataFrame({"high": highs, "low": lows, "volume": vols})
    all_zones, shown = sahi_style_key_levels(test_df)

    print(f"Key Levels ({len(all_zones)}, {len(shown)})")
    for z in shown:
        print(f"  {z.price_mode:>10.2f}  vol%={z.label:<5}  "
              f"range=[{z.price_low:.2f}, {z.price_high:.2f}]  color={z.color}")
