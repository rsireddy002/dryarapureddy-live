"""
High Volume Node / Low Volume Node detection.

Built to sit on top of composite_profile.py's volume-by-price histogram for
NIFTY futures (near-month, composite multi-day profile). Extend to other
instruments once this is validated.

ASSUMPTION: composite_profile() exposes (or can be modified to expose) the
raw histogram as two parallel arrays:
    price_bins : ascending array of bin-center prices
    volumes    : array of total volume traded in each bin

If composite_profile.py currently only returns POC/VAH/VAL, add a return
path (or a second function) that also hands back price_bins/volumes before
wiring this in.
"""

import numpy as np


def build_volume_profile(df, bin_size=5.0):
    """
    Build a real volume-by-price histogram from 5-min OHLCV candles.

    fno_delta_dashboard.py's compute_levels_and_baseline() does NOT build one
    of these -- its "POC/VAH/VAL" are actually VWAP and VWAP +/- a fixed pct
    of day range, not a volume-by-price calculation. This function fills
    that gap so find_hvn_lvn() below has real data to work on.

    Each candle's volume is spread evenly across every price bin its
    high-low range touches. This is the standard approximation when you
    only have OHLCV bars (not tick data) -- it's not exact, but it's far
    closer to a true profile than assigning all volume to a single price.

    Args:
        df: DataFrame with 'high', 'low', 'volume' columns. Filter this to
            the window you want BEFORE calling -- e.g. today's rows only
            for a developing profile, or the last 10-15 sessions for a
            composite profile.
        bin_size: price increment per bin. Start around 5.0 for NIFTY
            futures on a single-day profile; widen to 10-15 for a
            multi-day composite profile since price ranges more.

    Returns:
        (price_bins, volumes) -- numpy arrays, feed straight into find_hvn_lvn()
    """
    if df.empty:
        raise ValueError("df is empty - nothing to build a profile from")

    lo = np.floor(df["low"].min() / bin_size) * bin_size
    hi = np.ceil(df["high"].max() / bin_size) * bin_size
    bin_edges = np.arange(lo, hi + bin_size, bin_size)
    bin_centers = bin_edges[:-1] + bin_size / 2
    volumes = np.zeros(len(bin_centers))

    lows = df["low"].to_numpy()
    highs = df["high"].to_numpy()
    vols = df["volume"].to_numpy()

    for candle_lo, candle_hi, vol in zip(lows, highs, vols):
        if vol <= 0:
            continue
        touched = np.where((bin_edges[:-1] < candle_hi) & (bin_edges[1:] > candle_lo))[0]
        if len(touched) == 0:
            continue
        volumes[touched] += vol / len(touched)

    return bin_centers, volumes


def build_volume_profile_from_candles(candles, bin_size):
    """
    Same as build_volume_profile(), but takes RAW Upstox candle rows
    directly (list of [timestamp, open, high, low, close, volume, oi]) --
    written for repos like upstox-feed-listener that deliberately don't
    depend on pandas (it works with raw lists/dicts throughout, to keep the
    tick-processing loop lightweight).

    Args:
        candles: list of raw candle rows as returned by Upstox's
            historical-candle API (index 2=high, 3=low, 5=volume)
        bin_size: price increment per bin

    Returns:
        (price_bins, volumes) -- numpy arrays, feed straight into find_hvn_lvn()
    """
    if not candles:
        raise ValueError("candles is empty - nothing to build a profile from")

    highs = np.array([row[2] for row in candles], dtype=float)
    lows = np.array([row[3] for row in candles], dtype=float)
    vols = np.array([row[5] for row in candles], dtype=float)

    lo = np.floor(lows.min() / bin_size) * bin_size
    hi = np.ceil(highs.max() / bin_size) * bin_size
    bin_edges = np.arange(lo, hi + bin_size, bin_size)
    bin_centers = bin_edges[:-1] + bin_size / 2
    volumes = np.zeros(len(bin_centers))

    for candle_lo, candle_hi, vol in zip(lows, highs, vols):
        if vol <= 0:
            continue
        touched = np.where((bin_edges[:-1] < candle_hi) & (bin_edges[1:] > candle_lo))[0]
        if len(touched) == 0:
            continue
        volumes[touched] += vol / len(touched)

    return bin_centers, volumes


def find_hvn_lvn(price_bins, volumes, min_prominence_pct=0.15, min_bin_distance=2):
    """
    Identify High Volume Nodes (HVN) and Low Volume Nodes (LVN) from a
    volume profile histogram.

    Args:
        price_bins: array of bin-center prices, ascending
        volumes: array of volume per bin, same length as price_bins
        min_prominence_pct: how far above the mean bin volume a peak must
            rise, as a fraction of (max_volume - mean_volume), to count as
            a real HVN rather than noise. 0.15 = must clear 15% of that range.
        min_bin_distance: minimum number of bins between two accepted HVNs,
            so a single wide node doesn't get counted as two peaks.

    Returns:
        {
          "hvns": [{"price": float, "volume": float, "rank": int}, ...],  # rank 1 = POC
          "lvns": [{"price": float, "volume": float}, ...]  # gaps between consecutive HVNs
        }
    """
    volumes = np.asarray(volumes, dtype=float)
    price_bins = np.asarray(price_bins, dtype=float)
    n = len(volumes)

    if n < 3:
        raise ValueError("Need at least 3 bins to detect local extrema")

    mean_vol = volumes.mean()
    max_vol = volumes.max()
    prominence_floor = mean_vol + min_prominence_pct * (max_vol - mean_vol)

    # local maxima: strictly greater than both immediate neighbors, and clears the noise floor
    hvn_idx = [
        i for i in range(1, n - 1)
        if volumes[i] > volumes[i - 1] and volumes[i] > volumes[i + 1] and volumes[i] >= prominence_floor
    ]

    # enforce min_bin_distance: if two peaks are close, keep the taller one
    hvn_idx.sort(key=lambda i: -volumes[i])
    kept = []
    for i in hvn_idx:
        if all(abs(i - k) >= min_bin_distance for k in kept):
            kept.append(i)
    kept.sort()  # ascending price order for LVN pass below

    ranked = sorted(kept, key=lambda i: -volumes[i])
    hvns = [
        {"price": round(float(price_bins[i]), 2), "volume": float(volumes[i]), "rank": r + 1}
        for r, i in enumerate(ranked)
    ]

    # LVNs: the volume trough between each pair of consecutive (price-ordered) HVNs
    lvns = []
    for a, b in zip(kept, kept[1:]):
        seg = volumes[a:b + 1]
        trough_i = a + int(np.argmin(seg))
        lvns.append({"price": round(float(price_bins[trough_i]), 2), "volume": float(volumes[trough_i])})

    return {"hvns": hvns, "lvns": lvns}


if __name__ == "__main__":
    import pandas as pd

    # --- Test 1: find_hvn_lvn on synthetic histogram (as before) ---
    prices = np.arange(24000, 24800, 5)
    vols = (
        3000 * np.exp(-((prices - 24300) ** 2) / (2 * 30 ** 2))
        + 6000 * np.exp(-((prices - 24450) ** 2) / (2 * 25 ** 2))  # this should be POC
        + 2500 * np.exp(-((prices - 24600) ** 2) / (2 * 30 ** 2))
        + np.random.default_rng(0).normal(50, 15, len(prices)).clip(min=0)
    )
    result = find_hvn_lvn(prices, vols)
    print("=== Test 1: find_hvn_lvn on synthetic histogram ===")
    print("HVNs (rank 1 = POC):")
    for h in result["hvns"]:
        print(f"  rank {h['rank']}: {h['price']}  vol={h['volume']:.0f}")
    print("LVNs (gaps between HVNs):")
    for l in result["lvns"]:
        print(f"  {l['price']}  vol={l['volume']:.0f}")

    # --- Test 2: build_volume_profile from fake OHLCV candles, then find_hvn_lvn ---
    # Simulates what real 5-min candles from fetch_candles() would look like.
    rng = np.random.default_rng(1)
    n = 75  # one session's worth of 5-min bars
    closes = 24450 + np.cumsum(rng.normal(0, 8, n))
    highs = closes + rng.uniform(2, 15, n)
    lows = closes - rng.uniform(2, 15, n)
    volumes = rng.integers(500, 5000, n)
    fake_df = pd.DataFrame({"high": highs, "low": lows, "volume": volumes})

    price_bins, hist_volumes = build_volume_profile(fake_df, bin_size=5.0)
    result2 = find_hvn_lvn(price_bins, hist_volumes)
    print("\n=== Test 2: build_volume_profile -> find_hvn_lvn on fake OHLCV candles ===")
    print("HVNs (rank 1 = POC):")
    for h in result2["hvns"]:
        print(f"  rank {h['rank']}: {h['price']}  vol={h['volume']:.0f}")
    print("LVNs (gaps between HVNs):")
    for l in result2["lvns"]:
        print(f"  {l['price']}  vol={l['volume']:.0f}")
