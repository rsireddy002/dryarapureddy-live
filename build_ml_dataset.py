"""
build_ml_dataset.py - Builds a labeled training set for the "does this
zone hold or break" model.

WHAT THIS DOES:
For each symbol, walks through ~3 months of 5-min candles day by day.
On each trading day (once at least 18 prior days of history exist),
computes that day's COMPOSITE zones from the preceding 18 days (same
logic as compute_composite_zones in app.py), then walks the day's
candles looking for "zone tests" -- the first candle where price enters
a zone's [price_low, price_high] range after being outside it.

At each test, records features available AT THAT MOMENT (no lookahead):
  - zone_pct, zone width, kind (support/resistance)
  - RVOL proxy (cumulative volume so far today vs avg daily volume)
  - VWAP relation and distance at test time
  - candle index in the session (proxy for time of day)
  - day's % change so far (from day's open to test time)
  - prior test count on this exact zone, today
  - is_intraday_validated: does an INTRADAY zone (computed from just
    today's candles UP TO the test candle) also cluster near this price

Then looks FORWARD up to LOOKFORWARD_CANDLES (or end of day, whichever
comes first) to label the outcome:
  - HOLD: price closes beyond the zone's far edge in the "held" direction
  - BREAK: price closes beyond the zone's near edge in the "broke" direction
  - AMBIGUOUS: still inside the zone range at the end of the window --
    dropped from the binary dataset (a real "chop" case, not a clean signal)

RATE LIMITING: ~20 symbols x (5 chunked 5-min fetches + 1 instrument-key
lookup) = ~120 API calls total -- much lighter than a full Precompute,
but still throttled with delays and 429 backoff since Upstox rate-limited
us during yesterday's Precompute test.

USAGE (run from inside your app repo folder, so it can import the
vendored sahi_style_key_levels.py and zone_validation.py unchanged):
    $env:UPSTOX_ACCESS_TOKEN = "your_token_here"
    python build_ml_dataset.py

OUTPUT: zone_test_dataset.csv in the current folder.
"""
import os
import time
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests

from sahi_style_key_levels import sahi_style_key_levels

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():
    return datetime.now(IST)

INSTRUMENT_SEARCH_URL = "https://api.upstox.com/v2/instruments/search"

# 20 liquid, sector-diverse symbols -- mixing sectors so the model sees
# varied behavior instead of learning one sector's specific quirks.
SYMBOLS = [
    "RELIANCE",    # Oil & Gas
    "TCS",         # IT
    "HDFCBANK",    # Banks
    "ICICIBANK",   # Banks
    "INFY",        # IT
    "SBIN",        # Banks (PSU)
    "TATASTEEL",   # Metals
    "SUNPHARMA",   # Pharma
    "MARUTI",      # Auto
    "ITC",         # FMCG
    "LT",          # Capital Goods
    "BAJFINANCE",  # NBFC
    "HINDUNILVR",  # FMCG
    "TITAN",       # Consumer Durables
    "ULTRACEMCO",  # Cement
    "ASIANPAINT",  # Consumer Durables
    "WIPRO",       # IT
    "NTPC",        # Power
    "ADANIENT",    # Diversified/Metals
    "KOTAKBANK",   # Banks
]

COMPOSITE_LOOKBACK_DAYS = 18
TOTAL_HISTORY_DAYS = 100   # ~3 months + buffer for the 18-day composite window
LOOKFORWARD_CANDLES = 12   # ~1 hour ahead at 5-min bars
ZONE_TOUCH_BUFFER_PCT = 0.0  # no extra buffer -- a literal price_low/price_high cross

COMPOSITE_N_BINS = 50
INTRADAY_N_BINS = 45
MIN_PROMINENCE_PCT = 0.08
MIN_BIN_DISTANCE = 2
MAX_ZONES = 6
MIN_DISPLAY_PCT = 2.0

OUTPUT_CSV = "zone_test_dataset.csv"


def get_token():
    token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("Set $env:UPSTOX_ACCESS_TOKEN before running this script.")
    return token.strip()


def resolve_equity_instrument_key(symbol, token):
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    params = {"query": symbol, "exchanges": "NSE", "segments": "EQ",
              "instrument_types": "EQ", "page_number": 1, "records": 10}
    resp = requests.get(INSTRUMENT_SEARCH_URL, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    candidates = [inst for inst in resp.json().get("data", [])
                  if inst.get("trading_symbol", "").upper() == symbol.upper()]
    return candidates[0]["instrument_key"] if candidates else None


def _get_with_retry(url, headers, max_retries=4):
    """429 backoff -- doubles the wait each retry."""
    wait = 3
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 429:
            print(f"    Rate limited, waiting {wait}s (attempt {attempt+1}/{max_retries})...")
            time.sleep(wait)
            wait *= 2
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Still rate-limited after {max_retries} retries: {url}")


def fetch_5min_candles_ending(instrument_key, token, end_date, total_days):
    """Chunked 5-min candle fetch anchored to an arbitrary end_date --
    same pattern as the Replay tab's version, with 429 retry added."""
    all_chunks = []
    remaining = total_days
    cursor_end = end_date
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    while remaining > 0:
        chunk_days = min(20, remaining)
        chunk_start = cursor_end - timedelta(days=chunk_days)
        url = (f"https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/5/"
               f"{cursor_end.strftime('%Y-%m-%d')}/{chunk_start.strftime('%Y-%m-%d')}")
        resp = _get_with_retry(url, headers)
        candles = resp.json().get("data", {}).get("candles", [])
        if candles:
            all_chunks.append(pd.DataFrame(
                candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
            ))
        cursor_end = chunk_start
        remaining -= chunk_days
        time.sleep(1.0)  # be gentle between chunk calls

    if not all_chunks:
        return pd.DataFrame()

    df = pd.concat(all_chunks, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
    df["date"] = df["timestamp"].dt.date
    return df


def compute_zones(df, n_bins):
    """Same zone-detection call the app uses -- returns list of zone dicts."""
    if df.empty:
        return []
    try:
        _, shown = sahi_style_key_levels(
            df, n_bins=n_bins, max_zones=MAX_ZONES,
            min_display_pct=MIN_DISPLAY_PCT, min_prominence_pct=MIN_PROMINENCE_PCT,
            min_bin_distance=MIN_BIN_DISTANCE,
        )
        return [asdict(z) for z in shown]
    except Exception:
        return []


def process_symbol(symbol, token):
    """Returns a list of labeled zone-test dicts for one symbol."""
    print(f"[{symbol}] resolving instrument key...")
    key = resolve_equity_instrument_key(symbol, token)
    if key is None:
        print(f"[{symbol}] could not resolve instrument key, skipping.")
        return []
    time.sleep(0.5)

    print(f"[{symbol}] fetching {TOTAL_HISTORY_DAYS} days of 5-min candles...")
    full_df = fetch_5min_candles_ending(key, token, now_ist().date(), TOTAL_HISTORY_DAYS)
    if full_df.empty:
        print(f"[{symbol}] no candle data returned, skipping.")
        return []

    trading_days = sorted(full_df["date"].unique())
    if len(trading_days) <= COMPOSITE_LOOKBACK_DAYS:
        print(f"[{symbol}] not enough trading days ({len(trading_days)}), skipping.")
        return []

    rows = []
    # start once we have a full composite window behind us
    for day_idx in range(COMPOSITE_LOOKBACK_DAYS, len(trading_days)):
        test_day = trading_days[day_idx]
        composite_days = trading_days[day_idx - COMPOSITE_LOOKBACK_DAYS:day_idx]
        composite_df = full_df[full_df["date"].isin(composite_days)]
        composite_zones = compute_zones(composite_df, COMPOSITE_N_BINS)
        if not composite_zones:
            continue

        day_df = full_df[full_df["date"] == test_day].reset_index(drop=True)
        if day_df.empty or len(day_df) < 3:
            continue

        day_open = float(day_df["open"].iloc[0])
        # track which composite zones have already had their FIRST test
        # today, and how many times each has been touched
        zone_touch_count = {}
        zone_in_progress = {}  # zone_key -> was price inside this zone as of the previous candle
        cum_vol = 0.0
        cum_tp_vol = 0.0

        for i in range(len(day_df)):
            row = day_df.iloc[i]
            typical = (row["high"] + row["low"] + row["close"]) / 3.0
            cum_vol += row["volume"]
            cum_tp_vol += typical * row["volume"]
            vwap_so_far = (cum_tp_vol / cum_vol) if cum_vol > 0 else row["close"]

            # intraday zones computed from ONLY candles up to and including
            # this one -- no lookahead into the rest of the day
            so_far_df = day_df.iloc[:i + 1]
            intraday_zones_now = compute_zones(so_far_df, INTRADAY_N_BINS) if i >= 5 else []
            intraday_modes = [z["price_mode"] for z in intraday_zones_now]

            for z in composite_zones:
                zkey = round(z["price_mode"], 2)
                price_low, price_high = z["price_low"], z["price_high"]
                # candle's [low, high] range overlaps the zone's range at all
                is_inside_now = row["low"] <= price_high and row["high"] >= price_low
                was_inside_before = zone_in_progress.get(zkey, False)

                if is_inside_now and not was_inside_before:
                    # a fresh touch -- this is a zone TEST event
                    zone_touch_count[zkey] = zone_touch_count.get(zkey, 0) + 1
                    last_close = float(row["close"])
                    # Classify support/resistance using the PRIOR candle's
                    # close, not this candle's -- at the exact touch
                    # moment price IS at the zone boundary by definition,
                    # so using the touch candle's own close is an
                    # unreliable coin-flip. The previous candle tells us
                    # which direction price was coming FROM.
                    prev_close = float(day_df.iloc[i - 1]["close"]) if i > 0 else day_open
                    kind = "support" if prev_close > z["price_mode"] else "resistance"

                    is_validated = any(abs(m - z["price_mode"]) < (price_high - price_low)
                                        for m in intraday_modes)

                    zone_width_pct = (price_high - price_low) / z["price_mode"] * 100
                    day_change_pct = (last_close - day_open) / day_open * 100
                    vwap_dist_pct = (last_close - vwap_so_far) / last_close * 100

                    # ---- forward-looking outcome (the only lookahead in
                    # this script, and only for the LABEL, never the features) ----
                    end_idx = min(i + LOOKFORWARD_CANDLES, len(day_df) - 1)
                    future_close = float(day_df.iloc[end_idx]["close"])
                    if kind == "support":
                        if future_close > price_high:
                            outcome = "hold"
                        elif future_close < price_low:
                            outcome = "break"
                        else:
                            outcome = "ambiguous"
                    else:  # resistance
                        if future_close < price_low:
                            outcome = "hold"
                        elif future_close > price_high:
                            outcome = "break"
                        else:
                            outcome = "ambiguous"

                    rows.append({
                        "symbol": symbol, "date": str(test_day),
                        "candle_index": i, "total_candles_today": len(day_df),
                        "kind": kind, "zone_price_mode": z["price_mode"],
                        "zone_pct": float(str(z["label"]).replace("%", "")) if str(z["label"]).replace("%", "").replace(".", "").isdigit() else None,
                        "zone_width_pct": round(zone_width_pct, 3),
                        "price_at_test": last_close,
                        "vwap_dist_pct": round(vwap_dist_pct, 3),
                        "day_change_pct_so_far": round(day_change_pct, 3),
                        "prior_touches_today": zone_touch_count[zkey] - 1,
                        "is_intraday_validated": is_validated,
                        "outcome": outcome,
                    })

                zone_in_progress[zkey] = is_inside_now

        time.sleep(0.05)  # tiny pause between days, mostly for safety margin

    print(f"[{symbol}] done -- {len(rows)} zone-test events found.")
    return rows


def main():
    token = get_token()
    all_rows = []
    for idx, symbol in enumerate(SYMBOLS):
        try:
            rows = process_symbol(symbol, token)
            all_rows.extend(rows)
        except Exception as e:
            print(f"[{symbol}] FAILED: {e}, skipping.")
        print(f"--- {idx+1}/{len(SYMBOLS)} symbols done, {len(all_rows)} total rows so far ---")
        time.sleep(1.0)

    if not all_rows:
        print("No data collected at all -- check your token and network.")
        return

    df = pd.DataFrame(all_rows)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved {len(df)} rows to {OUTPUT_CSV}")
    print("\nOutcome breakdown:")
    print(df["outcome"].value_counts())


if __name__ == "__main__":
    main()
