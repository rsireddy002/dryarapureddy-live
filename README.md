# Sahi Key Levels LIVE — assembled in `upstox-live-feed/sahi-key-levels-live/`

This is your "Sahi Key Levels LIVE" system — the reference tool `viewer.py`'s
chart has been compared against throughout this project — brought together
into one self-contained folder inside `upstox-live-feed`, alongside (not
touching) your existing `viewer.py` / `poll_listener.py` / `indicators.py`
pipeline. Nothing in the parent folder was changed.

It's a much bigger system than the single-chart viewer: a 220+-symbol
scanner, cross-timeframe validated support/resistance zones, an ML
break-risk model, simulated paper trading (both REST-polling and
tick-driven versions), BUY/SELL alerts, a trade journal, sector grids, and
a Replay tab.

## What's in this folder

**Your real files, uploaded as-is:**
- `app.py` — the main Streamlit app (14 tabs: Scanner, Key Levels, Chart,
  Sectors, By RVOL, Wide Range, Zone Watch, Candle Close Signals, Setups,
  Paper Trading, Trade Journal, Replay, Alerts, Live Ticks)
- `zone_validation.py` — cross-timeframe zone validation + BUY/SELL signal
- `sahi_style_key_levels.py`, `hvn_lvn.py` — volume-profile zone detection
- `candles_with_levels.py` — the candlestick + zones + CVD chart renderer
- `ml_predict.py` + `zone_break_model.pkl` — the trained break-risk model
  (held-out AUC 0.76, see `build_ml_dataset.py` for training details)
- `feed_listener.py`, `candle_aggregator.py`, `proto_decoder.py`,
  `MarketDataFeedV3_pb2.py` — the WebSocket tick feed → local 5-min candle
  aggregator (replaces ~230 REST calls/refresh with one persistent
  connection)
- `tick_paper_trader.py`, `paper_trader_daemon.py` — simulated paper
  trading: tick-driven entries/exits, plus the standalone daemon that
  keeps zones fresh even when no browser tab is open
- `token_webhook.py`, `trigger_token_request.py` — the semi-automated
  daily token refresh flow (Upstox pushes the token, a webhook picks it
  up and restarts services)
- `tier1_keys.txt`, `tier2_keys.txt`, `requirements.txt`,
  `sahi_zones_cache.json`, `paper_trades.json`, `instrument_keys_cache.json`,
  `fno_universe_cache.json`, `fno_live_candles.json` — data/config as of
  when you uploaded them
- `README_fno_websocket_feed.md` — the original README for the
  feed-listener half of this system

**Written fresh for this assembly (the two files that weren't uploaded):**
- `instrument_resolver.py` — `resolve_all()` / `get_token()` for
  `feed_listener.py`. Reconstructed from `instrument_keys_cache.json`'s
  exact on-disk shape (`{symbol: instrument_key}`, matching your 220
  equities + NIFTY/BANKNIFTY) and the identical resolve-equity /
  resolve-futures logic already duplicated in `app.py` and
  `paper_trader_daemon.py` — just factored out and cached to disk.
- `live_feed_reader.py` — `get_live_candles(symbol)` reading
  `fno_live_candles.json`, written straight from
  `README_fno_websocket_feed.md`'s documented output format. This is a
  **different** file from `upstox-live-feed`'s own `live_feed_reader.py`
  (which reads the SQLite database `poll_listener.py` writes) — this one
  reads the JSON snapshot `feed_listener.py` in *this* folder dumps every
  5 seconds. The two pipelines don't share data or files.

Both of those were plumbing, not the actual zone/signal algorithm, so
reconstructing them carried little risk of diverging from your real
system. `zone_validation.py` — the one file that mattered — is your real
upload.

## Verified

- Every `.py` file in this folder byte-compiles cleanly
  (`python3 -m py_compile`).
- Every module imports cleanly: `app`, `zone_validation`,
  `sahi_style_key_levels`, `hvn_lvn`, `candles_with_levels`, `ml_predict`,
  `feed_listener`, `candle_aggregator`, `proto_decoder`,
  `tick_paper_trader`, `paper_trader_daemon`, `instrument_resolver`,
  `live_feed_reader` — no `ModuleNotFoundError`s anywhere.
- `app.py` runs its module-level setup correctly and stops exactly where
  it should without a real token: `RuntimeError: No token found` — i.e.
  it reached live-data code, not an import error. That's the expected
  behavior with no `UPSTOX_ACCESS_TOKEN` set, same as it'll do on a fresh
  machine before you configure one.

What could **not** be verified in this sandbox: no live Upstox
credentials here, so no actual API calls, WebSocket connection, or
zone/ML computation against real data were run. The logic is exactly
your uploaded code — this only confirms it's structurally complete and
wired together correctly, not that today's zones/signals come out
correctly (that needs a real token + real market data, which only your
machine has).

## Setup (same as your existing production setup)

```
cd upstox-live-feed/sahi-key-levels-live
pip install -r requirements.txt
$env:UPSTOX_ACCESS_TOKEN = "your_token_here"      # PowerShell
# or: export UPSTOX_ACCESS_TOKEN="your_token_here"  # bash

python instrument_resolver.py     # one-time: builds instrument_keys_cache.json
                                   # (already present from your upload, so
                                   #  this is a no-op unless you delete it)
python feed_listener.py           # WebSocket tick feed -> fno_live_candles.json
python paper_trader_daemon.py     # Precompute + Zone Refresh (independent of feed_listener.py)
streamlit run app.py              # the main dashboard
```

`token_webhook.py` / `trigger_token_request.py` are optional — only
needed if you're running the semi-automated daily token refresh (they
reference your EC2 paths/systemd service names, e.g.
`/home/ubuntu/dryarapureddy-tick-ML/upstox_token.env` and services
`feed-listener` / `paper-trader-daemon` / `scanner-app` — adjust those if
this folder's deployment path differs from where you were previously
running it).

## Relationship to `viewer.py`

Completely independent. `viewer.py`'s pipeline
(`poll_listener.py` → SQLite → `viewer.py`) is untouched by anything in
this folder. This app has its own tick feed (`feed_listener.py` →
`fno_live_candles.json`), its own instrument cache, its own zone cache.
Running both at once is fine — they don't share files or ports.
