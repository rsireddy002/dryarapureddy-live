"""
proto_decoder.py - Decodes Upstox V3 WebSocket protobuf messages into
{instrument_key: {"ltp":, "ltt":, "volume":}} dicts.

Built directly from your actual MarketDataFeedV3.proto schema and
MarketDataFeedV3_pb2.py (both copied into this repo unchanged), plus the
exact decoded-dict shape your own test_footprint.py already exercises
(via ParseFromString + google.protobuf.json_format.MessageToDict) --
not a re-guessed schema.

Handles all three Feed variants defined in the proto:
  - plain ltpc            (RequestMode.ltpc)
  - fullFeed.marketFF     (equities/futures, full_d5 / full_d30)
  - fullFeed.indexFF      (indices -- no vtt/volume field, by design)
  - firstLevelWithGreeks  (options, if you ever subscribe that mode)

int64 fields (ltt, vtt) come through MessageToDict as STRINGS, not ints --
this is a known protobuf JSON-mapping quirk, already identified and
handled elsewhere in your codebase (live_strategy.py's HVN/LVN patch
notes this explicitly). Cast back to int/float here, same fix.
"""
import MarketDataFeedV3_pb2 as pb
from google.protobuf.json_format import MessageToDict


def decode_feed_message(raw_bytes: bytes) -> dict:
    """
    Returns {instrument_key: {"ltp": float, "ltt": int_or_None, "volume": float_or_None}, ...}
    for every instrument present in this message.
    """
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(raw_bytes)
    data = MessageToDict(feed_response)

    updates = {}
    for instrument_key, feed in data.get("feeds", {}).items():
        ltp, ltt, volume = None, None, None

        if "ltpc" in feed:
            ltpc = feed["ltpc"]
            ltp = ltpc.get("ltp")
            ltt = ltpc.get("ltt")

        elif "fullFeed" in feed:
            full_feed = feed["fullFeed"]
            if "marketFF" in full_feed:
                mff = full_feed["marketFF"]
                ltpc = mff.get("ltpc", {})
                ltp = ltpc.get("ltp")
                ltt = ltpc.get("ltt")
                volume = mff.get("vtt")  # cumulative day volume, string per int64 quirk above
            elif "indexFF" in full_feed:
                iff = full_feed["indexFF"]
                ltpc = iff.get("ltpc", {})
                ltp = ltpc.get("ltp")
                ltt = ltpc.get("ltt")
                # indices have no vtt/volume field at all -- stays None,
                # candle_aggregator.py handles None volume fine (bar
                # volume just stays 0 for index bars, which is correct
                # since indices don't have real traded volume anyway).

        elif "firstLevelWithGreeks" in feed:
            flg = feed["firstLevelWithGreeks"]
            ltpc = flg.get("ltpc", {})
            ltp = ltpc.get("ltp")
            ltt = ltpc.get("ltt")
            volume = flg.get("vtt")

        if ltp is None:
            continue

        updates[instrument_key] = {
            "ltp": float(ltp),
            "ltt": int(ltt) if ltt else None,
            "volume": float(volume) if volume is not None else None,
        }

    return updates
