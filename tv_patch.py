"""
tv_patch.py
===========
tv-scraper CandleStreamer ka patched version.

Problems fixed:
1. numb_candles=5000 → TV sends ~4363 → `4363 >= 5000` never true → 16-packet timeout
2. WebSocket timeout=10s → after 16 packets, next recv times out → crash

Fix strategy:
- PatchedCandleStreamer overrides get_candles()
- Packet loop: break karo jab ANY substantial OHLCV data aa jaye (>= min_bars)
  instead of waiting for exact numb_candles match
- WebSocket timeout 10s → 30s (connect mein patch)
- numb_candles always capped at SAFE_MAX (4000) regardless of what caller passes
"""

import json
import logging
import time
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

SAFE_MAX_BARS = 4000   # Request this many — TV sends up to ~4363 in one packet
MIN_BARS_OK   = 200    # Agar itne bhi aa gaye to success consider karo


def _ohlcv_to_df(ohlcv_list: list[dict]) -> pd.DataFrame | None:
    """CandleStreamer ohlcv list → clean DataFrame."""
    if not ohlcv_list:
        return None
    df = pd.DataFrame(ohlcv_list)
    if "timestamp" not in df.columns or "close" not in df.columns:
        return None
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    df = df.set_index("datetime").sort_index()
    cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
    return df[cols]


def fetch_symbol_patched(symbol: str, cookie: str = "", retries: int = 1) -> pd.DataFrame | None:
    """
    Patched single-symbol fetch:
    - WebSocket timeout = 30s
    - Packet loop breaks when ANY ohlcv data >= MIN_BARS_OK received
    - numb_candles capped at SAFE_MAX_BARS
    - Each call creates fresh WebSocket (avoids stale connection issues)
    """
    try:
        from tv_scraper import CandleStreamer
        from tv_scraper.streaming.auth import get_valid_jwt_token
        from websocket import create_connection, WebSocketConnectionClosedException
        import secrets, string, re
    except ImportError as e:
        logger.error("tv-scraper import failed: %s", e)
        return None

    clean = symbol.replace(".NS", "").replace(".BO", "").upper().strip()

    # Build session IDs (same as BaseStreamer logic)
    def _gen_session(prefix):
        rand = "".join(secrets.choice(string.ascii_lowercase) for _ in range(12))
        return prefix + rand

    def _send(ws, func, args):
        payload = json.dumps({"m": func, "p": args}, separators=(",", ":"))
        ws.send(f"~m~{len(payload)}~m~{payload}")

    WS_URL = "https://data.tradingview.com/socket.io/websocket?from=chart%2F&type=chart"
    HEADERS = {
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "Upgrade",
        "Host": "data.tradingview.com",
        "Origin": "https://www.tradingview.com",
        "Pragma": "no-cache",
        "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
        "Upgrade": "websocket",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
    }
    TV_TIMEFRAMES = {"1d": "1D", "1w": "1W", "1M": "1M"}

    for attempt in range(retries + 1):
        ws = None
        try:
            # Auth token
            jwt = "unauthorized_user_token"
            if cookie:
                try:
                    jwt = get_valid_jwt_token(cookie)
                except Exception:
                    jwt = "unauthorized_user_token"

            # Connect with 30s timeout
            ws = create_connection(
                WS_URL,
                headers=HEADERS,
                timeout=30,
                enable_multithread=True,
            )

            # Sessions
            qs = _gen_session("qs_")
            cs = _gen_session("cs_")
            sym_json = json.dumps({"adjustment": "splits", "symbol": f"NSE:{clean}"}, separators=(",", ":"))

            # Handshake
            _send(ws, "set_auth_token", [jwt])
            _send(ws, "set_locale", ["en", "US"])
            _send(ws, "chart_create_session", [cs, ""])
            _send(ws, "quote_create_session", [qs])
            _send(ws, "quote_hibernate_all", [qs])

            # Subscribe chart — daily, SAFE_MAX_BARS candles
            _send(ws, "resolve_symbol", [cs, "sds_sym_1", f"={sym_json}"])
            _send(ws, "create_series", [cs, "sds_1", "s1", "sds_sym_1", "1D", SAFE_MAX_BARS, ""])

            # Read packets until we get OHLCV data
            ohlcv_data: list[dict] = []
            packet_count = 0
            MAX_PACKETS = 30  # increased from library's 16

            while packet_count < MAX_PACKETS:
                try:
                    raw = ws.recv()
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                except Exception:
                    break

                # Heartbeat
                if re.match(r"~m~\d+~m~~h~\d+$", raw):
                    ws.send(raw)
                    packet_count += 1
                    continue

                # Parse all messages in packet
                parts = [x for x in re.split(r"~m~\d+~m~", raw) if x]
                for part in parts:
                    try:
                        msg = json.loads(part)
                    except Exception:
                        continue

                    if msg.get("m") == "timescale_update":
                        p_data = msg.get("p", [])
                        if len(p_data) >= 2 and isinstance(p_data[1], dict):
                            entries = p_data[1].get("sds_1", {}).get("s", [])
                            parsed = []
                            for e in entries:
                                if "i" not in e or "v" not in e or len(e["v"]) < 5:
                                    continue
                                rec = {
                                    "index": e["i"],
                                    "timestamp": e["v"][0],
                                    "open":  e["v"][1],
                                    "high":  e["v"][2],
                                    "low":   e["v"][3],
                                    "close": e["v"][4],
                                }
                                if len(e["v"]) > 5:
                                    rec["volume"] = e["v"][5]
                                parsed.append(rec)
                            if parsed:
                                ohlcv_data = parsed  # replace with latest full batch

                    elif msg.get("m") == "series_completed":
                        # TV has finished sending all candles
                        break

                packet_count += 1

                # Break if we have enough data and series seems done
                if len(ohlcv_data) >= MIN_BARS_OK:
                    # Wait one more round for series_completed or du packet
                    if packet_count > 5:
                        break

            ws.close()
            ws = None

            if not ohlcv_data:
                if attempt < retries:
                    time.sleep(2.0)
                    continue
                return None

            # Sort by index (chronological)
            ohlcv_data.sort(key=lambda x: x["index"])
            return _ohlcv_to_df(ohlcv_data)

        except Exception as e:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
            if attempt < retries:
                time.sleep(3.0)
            else:
                return None

    return None
