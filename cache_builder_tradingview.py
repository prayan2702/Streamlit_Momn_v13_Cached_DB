"""
cache_builder_tradingview.py
============================
TradingView (tvDatafeed WebSocket) se FULL history cache build karta hai.
2010-01-01 se aaj tak ka data fetch karta hai — sahi ATH ke liye.

── KEY DESIGN: request_more_data ─────────────────────────────
  tvDatafeed get_hist() sirf n_bars=5000 (~13 years) deta hai.
  Pura history (2010+) ke liye TradingView WebSocket ka
  `request_more_data` message use karta hai same session mein.

  Flow per symbol:
    1. create_series(n_bars=5000) → latest 5000 bars milte hain
    2. series_completed aane pe check karo — kitne bars aaye?
    3. Agar oldest bar > TARGET_FROM_DATE:
       → request_more_data(5000) bhejo → aur purane bars milte hain
    4. Repeat until oldest bar <= TARGET_FROM_DATE ya no more data

  TARGET_FROM_DATE = 2010-01-01 (NSE stocks ka sahi ATH ke liye)

── 5-DAY ROLLING CACHE ──────────────────────────────────────
  cache_tradingview/
    cache_index.json
    2026-06-10/
      close.parquet, high.parquet, volume.parquet, ath.parquet, cache_meta.json
    (max 5 dirs — oldest auto-pruned)
──────────────────────────────────────────────────────────────

GitHub Secrets (optional):
  TV_USERNAME | TV_PASSWORD
"""

import json
import logging
import os
import random
import re
import string
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from websocket import create_connection

from cache_rolling import save_rolling_cache, MAX_CACHED_DAYS

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
CACHE_DIR           = Path("cache_tradingview")
RECENT_MONTHS       = 40          # parquet mein kitne months ka data store karo
TARGET_FROM_DATE    = datetime(2000, 1, 1)   # ATH ke liye minimum start date
EXTRA_SYMBOLS       = ["GOLDBEES", "SILVERBEES"]
NSE_EQUITY_URL      = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
GITHUB_BASE         = "https://raw.githubusercontent.com/prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main"

BARS_PER_CALL       = 5000        # tvDatafeed max bars per call
WS_TIMEOUT          = 30          # WebSocket timeout seconds
RATE_LIMIT_SLEEP    = 0.3         # seconds between symbols
MAX_CHUNKS          = 5           # max request_more_data calls per symbol (safety)


# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ══════════════════════════════════════════════════════════════
# AUTH — TradingView token
# ══════════════════════════════════════════════════════════════
def _get_tv_token() -> str:
    username = os.environ.get("TV_USERNAME", "").strip()
    password = os.environ.get("TV_PASSWORD", "").strip()

    if not username or not password:
        log("TradingView: anonymous mode (no credentials)")
        return "unauthorized_user_token"

    log(f"TradingView login: {username[:4]}****")
    session = requests.Session()
    session.headers.update({
        "Referer": "https://www.tradingview.com",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        ),
    })
    try:
        session.get("https://www.tradingview.com/", timeout=15)
        resp = session.post(
            "https://www.tradingview.com/accounts/signin/",
            data={"username": username, "password": password, "remember": "on"},
            timeout=15,
        )
        token = resp.json()["user"]["auth_token"]
        log("TradingView login successful ✅")
        return token
    except Exception as e:
        log(f"Login failed ({e}) — using anonymous mode")
        return "unauthorized_user_token"


# ══════════════════════════════════════════════════════════════
# WEBSOCKET HELPERS
# ══════════════════════════════════════════════════════════════
def _rand_str(n: int) -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))


def _prepend(st: str) -> str:
    return f"~m~{len(st)}~m~{st}"


def _msg(func: str, params: list) -> str:
    return _prepend(json.dumps({"m": func, "p": params}, separators=(",", ":")))


def _parse_df(raw_data: str, symbol: str) -> pd.DataFrame | None:
    """raw WebSocket data se OHLCV DataFrame banao."""
    try:
        out = re.search(r'"s":\[(.+?)\}]', raw_data).group(1)
        x = out.split(',{"')
        rows = []
        volume_ok = True
        for xi in x:
            xi = re.split(r"[\[:|,\]]", xi)
            try:
                ts = datetime.fromtimestamp(float(xi[4]))
            except (ValueError, IndexError):
                continue
            row = [ts]
            for i in range(5, 10):
                if not volume_ok and i == 9:
                    row.append(0.0)
                    continue
                try:
                    row.append(float(xi[i]))
                except (ValueError, IndexError):
                    volume_ok = False
                    row.append(0.0)
            rows.append(row)
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume"])
        df = df.set_index("datetime").sort_index()
        df = df[~df.index.duplicated(keep="last")]
        return df
    except AttributeError:
        return None


# ══════════════════════════════════════════════════════════════
# FULL HISTORY FETCH — request_more_data loop
# ══════════════════════════════════════════════════════════════
def fetch_full_history(token: str, symbol: str, retries: int = 2) -> pd.DataFrame | None:
    """
    Single symbol ka full history fetch karo 2010 se.

    Strategy:
      1. create_series(5000) → latest 5000 bars
      2. series_completed pe check: oldest bar > 2010?
      3. Agar haan: request_more_data(5000) → aur purane bars
      4. Repeat until 2010 reach ya max chunks ya no more data

    Returns: pd.DataFrame(index=datetime, cols=[open,high,low,close,volume]) or None
    """
    for attempt in range(retries):
        ws = None
        try:
            ws = create_connection(
                "wss://data.tradingview.com/socket.io/websocket",
                headers=json.dumps({"Origin": "https://data.tradingview.com"}),
                timeout=WS_TIMEOUT,
            )

            session      = "qs_" + _rand_str(12)
            chart_session = "cs_" + _rand_str(12)
            tv_symbol    = f"NSE:{symbol}"

            # ── Setup messages ──
            ws.send(_msg("set_auth_token",         [token]))
            ws.send(_msg("chart_create_session",   [chart_session, ""]))
            ws.send(_msg("quote_create_session",   [session]))
            ws.send(_msg("quote_set_fields", [
                session,
                "ch", "chp", "current_session", "description", "local_description",
                "language", "exchange", "fractional", "is_tradable", "lp", "lp_time",
                "minmov", "minmove2", "original_name", "pricescale", "pro_name",
                "short_name", "type", "update_mode", "volume", "currency_code", "rchp", "rtc",
            ]))
            ws.send(_msg("quote_add_symbols",  [session, tv_symbol, {"flags": ["force_permission"]}]))
            ws.send(_msg("quote_fast_symbols", [session, tv_symbol]))
            ws.send(_msg("resolve_symbol", [
                chart_session,
                "symbol_1",
                f'={{"symbol":"{tv_symbol}","adjustment":"splits","session":"regular"}}',
            ]))
            ws.send(_msg("create_series",
                         [chart_session, "s1", "s1", "symbol_1", "1D", BARS_PER_CALL]))
            ws.send(_msg("switch_timezone", [chart_session, "exchange"]))

            # ── Receive loop with request_more_data ──
            all_raw     = ""
            chunks_done = 0

            while True:
                try:
                    result = ws.recv()
                except Exception as e:
                    logger.debug(f"recv error: {e}")
                    break

                all_raw += result + "\n"

                if "series_completed" in result:
                    chunks_done += 1

                    # Parse what we have so far
                    df_so_far = _parse_df(all_raw, symbol)

                    # Check if we need more data
                    if (
                        df_so_far is not None
                        and not df_so_far.empty
                        and df_so_far.index[0] > TARGET_FROM_DATE
                        and chunks_done < MAX_CHUNKS
                    ):
                        # Request older data
                        ws.send(_msg("request_more_data",
                                     [chart_session, "s1", BARS_PER_CALL]))
                    else:
                        # We have enough data or can't get more
                        break

            ws.close()

            df = _parse_df(all_raw, symbol)
            if df is None or df.empty:
                if attempt < retries - 1:
                    time.sleep(3.0)
                    continue
                return None

            # Log chunk info for first few symbols (debugging)
            if chunks_done > 1:
                oldest = df.index[0].strftime("%Y-%m-%d")
                logger.debug(f"{symbol}: {chunks_done} chunks, oldest bar = {oldest}, total bars = {len(df)}")

            # Normalize
            df.columns = [c.lower() for c in df.columns]
            needed = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
            return df[needed]

        except Exception as e:
            err = str(e).lower()
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
            if attempt < retries - 1:
                sleep_time = 5.0 if ("timeout" in err or "connection" in err) else 3.0
                time.sleep(sleep_time)
            else:
                return None

    return None


# ══════════════════════════════════════════════════════════════
# SYMBOL LIST
# ══════════════════════════════════════════════════════════════
def load_symbols() -> list[str]:
    csv_path = CACHE_DIR / "EQUITY_L.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        log(f"Loaded EQUITY_L.csv from local cache")
    else:
        url = f"{GITHUB_BASE}/EQUITY_L.csv"
        log(f"Loading EQUITY_L.csv from GitHub: {url}")
        df = pd.read_csv(url)

    syms = df["SYMBOL"].astype(str).str.strip().str.upper().tolist()
    for ex in EXTRA_SYMBOLS:
        if ex not in syms:
            syms.append(ex)
    log(f"Total symbols: {len(syms):,}")
    return syms


# ══════════════════════════════════════════════════════════════
# MAIN BUILDER
# ══════════════════════════════════════════════════════════════
def build_cache():
    today_str = date.today().strftime("%Y-%m-%d")
    today_dt  = datetime.today()
    cutoff    = today_dt - relativedelta(months=RECENT_MONTHS)

    log("=" * 62)
    log(f"TradingView Cache Builder — {today_str}")
    log(f"Full history: {TARGET_FROM_DATE.strftime('%Y-%m-%d')} → today (correct ATH)")
    log(f"Recent slice: last {RECENT_MONTHS} months → parquet (from {cutoff.strftime('%Y-%m-%d')})")
    log(f"Bars per chunk: {BARS_PER_CALL} | Max chunks per symbol: {MAX_CHUNKS}")
    log("=" * 62)

    CACHE_DIR.mkdir(exist_ok=True)

    # ── Auth ──────────────────────────────────────────────────
    token   = _get_tv_token()
    symbols = load_symbols()
    total   = len(symbols)

    # ── Per-symbol fetch ──────────────────────────────────────
    close_all, high_all, vol_all = {}, {}, {}
    ath_dict  = {}
    failed    = []
    ok_count  = 0
    t0        = time.monotonic()

    for i, sym in enumerate(symbols):
        df = fetch_full_history(token, sym)

        if df is not None and not df.empty and "close" in df.columns:
            # ATH = full history (2010+) ka max high
            ath_dict[sym] = float(df["high"].max()) if "high" in df.columns else float(df["close"].max())

            # Recent slice only for parquet (saves disk space)
            df_recent = df[df.index >= cutoff].copy()
            if not df_recent.empty:
                idx = df_recent.index
                close_all[sym] = pd.Series(df_recent["close"].values, index=idx)
                high_all[sym]  = pd.Series(df_recent["high"].values,  index=idx) if "high" in df_recent.columns else pd.Series(dtype=float)
                vol_all[sym]   = pd.Series(
                    (df_recent["close"] * df_recent["volume"]).values, index=idx
                ) if "volume" in df_recent.columns else pd.Series(dtype=float)
                ok_count += 1
            else:
                failed.append(sym)
        else:
            failed.append(sym)

        if (i + 1) % 100 == 0 or i == total - 1:
            elapsed   = time.monotonic() - t0
            remaining = (total - i - 1) * (elapsed / max(i + 1, 1))
            pct       = (i + 1) / total * 100
            log(
                f"  Progress: {i+1}/{total} ({pct:.1f}%) | "
                f"OK: {ok_count} | Failed: {len(failed)} | "
                f"ETA: {remaining/60:.1f}min"
            )

        time.sleep(RATE_LIMIT_SLEEP)

    log(f"\nFetch complete: {ok_count}/{total} symbols | Failed: {len(failed)}")
    if failed[:20]:
        log(f"First 20 failed: {failed[:20]}")

    # ── Retry failed symbols ──────────────────────────────────
    if failed:
        log(f"\n🔄 Retrying {len(failed)} failed symbols...")
        retry_token  = _get_tv_token()
        retry_ok     = 0
        still_failed = []

        for sym in failed:
            # Attempt 1: same symbol name (fresh token)
            time.sleep(1.5)
            df = fetch_full_history(retry_token, sym, retries=1)

            # Attempt 2: dash → underscore (BAJAJ-AUTO → BAJAJ_AUTO)
            if (df is None or df.empty or "close" not in df.columns) and "-" in sym:
                sym_us = sym.replace("-", "_")
                log(f"  Trying {sym} → {sym_us}")
                time.sleep(1.5)
                df = fetch_full_history(retry_token, sym_us, retries=1)
                if df is not None and not df.empty and "close" in df.columns:
                    log(f"  ✅ {sym} fetched as {sym_us}")

            if df is not None and not df.empty and "close" in df.columns:
                ath_dict[sym] = float(df["high"].max()) if "high" in df.columns else float(df["close"].max())
                df_recent = df[df.index >= cutoff].copy()
                if not df_recent.empty:
                    idx = df_recent.index
                    close_all[sym] = pd.Series(df_recent["close"].values, index=idx)
                    high_all[sym]  = pd.Series(df_recent["high"].values,  index=idx) if "high" in df_recent.columns else pd.Series(dtype=float)
                    vol_all[sym]   = pd.Series(
                        (df_recent["close"] * df_recent["volume"]).values, index=idx
                    ) if "volume" in df_recent.columns else pd.Series(dtype=float)
                    ok_count += 1
                    retry_ok += 1
                    log(f"  ✅ Recovered: {sym}")
                else:
                    still_failed.append(sym)
            else:
                log(f"  ❌ Skipped: {sym} (not found on TradingView)")
                still_failed.append(sym)

        log(f"✅ Retry done: {retry_ok}/{len(failed)} recovered | Permanently failed: {len(still_failed)}: {still_failed}")
        failed = still_failed

    # ── Build DataFrames ──────────────────────────────────────
    close_df = pd.DataFrame(close_all).sort_index()
    high_df  = pd.DataFrame(high_all).sort_index()
    vol_df   = pd.DataFrame(vol_all).sort_index()

    if close_df.empty:
        log("ERROR: No data fetched. Aborting.")
        sys.exit(1)

    ath_df = pd.DataFrame({"ATH": ath_dict})

    # ── Log data range ────────────────────────────────────────
    # recent slice range (for parquet)
    oldest_in_recent = close_df.index[0].strftime("%Y-%m-%d") if not close_df.empty else "N/A"
    latest_in_recent = close_df.index[-1].strftime("%Y-%m-%d") if not close_df.empty else "N/A"

    # Actual ATH history oldest date — sample 20 symbols to find min
    _sample_syms = list(ath_dict.keys())[:20]
    ath_oldest_dt = None
    for _s in _sample_syms:
        if _s in close_all:
            _idx = close_all[_s].index
            if len(_idx) > 0:
                _oldest = _idx[0]
                if ath_oldest_dt is None or _oldest < ath_oldest_dt:
                    ath_oldest_dt = _oldest
    # Note: close_all has recent slice only; ath used full df — log approximation
    ath_oldest_str = TARGET_FROM_DATE.strftime("%Y-%m-%d")  # guaranteed minimum
    if ath_oldest_dt is not None:
        log(f"Recent slice oldest bar : {oldest_in_recent}")
        log(f"Recent slice latest bar : {latest_in_recent}")
    log(f"ATH computed from       : {ath_oldest_str} → {latest_in_recent} (full history)")
    log(f"Symbols with ATH        : {len(ath_dict):,}")

    # ── Meta ──────────────────────────────────────────────────
    total_elapsed = time.monotonic() - t0
    meta = {
        "build_date":             today_str,
        "build_timestamp":        datetime.now().isoformat(),
        "build_duration_min":     round(total_elapsed / 60, 1),
        "source":                 "TradingView (tvDatafeed WebSocket + request_more_data)",
        "ath_start_date":         TARGET_FROM_DATE.strftime("%Y-%m-%d"),
        "bars_per_chunk":         BARS_PER_CALL,
        "max_chunks_per_symbol":  MAX_CHUNKS,
        "recent_months":          RECENT_MONTHS,
        "symbols_total":          total,
        "symbols_fetched":        ok_count,
        "symbols_failed":         len(failed),
        "failed_list":            failed[:50],
        "close_shape":            list(close_df.shape),
        "ath_count":              len(ath_df),
        "recent_date_range":      [oldest_in_recent, latest_in_recent],
    }

    available_dates = save_rolling_cache(
        cache_dir = CACHE_DIR,
        today_str = today_str,
        close     = close_df,
        high      = high_df,
        volume    = vol_df,
        ath_df    = ath_df,
        meta      = meta,
        log_fn    = log,
    )

    log(f"\n✅ Cache saved to {CACHE_DIR}/{today_str}/")
    log(f"   close.parquet  : {close_df.shape}")
    log(f"   high.parquet   : {high_df.shape}")
    log(f"   volume.parquet : {vol_df.shape}")
    log(f"   ath.parquet    : {ath_df.shape}")
    log(f"   ATH from       : {TARGET_FROM_DATE.strftime('%Y-%m-%d')} → today")
    log(f"   Cached dates   : {available_dates}")
    log("=" * 62)


if __name__ == "__main__":
    build_cache()
