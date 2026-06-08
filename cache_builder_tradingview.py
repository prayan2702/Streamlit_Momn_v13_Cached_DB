"""
cache_builder_tradingview.py
============================
TradingView (tv-scraper CandleStreamer) se full history cache build karta hai.
GitHub Actions pe daily chalta hai.

tvDatafeed → tv-scraper migration:
  - tvDatafeed ka login issue aur nil data problem fix
  - CandleStreamer WebSocket-based fetch — no login required (anonymous works)
  - Optional: TV_COOKIE secret set karo GitHub Secrets mein (better access)

── 5-DAY ROLLING CACHE ──────────────────────────────────────
  cache_tradingview/
    cache_index.json         ← {"dates": [...], "latest": "YYYY-MM-DD"}
    2026-05-26/
      close.parquet, high.parquet, volume.parquet, ath.parquet, cache_meta.json
    (max 5 dirs — 6th build pe oldest auto-pruned)
──────────────────────────────────────────────────────────────

Key design:
  • CandleStreamer.get_candles() — NSE daily, numb_candles=5000 (~13+ years)
  • ATH = all 5000 bars ka high.max() → correct lifetime ATH
  • Recent 40M = slice from full data
  • Rate limit: 1.0s sleep between symbols (WebSocket reconnect overhead)
  • Auth: TV_COOKIE GitHub Secret (optional — anonymous mode bhi kaam karta hai)

GitHub Secrets:
  PAT_TOKEN    (required — repo write access)
  TV_COOKIE    (optional — better data, captcha bypass)
"""

import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

try:
    from tv_scraper import CandleStreamer
except ImportError:
    print("ERROR: tv-scraper not installed. pip install git+https://github.com/smitkunpara/tv-scraper.git", flush=True)
    sys.exit(1)

from cache_rolling import save_rolling_cache, MAX_CACHED_DAYS

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
CACHE_DIR      = Path("cache_tradingview")
RECENT_MONTHS  = 40
EXTRA_SYMBOLS  = ["GOLDBEES", "SILVERBEES"]
NSE_EQUITY_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
GITHUB_BASE    = "https://raw.githubusercontent.com/prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main"

TV_MAX_BARS         = 5000   # CandleStreamer max candles per call
RATE_LIMIT_SLEEP    = 1.0    # seconds between symbols (WebSocket reconnect)
MAX_RETRIES         = 2


# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ══════════════════════════════════════════════════════════════
# AUTH — CandleStreamer client
# tv-scraper anonymous mode = no login needed
# Optional: TV_COOKIE env var set karo better access ke liye
# ══════════════════════════════════════════════════════════════
def get_streamer() -> CandleStreamer:
    cookie = os.environ.get("TV_COOKIE", "").strip()
    if cookie:
        log("TradingView: cookie-based auth mode")
        return CandleStreamer(cookie=cookie)
    log("TradingView: anonymous mode (no TV_COOKIE found — works for NSE data)")
    return CandleStreamer()


# ══════════════════════════════════════════════════════════════
# SYMBOL LIST
# ══════════════════════════════════════════════════════════════
def load_symbols() -> list[str]:
    """
    EQUITY_L.csv se NSE symbols load karo.
    tv-scraper format: plain symbol (no .NS suffix, no -EQ).
    """
    csv_path = CACHE_DIR / "EQUITY_L.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
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
# SINGLE SYMBOL FETCH via CandleStreamer
# ══════════════════════════════════════════════════════════════
def fetch_symbol(streamer: CandleStreamer, symbol: str, retries: int = MAX_RETRIES) -> pd.DataFrame | None:
    """
    Single NSE symbol ka full history fetch (upto TV_MAX_BARS daily bars).

    tv-scraper CandleStreamer.get_candles() returns:
      {
        "status": "success",
        "data": {
          "ohlcv": [
            {"index":0, "timestamp":1700000000, "open":..., "high":..., "low":..., "close":..., "volume":...},
            ...
          ]
        }
      }

    Returns DataFrame(index=datetime, cols=[open,high,low,close,volume]) or None.
    """
    delay = 2.0
    for attempt in range(retries + 1):
        try:
            result = streamer.get_candles(
                exchange="NSE",
                symbol=symbol,
                timeframe="1d",
                numb_candles=TV_MAX_BARS,
            )

            if result.get("status") != "success":
                err = result.get("error", "Unknown error")
                if attempt < retries:
                    time.sleep(delay)
                    delay *= 2
                    continue
                return None

            ohlcv_list = result.get("data", {}).get("ohlcv", [])
            if not ohlcv_list:
                return None

            # Convert list of dicts → DataFrame
            df = pd.DataFrame(ohlcv_list)

            # timestamp → datetime index
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            df = df.set_index("datetime").sort_index()

            # Keep only OHLCV columns
            cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
            if "close" not in cols:
                return None

            return df[cols]

        except Exception as e:
            err_str = str(e).lower()
            if attempt < retries:
                wait = delay * 2 if ("rate" in err_str or "limit" in err_str or "429" in err_str) else delay
                log(f"  Retry {attempt+1}/{retries} for {symbol}: {str(e)[:60]} — waiting {wait:.1f}s")
                time.sleep(wait)
                delay *= 2
            else:
                return None

    return None


# ══════════════════════════════════════════════════════════════
# MAIN BUILDER
# ══════════════════════════════════════════════════════════════
def build_cache():
    today_str  = date.today().strftime("%Y-%m-%d")
    today_dt   = datetime.today()
    cutoff     = today_dt - relativedelta(months=RECENT_MONTHS)

    log("=" * 60)
    log(f"TradingView Cache Builder (tv-scraper) — {today_str}")
    log(f"Recent slice: last {RECENT_MONTHS} months (from {cutoff.strftime('%Y-%m-%d')})")
    log(f"Max bars per symbol: {TV_MAX_BARS}")
    log("=" * 60)

    # ── Streamer init ─────────────────────────────────────────
    streamer = get_streamer()
    symbols  = load_symbols()
    total    = len(symbols)

    # ── Per-symbol fetch ──────────────────────────────────────
    close_all, high_all, vol_all = {}, {}, {}
    ath_dict   = {}
    failed     = []
    ok_count   = 0

    for i, sym in enumerate(symbols):
        df = fetch_symbol(streamer, sym)

        if df is not None and not df.empty and "close" in df.columns:
            # ATH = entire history ka max high
            ath_dict[sym] = float(df["high"].max()) if "high" in df.columns else float(df["close"].max())

            # Recent slice for parquet
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
            pct = (i + 1) / total * 100
            log(f"  Progress: {i+1}/{total} ({pct:.1f}%) | OK: {ok_count} | Failed: {len(failed)}")

        time.sleep(RATE_LIMIT_SLEEP)

    log(f"\nFetch complete: {ok_count}/{total} symbols | Failed: {len(failed)}")
    if failed[:20]:
        log(f"First 20 failed: {failed[:20]}")

    # ── Build DataFrames ──────────────────────────────────────
    close_df  = pd.DataFrame(close_all).sort_index()
    high_df   = pd.DataFrame(high_all).sort_index()
    vol_df    = pd.DataFrame(vol_all).sort_index()

    if close_df.empty:
        log("ERROR: No data fetched. Aborting.")
        sys.exit(1)

    # ── ATH DataFrame ─────────────────────────────────────────
    ath_df = pd.DataFrame({"ATH": ath_dict})

    # ── Save via rolling cache ────────────────────────────────
    meta = {
        "build_date":      today_str,
        "build_timestamp": datetime.now().isoformat(),
        "source":          "TradingView (tv-scraper CandleStreamer)",
        "n_bars":          TV_MAX_BARS,
        "recent_months":   RECENT_MONTHS,
        "symbols_total":   total,
        "symbols_fetched": ok_count,
        "symbols_failed":  len(failed),
        "failed_list":     failed[:50],
        "close_shape":     list(close_df.shape),
        "date_range":      [
            close_df.index[0].strftime("%Y-%m-%d"),
            close_df.index[-1].strftime("%Y-%m-%d"),
        ] if not close_df.empty else [],
    }

    save_rolling_cache(
        cache_dir  = CACHE_DIR,
        today_str  = today_str,
        close      = close_df,
        high       = high_df,
        volume     = vol_df,
        ath_df     = ath_df,
        meta       = meta,
    )

    log(f"\n✅ Cache saved to {CACHE_DIR}/{today_str}/")
    log(f"   close.parquet  : {close_df.shape}")
    log(f"   high.parquet   : {high_df.shape}")
    log(f"   volume.parquet : {vol_df.shape}")
    log(f"   ath.parquet    : {ath_df.shape}")
    log("=" * 60)


if __name__ == "__main__":
    build_cache()
