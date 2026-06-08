"""
cache_builder_tradingview.py
============================
TradingView se full history cache build karta hai.
GitHub Actions pe daily chalta hai.

tv-scraper library use karta hai lekin CandleStreamer ka
broken 16-packet timeout BYPASS karta hai — direct WebSocket
via tv_patch.py ka fetch_symbol_patched().

── 5-DAY ROLLING CACHE ──────────────────────────────────────
  cache_tradingview/
    cache_index.json         ← {"dates": [...], "latest": "YYYY-MM-DD"}
    2026-06-08/
      close.parquet, high.parquet, volume.parquet, ath.parquet, cache_meta.json
    (max 5 dirs — 6th build pe oldest auto-pruned)
──────────────────────────────────────────────────────────────

Key design:
  • tv_patch.fetch_symbol_patched() — NSE daily, 4000 bars (~16 years)
  • ATH = all bars ka high.max() → correct lifetime ATH
  • Recent 40M = slice from full data
  • Rate limit: 0.3s sleep between symbols
  • Auth: TV_COOKIE env var (optional — anonymous mode works)

GitHub Secrets:
  PAT_TOKEN  (required — repo write access)
  TV_COOKIE  (optional — better data, captcha bypass)
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

# tv_patch must be in same directory
try:
    from tv_patch import fetch_symbol_patched
except ImportError:
    print("ERROR: tv_patch.py not found in same directory as cache_builder_tradingview.py", flush=True)
    sys.exit(1)

from cache_rolling import save_rolling_cache, MAX_CACHED_DAYS

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
CACHE_DIR      = Path("cache_tradingview")
RECENT_MONTHS  = 40
EXTRA_SYMBOLS  = ["GOLDBEES", "SILVERBEES"]
GITHUB_BASE    = "https://raw.githubusercontent.com/prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main"

RATE_LIMIT_SLEEP = 0.3   # seconds between symbols
MAX_RETRIES      = 1     # fast fail per symbol


# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ══════════════════════════════════════════════════════════════
# SYMBOL LIST
# ══════════════════════════════════════════════════════════════
def load_symbols() -> list[str]:
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
# MAIN BUILDER
# ══════════════════════════════════════════════════════════════
def build_cache():
    today_str = date.today().strftime("%Y-%m-%d")
    today_dt  = datetime.today()
    cutoff    = today_dt - relativedelta(months=RECENT_MONTHS)

    tv_cookie = os.environ.get("TV_COOKIE", "").strip()

    log("=" * 60)
    log(f"TradingView Cache Builder — {today_str}")
    log(f"Recent slice: last {RECENT_MONTHS} months (from {cutoff.strftime('%Y-%m-%d')})")
    log(f"Auth: {'cookie mode' if tv_cookie else 'anonymous mode'}")
    log("=" * 60)

    symbols = load_symbols()
    total   = len(symbols)

    close_all, high_all, vol_all = {}, {}, {}
    ath_dict  = {}
    failed    = []
    ok_count  = 0

    for i, sym in enumerate(symbols):
        df = fetch_symbol_patched(sym, cookie=tv_cookie, retries=MAX_RETRIES)

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

    close_df = pd.DataFrame(close_all).sort_index()
    high_df  = pd.DataFrame(high_all).sort_index()
    vol_df   = pd.DataFrame(vol_all).sort_index()

    if close_df.empty:
        log("ERROR: No data fetched. Aborting.")
        sys.exit(1)

    ath_df = pd.DataFrame({"ATH": ath_dict})

    meta = {
        "build_date":      today_str,
        "build_timestamp": datetime.now().isoformat(),
        "source":          "TradingView (tv-scraper patched WebSocket)",
        "n_bars":          4000,
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
        cache_dir = CACHE_DIR,
        today_str = today_str,
        close     = close_df,
        high      = high_df,
        volume    = vol_df,
        ath_df    = ath_df,
        meta      = meta,
    )

    log(f"\n✅ Cache saved to {CACHE_DIR}/{today_str}/")
    log(f"   close.parquet  : {close_df.shape}")
    log(f"   high.parquet   : {high_df.shape}")
    log(f"   volume.parquet : {vol_df.shape}")
    log(f"   ath.parquet    : {ath_df.shape}")
    log("=" * 60)


if __name__ == "__main__":
    build_cache()
