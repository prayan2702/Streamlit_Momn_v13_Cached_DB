"""
cache_builder_tradingview.py
============================
TradingView (tvDatafeed) se full history cache build karta hai.
GitHub Actions pe daily chalta hai.

── 5-DAY ROLLING CACHE ──────────────────────────────────────
  cache_tradingview/
    cache_index.json         ← {"dates": [...], "latest": "YYYY-MM-DD"}
    2026-05-26/
      close.parquet, high.parquet, volume.parquet, ath.parquet, cache_meta.json
    (max 5 dirs — 6th build pe oldest auto-pruned)
──────────────────────────────────────────────────────────────

Key design:
  • tvDatafeed get_hist() — NSE daily, n_bars=5000 (~13+ years)
  • ATH = all 5000 bars ka high.max() → correct lifetime ATH
  • Recent 40M = slice from full data
  • Rate limit: 0.2s sleep between symbols
  • Login: TV_USERNAME + TV_PASSWORD GitHub Secrets (optional)

GitHub Secrets required (optional — for better data):
  TV_USERNAME | TV_PASSWORD
"""

import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

try:
    from tvDatafeed import TvDatafeed, Interval
except ImportError:
    print("ERROR: tvDatafeed not installed. pip install tvDatafeed", flush=True)
    sys.exit(1)

from cache_rolling import save_rolling_cache, MAX_CACHED_DAYS

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
CACHE_DIR      = Path("cache_tradingview")
RECENT_MONTHS  = 40
EXTRA_SYMBOLS  = ["GOLDBEES", "SILVERBEES"]   # tvDatafeed uses plain NSE symbols
NSE_EQUITY_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
GITHUB_BASE    = "https://raw.githubusercontent.com/prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main"

TV_MAX_BARS         = 5000   # tvDatafeed maximum bars per symbol
RATE_LIMIT_SLEEP    = 0.2    # seconds between symbols


# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ══════════════════════════════════════════════════════════════
# AUTH — TradingView client
# ══════════════════════════════════════════════════════════════
def get_tv_client() -> TvDatafeed:
    username = os.environ.get("TV_USERNAME", "").strip()
    password = os.environ.get("TV_PASSWORD", "").strip()
    if username and password:
        log(f"TradingView login: {username[:4]}****")
        return TvDatafeed(username=username, password=password)
    log("TradingView: anonymous mode (no credentials found)")
    return TvDatafeed()


# ══════════════════════════════════════════════════════════════
# SYMBOL LIST
# ══════════════════════════════════════════════════════════════
def load_symbols() -> list[str]:
    """
    EQUITY_L.csv se NSE symbols load karo.
    tvDatafeed format: plain symbol (no .NS suffix, no -EQ).
    """
    csv_path = CACHE_DIR / "EQUITY_L.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        # GitHub se fallback
        url = f"{GITHUB_BASE}/EQUITY_L.csv"
        log(f"Loading EQUITY_L.csv from GitHub: {url}")
        df = pd.read_csv(url)

    # EQUITY_L.csv column is 'SYMBOL'
    syms = df["SYMBOL"].astype(str).str.strip().str.upper().tolist()
    # Add extra
    for ex in EXTRA_SYMBOLS:
        if ex not in syms:
            syms.append(ex)
    log(f"Total symbols: {len(syms):,}")
    return syms


# ══════════════════════════════════════════════════════════════
# SINGLE SYMBOL FETCH
# ══════════════════════════════════════════════════════════════
def fetch_symbol(tv: TvDatafeed, symbol: str, retries: int = 2) -> pd.DataFrame | None:
    """
    Single symbol ka full history fetch karo (upto TV_MAX_BARS daily bars).
    Returns DataFrame(index=datetime, cols=[open,high,low,close,volume]) or None.
    """
    delay = 2.0
    for attempt in range(retries):
        try:
            df = tv.get_hist(
                symbol=symbol,
                exchange="NSE",
                interval=Interval.in_daily,
                n_bars=TV_MAX_BARS,
            )
            if df is None or df.empty:
                return None

            # Normalize index
            if "datetime" in df.columns:
                df = df.set_index("datetime")
            df.index = pd.to_datetime(df.index)
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            df = df.sort_index()
            df.columns = [c.lower() for c in df.columns]

            needed = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
            return df[needed]

        except Exception as e:
            err_str = str(e).lower()
            if "rate" in err_str or "limit" in err_str or "429" in err_str:
                log(f"  Rate limit hit for {symbol} — sleeping {delay*2:.1f}s")
                time.sleep(delay * 2); delay *= 2
            elif attempt < retries - 1:
                time.sleep(delay); delay *= 2
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
    log(f"TradingView Cache Builder — {today_str}")
    log(f"Recent slice: last {RECENT_MONTHS} months (from {cutoff.strftime('%Y-%m-%d')})")
    log(f"Max bars per symbol: {TV_MAX_BARS}")
    log("=" * 60)

    # ── TV client ─────────────────────────────────────────────
    tv = get_tv_client()
    symbols = load_symbols()
    total   = len(symbols)

    # ── Per-symbol fetch ──────────────────────────────────────
    close_all, high_all, vol_all = {}, {}, {}
    ath_dict   = {}
    failed     = []
    ok_count   = 0

    for i, sym in enumerate(symbols):
        df = fetch_symbol(tv, sym)

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
    ath_df = pd.DataFrame.from_dict(
        {"ATH": ath_dict}, orient="index"
    ).T  # shape: (1, n_symbols) then transpose to (n_symbols, 1)
    # Standard format: index=symbol, column=ATH
    ath_df = pd.DataFrame({"ATH": ath_dict})

    # ── Save via rolling cache ────────────────────────────────
    meta = {
        "build_date":      today_str,
        "build_timestamp": datetime.now().isoformat(),
        "source":          "TradingView (tvDatafeed)",
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
        close_df   = close_df,
        high_df    = high_df,
        volume_df  = vol_df,
        ath_df     = ath_df,
        meta       = meta,
        date_str   = today_str,
    )

    log(f"\n✅ Cache saved to {CACHE_DIR}/{today_str}/")
    log(f"   close.parquet  : {close_df.shape}")
    log(f"   high.parquet   : {high_df.shape}")
    log(f"   volume.parquet : {vol_df.shape}")
    log(f"   ath.parquet    : {ath_df.shape}")
    log("=" * 60)


if __name__ == "__main__":
    build_cache()
