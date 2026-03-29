"""
cache_builder.py
================
GitHub Actions pe daily chalta hai (roz 8:30 PM IST).
YFinance se data fetch karta hai aur cache/ folder mein save karta hai.

Kya karta hai:
  1. NSE_EQ_ALL.csv se symbols load karo (+ GOLDBEES + SILVERBEES)
  2. yfinance.download(start=2000-01-01) — full history ek hi call mein
  3. ATH = high.max() — sirf ek number per symbol (tiny file)
  4. Recent 40 months close/high/volume — Parquet files
  5. cache_meta.json — build info

Run kaise karein:
  Local test : python cache_builder.py
  GitHub     : .github/workflows/daily_cache.yml automatically chalata hai
"""

import json
import time
import sys
from datetime import datetime, date
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from dateutil.relativedelta import relativedelta

# ── Config ────────────────────────────────────────────────────
GITHUB_BASE   = "https://raw.githubusercontent.com/prayan2702/Streamlit-momn/refs/heads/main"
CACHE_DIR     = Path("cache")
CHUNK_SIZE    = 50       # symbols per yfinance.download call
CHUNK_SLEEP   = 0.5     # seconds between chunks (yfinance rate limit safe)
RECENT_MONTHS = 40       # recent data kitne months ka store karein
EXTRA_SYMBOLS = ["GOLDBEES.NS", "SILVERBEES.NS"]

# ── Helpers ───────────────────────────────────────────────────
def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def load_symbols() -> list:
    """NSE_EQ_ALL.csv se symbols load + GOLDBEES/SILVERBEES add."""
    log("Loading symbol list from GitHub...")
    url = f"{GITHUB_BASE}/NSE_EQ_ALL.csv"
    df  = pd.read_csv(url)
    symbols = (df['Symbol'].astype(str).str.strip() + '.NS').tolist()
    for s in EXTRA_SYMBOLS:
        if s not in symbols:
            symbols.append(s)
    log(f"  Symbols loaded: {len(symbols):,} (incl. GOLDBEES & SILVERBEES)")
    return symbols


def fetch_all_chunks(symbols: list, start_full: datetime, end_date: datetime):
    """
    Chunked yfinance download — 2000 se aaj tak.
    Returns: ath_dict, close_chunks, high_chunks, vol_chunks, failed
    """
    total        = len(symbols)
    n_chunks     = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
    ath_dict     = {}
    close_chunks = []
    high_chunks  = []
    vol_chunks   = []
    failed       = []
    start_recent = end_date - relativedelta(months=RECENT_MONTHS)

    log(f"Starting fetch: {total:,} symbols | {n_chunks} chunks | start=2000-01-01")
    log(f"Recent window: {start_recent.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")

    t0 = time.monotonic()

    for k in range(0, total, CHUNK_SIZE):
        chunk     = symbols[k : k + CHUNK_SIZE]
        chunk_num = k // CHUNK_SIZE + 1
        pct       = min((k + CHUNK_SIZE) / total, 1.0)

        try:
            raw = yf.download(
                chunk,
                start=start_full,
                end=end_date,
                progress=False,
                auto_adjust=True,
                threads=True,
                multi_level_index=False,
            )

            if raw.empty:
                log(f"  Chunk {chunk_num}/{n_chunks} — EMPTY, skipping")
                failed.extend(chunk)
                time.sleep(CHUNK_SLEEP)
                continue

            # ── ATH: max High from 2000 to today ──────────────
            if "High" in raw.columns:
                ath_dict.update(raw["High"].max().to_dict())

            # ── Recent slice: last 40 months ───────────────────
            raw_r = raw[raw.index >= start_recent].copy()

            if "Close" in raw_r.columns:
                close_chunks.append(raw_r["Close"])
            if "High" in raw_r.columns:
                high_chunks.append(raw_r["High"])
            if "Close" in raw_r.columns and "Volume" in raw_r.columns:
                vol_chunks.append(raw_r["Close"] * raw_r["Volume"])

        except Exception as e:
            log(f"  Chunk {chunk_num}/{n_chunks} — ERROR: {e}")
            failed.extend(chunk)

        # ── Progress log ───────────────────────────────────────
        elapsed   = time.monotonic() - t0
        remaining = (n_chunks - chunk_num) * (elapsed / chunk_num)
        log(
            f"  Chunk {chunk_num}/{n_chunks} | {pct*100:.0f}% | "
            f"ATH: {len(ath_dict):,} | "
            f"Elapsed: {elapsed/60:.1f}min | "
            f"ETA: {remaining/60:.1f}min"
        )

        time.sleep(CHUNK_SLEEP)

    return ath_dict, close_chunks, high_chunks, vol_chunks, failed


def concat_and_dedup(chunks: list) -> pd.DataFrame:
    """Chunks concat karo + duplicate columns remove karo."""
    if not chunks:
        return pd.DataFrame()
    df = pd.concat(chunks, axis=1)
    df = df.loc[:, ~df.columns.duplicated()]
    df.index = pd.to_datetime(df.index)
    # Timezone remove karo agar ho
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df


def build_ath_df(ath_dict: dict) -> pd.DataFrame:
    """ATH dict → clean DataFrame."""
    s = pd.Series(ath_dict, name="ATH", dtype=float)
    s = s.replace([np.inf, -np.inf], np.nan).dropna()
    return s.to_frame()


# ── Main ──────────────────────────────────────────────────────
def build_cache():
    log("=" * 55)
    log("MOMN CACHE BUILDER — Starting")
    log("=" * 55)

    CACHE_DIR.mkdir(exist_ok=True)
    t_total = time.monotonic()

    # 1. Symbols
    symbols   = load_symbols()
    end_date  = datetime.combine(date.today(), datetime.min.time())
    start_full= datetime(2000, 1, 1)

    # 2. Fetch
    ath_dict, close_chunks, high_chunks, vol_chunks, failed = fetch_all_chunks(
        symbols, start_full, end_date
    )

    # 3. Concat
    log("Concatenating DataFrames...")
    close  = concat_and_dedup(close_chunks)
    high   = concat_and_dedup(high_chunks)
    volume = concat_and_dedup(vol_chunks)
    ath_df = build_ath_df(ath_dict)

    log(f"  close  shape: {close.shape}")
    log(f"  high   shape: {high.shape}")
    log(f"  volume shape: {volume.shape}")
    log(f"  ath    shape: {ath_df.shape}")

    if close.empty:
        log("ERROR: close DataFrame is empty — something went wrong!")
        sys.exit(1)

    # 4. Save Parquet files
    log("Saving Parquet files...")
    close.to_parquet(CACHE_DIR / "close.parquet")
    high.to_parquet(CACHE_DIR  / "high.parquet")
    volume.to_parquet(CACHE_DIR/ "volume.parquet")
    ath_df.to_parquet(CACHE_DIR/ "ath.parquet")

    # File sizes log karo
    for fname in ["close.parquet", "high.parquet", "volume.parquet", "ath.parquet"]:
        size_mb = (CACHE_DIR / fname).stat().st_size / (1024 * 1024)
        log(f"  {fname}: {size_mb:.1f} MB")

    # 5. Meta JSON
    total_time_min = (time.monotonic() - t_total) / 60
    meta = {
        "build_date"         : date.today().isoformat(),
        "build_time_utc"     : datetime.utcnow().strftime("%H:%M:%S"),
        "build_duration_min" : round(total_time_min, 1),
        "symbols_total"      : len(symbols),
        "symbols_fetched"    : len(ath_dict),
        "symbols_failed"     : len(failed),
        "failed_symbols"     : sorted(failed)[:50],
        "data_start_full"    : "2000-01-01",
        "data_start_recent"  : (end_date - relativedelta(months=RECENT_MONTHS)).strftime("%Y-%m-%d"),
        "data_end"           : end_date.strftime("%Y-%m-%d"),
        "recent_months"      : RECENT_MONTHS,
        "source"             : "YFinance",
        "extra_symbols"      : EXTRA_SYMBOLS,
        "close_shape"        : list(close.shape),
        "high_shape"         : list(high.shape),
        "volume_shape"       : list(volume.shape),
        "ath_count"          : len(ath_df),
    }

    with open(CACHE_DIR / "cache_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    log("=" * 55)
    log(f"✅ CACHE BUILD COMPLETE")
    log(f"   Symbols: {meta['symbols_fetched']}/{meta['symbols_total']} fetched")
    log(f"   Failed : {meta['symbols_failed']} symbols")
    log(f"   Time   : {total_time_min:.1f} minutes")
    log(f"   Files  : cache/ath.parquet + close/high/volume.parquet + cache_meta.json")
    log("=" * 55)


if __name__ == "__main__":
    build_cache()
