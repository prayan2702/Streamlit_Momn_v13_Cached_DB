"""
cache_builder.py
================
GitHub Actions pe daily chalta hai (roz 7:30 PM IST / 14:00 UTC).
YFinance se data fetch karta hai aur cache/ folder mein save karta hai.

Kya karta hai:
  1. NSE EQUITY_L.csv se symbols load karo (+ GOLDBEES + SILVERBEES)
  2. yfinance.download(start=2000-01-01) — full history ek hi call mein
  3. ATH = high.max() — sirf ek number per symbol (tiny file)
  4. Recent 40 months close/high/volume — Parquet files
  5. cache_meta.json — build info (last_date_in_cache + today_data_present fields)

── BUG FIX: yfinance end-date ────────────────────────────────
  yfinance `end` parameter EXCLUSIVE hai (Python range jaisa).
  end = "2026-04-10"  →  data sirf 2026-04-09 tak milta hai  ❌
  end = "2026-04-11"  →  data 2026-04-10 tak milta hai       ✅
  Isliye: end_date_yf = today + 1 day use karo.
  Metadata mein `data_end` = today (actual last trading day) dikhata hai.
──────────────────────────────────────────────────────────────
"""

import json
import time
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from dateutil.relativedelta import relativedelta

# ── Config ────────────────────────────────────────────────────
GITHUB_BASE   = "https://raw.githubusercontent.com/prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main"
CACHE_DIR     = Path("cache")
CHUNK_SIZE    = 50
CHUNK_SLEEP   = 0.5
RECENT_MONTHS = 40
EXTRA_SYMBOLS = ["GOLDBEES.NS", "SILVERBEES.NS"]

NSE_EQUITY_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer":         "https://www.nseindia.com/",
}

# ── Helpers ───────────────────────────────────────────────────
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_symbols() -> list:
    log("Downloading EQUITY_L.csv from NSE...")
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=15)
        time.sleep(1)
        resp = session.get(NSE_EQUITY_URL, headers=NSE_HEADERS, timeout=30)
        resp.raise_for_status()

        from io import StringIO
        df = pd.read_csv(StringIO(resp.text), skipinitialspace=True)
        df.columns = [c.strip() for c in df.columns]
        if 'SERIES' in df.columns:
            df = df[df['SERIES'].str.strip() == 'EQ'].copy()
        df['SYMBOL'] = df['SYMBOL'].str.strip().str.upper()
        df = df.reset_index(drop=True)
        symbols = (df['SYMBOL'] + '.NS').tolist()
        log(f"  ✅ NSE EQUITY_L.csv: {len(df):,} EQ stocks")
        df.to_csv(CACHE_DIR / "EQUITY_L.csv", index=False)
        df.to_csv(Path("EQUITY_L.csv"), index=False)

    except Exception as e:
        log(f"  ⚠️  NSE download failed: {e} — falling back to GitHub...")
        try:
            url = f"{GITHUB_BASE}/NSE_EQ_ALL.csv"
            df  = pd.read_csv(url)
            symbols = (df['Symbol'].astype(str).str.strip() + '.NS').tolist()
            log(f"  ✅ GitHub fallback: {len(symbols):,} symbols")
        except Exception as e2:
            raise RuntimeError("Symbol list load failed.") from e2

    for s in EXTRA_SYMBOLS:
        if s not in symbols:
            symbols.append(s)
    log(f"  Total: {len(symbols):,} (incl. GOLDBEES & SILVERBEES)")
    return symbols


def fetch_all_chunks(symbols: list, start_full: datetime, end_date_yf: datetime, start_recent: datetime):
    """
    Chunked yfinance download.

    end_date_yf = today + 1 day  (yfinance end is EXCLUSIVE)
    Actual data will come up to today (last trading day).
    """
    total        = len(symbols)
    n_chunks     = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
    ath_dict     = {}
    close_chunks = []
    high_chunks  = []
    vol_chunks   = []
    failed       = []

    effective_end = (end_date_yf - timedelta(days=1)).strftime('%Y-%m-%d')
    log(f"Fetch: {total:,} symbols | {n_chunks} chunks")
    log(f"yfinance end (exclusive): {end_date_yf.date()} → effective last date: {effective_end}")
    log(f"Recent window start: {start_recent.strftime('%Y-%m-%d')}")

    t0 = time.monotonic()

    for k in range(0, total, CHUNK_SIZE):
        chunk     = symbols[k : k + CHUNK_SIZE]
        chunk_num = k // CHUNK_SIZE + 1
        pct       = min((k + CHUNK_SIZE) / total, 1.0)

        try:
            raw = yf.download(
                chunk,
                start=start_full,
                end=end_date_yf,        # ← FIXED: today+1 so today's data is included
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

            if "High" in raw.columns:
                ath_dict.update(raw["High"].max().to_dict())

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

        elapsed   = time.monotonic() - t0
        remaining = (n_chunks - chunk_num) * (elapsed / chunk_num)
        log(
            f"  Chunk {chunk_num}/{n_chunks} | {pct*100:.0f}% | "
            f"ATH: {len(ath_dict):,} | "
            f"Elapsed: {elapsed/60:.1f}min | ETA: {remaining/60:.1f}min"
        )
        time.sleep(CHUNK_SLEEP)

    return ath_dict, close_chunks, high_chunks, vol_chunks, failed


def concat_and_dedup(chunks: list) -> pd.DataFrame:
    if not chunks:
        return pd.DataFrame()
    df = pd.concat(chunks, axis=1)
    df = df.loc[:, ~df.columns.duplicated()]
    df.index = pd.to_datetime(df.index)
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df


def build_ath_df(ath_dict: dict) -> pd.DataFrame:
    s = pd.Series(ath_dict, name="ATH", dtype=float)
    s = s.replace([np.inf, -np.inf], np.nan).dropna()
    return s.to_frame()


def verify_data_freshness(close: pd.DataFrame, today: date) -> bool:
    """
    Cache mein today ka data hai ya nahi — check karo aur log karo.
    Returns True if today's data is present.
    """
    if close.empty:
        log("  ⚠️  close DataFrame empty — cannot verify freshness")
        return False
    last_date = close.index[-1].date()
    log(f"  Last date in cache : {last_date}")
    log(f"  Today's date       : {today}")
    if last_date >= today:
        log(f"  ✅ TODAY'S DATA CONFIRMED in cache ({last_date})")
        return True
    else:
        log(f"  ⚠️  Today's data ({today}) NOT in cache!")
        log(f"     Last available: {last_date}")
        log(f"     Possible reason: market holiday/weekend, or yfinance data delay")
        return False


# ── Main ──────────────────────────────────────────────────────
def build_cache():
    log("=" * 58)
    log("MOMN CACHE BUILDER (YFinance) — Starting")
    log("=" * 58)

    CACHE_DIR.mkdir(exist_ok=True)
    t_total = time.monotonic()

    today      = date.today()
    start_full = datetime(2000, 1, 1)

    # ── KEY FIX: yfinance end is EXCLUSIVE ────────────────────
    # end = today         →  data only up to YESTERDAY  ❌
    # end = today + 1     →  data includes TODAY         ✅
    end_date_yf  = datetime.combine(today + timedelta(days=1), datetime.min.time())
    start_recent = datetime.combine(today, datetime.min.time()) - relativedelta(months=RECENT_MONTHS)

    log(f"Today              : {today}")
    log(f"yfinance end (excl): {end_date_yf.date()}  (includes data up to {today})")
    log(f"Recent window start: {start_recent.date()}")

    # 1. Symbols
    symbols = load_symbols()

    # 2. Fetch
    ath_dict, close_chunks, high_chunks, vol_chunks, failed = fetch_all_chunks(
        symbols, start_full, end_date_yf, start_recent
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
        log("ERROR: close DataFrame is empty!")
        sys.exit(1)

    # 4. Data freshness check
    log("Verifying data freshness...")
    today_present = verify_data_freshness(close, today)
    last_date_in_cache = close.index[-1].date() if not close.empty else None

    # 5. Save Parquet
    log("Saving Parquet files...")
    close.to_parquet(CACHE_DIR / "close.parquet")
    high.to_parquet(CACHE_DIR  / "high.parquet")
    volume.to_parquet(CACHE_DIR/ "volume.parquet")
    ath_df.to_parquet(CACHE_DIR/ "ath.parquet")

    for fname in ["close.parquet", "high.parquet", "volume.parquet", "ath.parquet"]:
        size_mb = (CACHE_DIR / fname).stat().st_size / (1024 * 1024)
        log(f"  {fname}: {size_mb:.1f} MB")

    # 6. Meta JSON
    total_time_min = (time.monotonic() - t_total) / 60
    meta = {
        "build_date"             : today.isoformat(),
        "build_time_utc"         : datetime.utcnow().strftime("%H:%M:%S"),
        "build_duration_min"     : round(total_time_min, 1),
        "symbols_total"          : len(symbols),
        "symbols_fetched"        : len(ath_dict),
        "symbols_failed"         : len(failed),
        "failed_symbols"         : sorted(failed)[:50],
        "data_start_full"        : "2000-01-01",
        "data_start_recent"      : start_recent.strftime("%Y-%m-%d"),
        "data_end"               : today.isoformat(),
        "last_date_in_cache"     : str(last_date_in_cache),
        "today_data_present"     : today_present,
        "recent_months"          : RECENT_MONTHS,
        "source"                 : "YFinance",
        "yfinance_end_exclusive" : end_date_yf.strftime("%Y-%m-%d"),
        "symbol_source"          : "NSE EQUITY_L.csv (direct)",
        "extra_symbols"          : EXTRA_SYMBOLS,
        "close_shape"            : list(close.shape),
        "high_shape"             : list(high.shape),
        "volume_shape"           : list(volume.shape),
        "ath_count"              : len(ath_df),
    }

    with open(CACHE_DIR / "cache_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    log("=" * 58)
    log("✅ CACHE BUILD COMPLETE")
    log(f"   Symbols       : {meta['symbols_fetched']}/{meta['symbols_total']} fetched")
    log(f"   Failed        : {meta['symbols_failed']} symbols")
    log(f"   Last date     : {last_date_in_cache}")
    log(f"   Today present : {'✅ YES' if today_present else '⚠️  NO (holiday/weekend?)'}")
    log(f"   Time          : {total_time_min:.1f} minutes")
    log("=" * 58)


if __name__ == "__main__":
    build_cache()
