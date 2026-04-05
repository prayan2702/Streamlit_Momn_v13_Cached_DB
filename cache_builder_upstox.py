"""
cache_builder_upstox.py
=======================
Upstox V3 API se full history cache build karta hai.
GitHub Actions pe daily 9:30 PM IST pe chalta hai.

Key design — original data_service.py ka same sequential pattern:
  for each symbol:
      for each decade (3 calls):
          fetch()
          time.sleep(0.05)
  → Network latency (~0.5-1s per call) + sleep = ~1-2 req/sec
  → Naturally safe for all 3 Upstox limits

Decade ranges (Upstox max = 1 decade per days-interval call):
  Call 1: 2000-01-01 → 2009-12-31
  Call 2: 2010-01-01 → 2019-12-31
  Call 3: 2020-01-01 → today

ATH = concat(call1+call2+call3).high.max() — correct 2000-to-today ATH
Recent 40M = slice from merged data — close/high/volume parquet

SECURITY: access_token kabhi log nahi hota (sirf masked form mein)
"""

import json
import os
import sys
import time
from datetime import date, datetime
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

from upstox_auto_auth import get_token_from_env, _mask, _safe_log

# ── Config ────────────────────────────────────────────────────
GITHUB_BASE    = "https://raw.githubusercontent.com/prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main"
CACHE_DIR      = Path("cache_upstox")   # ← Separate folder — YFinance cache/ se alag
RECENT_MONTHS  = 40
EXTRA_SYMBOLS  = ["GOLDBEES.NS", "SILVERBEES.NS"]
NSE_EQUITY_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

# Upstox V3 daily candle endpoint
_UPSTOX_CANDLE = "https://api.upstox.com/v3/historical-candle"

# Decade ranges — 1 decade per call (Upstox limit)
_DECADE_RANGES = [
    ("2000-01-01", "2009-12-31"),
    ("2010-01-01", "2019-12-31"),
]
# Third range: 2020-01-01 → today (added dynamically in build_cache)


def log(msg: str):
    """Safe log — credentials is function se kabhi pass nahi hone chahiye."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Symbol loading ────────────────────────────────────────────
def load_symbols() -> list:
    """NSE EQUITY_L.csv + GOLDBEES + SILVERBEES."""
    log("Downloading EQUITY_L.csv from NSE...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0",
        "Referer":    "https://www.nseindia.com/",
        "Accept":     "text/html,*/*",
    }
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=15)
        time.sleep(1)
        resp = session.get(NSE_EQUITY_URL, headers=headers, timeout=30)
        resp.raise_for_status()

        df = pd.read_csv(StringIO(resp.text), skipinitialspace=True)
        df.columns = [c.strip() for c in df.columns]
        if "SERIES" in df.columns:
            df = df[df["SERIES"].str.strip() == "EQ"].copy()
        df["SYMBOL"] = df["SYMBOL"].str.strip().str.upper()
        symbols = (df["SYMBOL"] + ".NS").tolist()

        # Save for reference
        df.to_csv(CACHE_DIR / "EQUITY_L.csv", index=False)
        log(f"  NSE EQUITY_L.csv: {len(symbols):,} EQ stocks downloaded")

    except Exception as e:
        log(f"  NSE download failed ({type(e).__name__}) — GitHub fallback...")
        try:
            df  = pd.read_csv(f"{GITHUB_BASE}/NSE_EQ_ALL.csv")
            symbols = (df["Symbol"].astype(str).str.strip() + ".NS").tolist()
            log(f"  GitHub fallback: {len(symbols):,} symbols")
        except Exception as e2:
            raise RuntimeError(f"Symbol load failed completely: {type(e2).__name__}") from None

    for s in EXTRA_SYMBOLS:
        if s not in symbols:
            symbols.append(s)

    log(f"  Total: {len(symbols):,} (+ GOLDBEES & SILVERBEES)")
    return symbols


# ── Upstox instrument master ──────────────────────────────────
def load_instrument_map() -> dict:
    log("Loading Upstox instrument master...")
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    try:
        df  = pd.read_csv(url, compression="gzip", low_memory=False)
        df  = df[df["instrument_key"].astype(str).str.startswith("NSE_EQ|")].copy()
        mapping = dict(zip(df["tradingsymbol"].str.upper(), df["instrument_key"]))
        log(f"  {len(mapping):,} NSE EQ instruments loaded")
        return mapping
    except Exception as e:
        raise RuntimeError(f"Instrument master load failed: {type(e).__name__}") from None


def _get_key(symbol: str, instrument_map: dict) -> str | None:
    clean = symbol.replace(".NS", "").replace(".BO", "").upper().strip()
    return instrument_map.get(clean)


# ── Single symbol, single decade fetch ───────────────────────
def _fetch_one_decade(
    instrument_key : str,
    access_token   : str,
    from_date      : str,
    to_date        : str,
    retries        : int = 2,
) -> pd.DataFrame | None:
    """
    Ek decade ka data fetch karo.
    SAME logic as original data_service._fetch_upstox_history_live.
    access_token kabhi log nahi hota.
    """
    encoded = instrument_key.replace("|", "%7C")
    url     = f"{_UPSTOX_CANDLE}/{encoded}/days/1/{to_date}/{from_date}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json",
    }
    delay = 1.0

    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=15)

            if resp.status_code == 429:
                time.sleep(delay * 2)
                delay *= 2
                continue

            if resp.status_code in (401, 403):
                # Token invalid — value log nahi karte
                raise ValueError(f"Token invalid (HTTP {resp.status_code})")

            resp.raise_for_status()
            candles = resp.json().get("data", {}).get("candles", [])

            if not candles:
                return None

            df = pd.DataFrame(
                candles,
                columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            # Timezone strip: tz_convert("Asia/Kolkata") → tz_localize(None)
            # Upstox returns ISO timestamps like "2026-04-04T09:15:00+05:30"
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            df.set_index("timestamp", inplace=True)
            # ── CRITICAL FIX (Issue 1): Normalize to date-only (00:00:00) ──
            # Upstox candle timestamps have time component (e.g. 09:15:00).
            # bdate_range uses 00:00:00 → reindex() fails to match → all NaN.
            # normalize() strips time → 2026-04-04T09:15 → 2026-04-04T00:00
            df.index = df.index.normalize()
            df.sort_index(inplace=True)
            # Keep last candle per day (in case of duplicates after normalize)
            df = df[~df.index.duplicated(keep="last")]
            return df[["open", "high", "low", "close", "volume"]]

        except ValueError:
            raise  # Token error — caller handle karega
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay)
            delay *= 2

    return None


# ── Sequential bulk fetch — 3 decades per symbol ─────────────
def fetch_all_sequential(
    symbols        : list,
    instrument_map : dict,
    access_token   : str,
    decade_ranges  : list,
    end_date       : datetime,
) -> tuple[dict, dict, dict, dict, list]:
    """
    Original data_service.py ka same sequential pattern.
    for each symbol:
        for each of 3 decades:
            _fetch_one_decade()
            time.sleep(0.05)   ← original se same
    Network latency (~0.5-1s) + sleep = ~1-2 req/sec naturally safe.

    Returns: ath_dict, close_map, high_map, vol_map, failed
    """
    start_recent = end_date - relativedelta(months=RECENT_MONTHS)
    total        = len(symbols)
    not_found    = 0
    ath_dict     = {}
    close_map    = {}
    high_map     = {}
    vol_map      = {}
    failed       = []
    t0           = time.monotonic()

    for i, sym in enumerate(symbols):
        progress       = (i + 1) / total
        instrument_key = _get_key(sym, instrument_map)

        if not instrument_key:
            not_found += 1
            failed.append(sym)
        else:
            decade_dfs = []
            token_expired = False

            # ── 3 decade calls — same as original per-symbol ──
            for from_d, to_d in decade_ranges:
                try:
                    df = _fetch_one_decade(
                        instrument_key, access_token, from_d, to_d
                    )
                    if df is not None and not df.empty:
                        decade_dfs.append(df)
                except ValueError:
                    # Token expired mid-download
                    log("Token expired mid-download — stopping.")
                    token_expired = True
                    break
                except Exception:
                    pass  # Is decade ka data nahi mila — skip, agla try karo

                time.sleep(0.05)  # Original se same sleep

            if token_expired:
                # Baaki symbols skip karo — token invalid ho gaya
                raise RuntimeError(
                    "Upstox token expired mid-download. "
                    "Re-run the workflow to get a fresh token."
                )

            if decade_dfs:
                # Merge all decades
                merged = pd.concat(decade_dfs).sort_index()
                merged = merged[~merged.index.duplicated(keep="last")]

                # ATH — full 2000-today max
                ath_dict[sym] = float(merged["high"].max())

                # Recent slice — last 40 months only (for parquet files)
                df_r = merged[merged.index >= start_recent]
                if not df_r.empty:
                    idx = pd.to_datetime(df_r.index)
                    close_map[sym] = pd.Series(df_r["close"].values,                    index=idx)
                    high_map[sym]  = pd.Series(df_r["high"].values,                     index=idx)
                    vol_map[sym]   = pd.Series((df_r["close"]*df_r["volume"]).values,   index=idx)
            else:
                failed.append(sym)

        # ── Progress log every 50 symbols ─────────────────────
        if i % 50 == 0 or i == total - 1:
            elapsed   = time.monotonic() - t0
            remaining = (total - i - 1) * (elapsed / max(i + 1, 1))
            log(
                f"  [{i+1}/{total}] {int(progress*100)}% | "
                f"✅ {len(ath_dict)} fetched | "
                f"❌ {len(failed) - not_found} failed | "
                f"🔍 {not_found} not in master | "
                f"ETA: {remaining/60:.1f}min"
            )

    log(
        f"Fetch complete: {len(ath_dict)}/{total} | "
        f"Not in master: {not_found} | "
        f"Failed: {len(failed) - not_found} | "
        f"Time: {(time.monotonic()-t0)/60:.1f}min"
    )
    return ath_dict, close_map, high_map, vol_map, failed


# ── Main ──────────────────────────────────────────────────────
def build_cache():
    log("=" * 55)
    log("MOMN CACHE BUILDER — UPSTOX VERSION")
    log("=" * 55)
    CACHE_DIR.mkdir(exist_ok=True)
    t_total = time.monotonic()

    # 1. Auth (credentials env vars se — kabhi log nahi hote)
    log("Authenticating with Upstox...")
    access_token = get_token_from_env()
    # Token value kabhi log nahi karte — sirf masked confirmation
    log(f"  Token received: {_mask(access_token)} ✅")

    # 2. Symbols
    symbols = load_symbols()

    # 3. Instrument master
    instrument_map = load_instrument_map()

    # 4. Build decade ranges (3rd range = 2020 → today)
    end_date    = datetime.combine(date.today(), datetime.min.time())
    today_str   = end_date.strftime("%Y-%m-%d")
    decade_ranges = [
        ("2000-01-01", "2009-12-31"),
        ("2010-01-01", "2019-12-31"),
        ("2020-01-01", today_str),
    ]
    log(f"Decade ranges: {[f'{f}→{t}' for f,t in decade_ranges]}")
    log(f"Total API calls: {len(symbols)} symbols × 3 decades = {len(symbols)*3:,}")

    # 5. Sequential fetch (same pattern as original data_service.py)
    ath_dict, close_map, high_map, vol_map, failed = fetch_all_sequential(
        symbols, instrument_map, access_token, decade_ranges, end_date
    )

    # 6. Assemble DataFrames
    log("Assembling DataFrames...")
    start_recent = end_date - relativedelta(months=RECENT_MONTHS)
    all_idx      = pd.bdate_range(start=start_recent, end=end_date)

    def _make_df(data_map):
        df = pd.DataFrame(
            {s: v.reindex(all_idx) for s, v in data_map.items()},
            index=all_idx
        )
        return df.loc[:, ~df.columns.duplicated()]

    close  = _make_df(close_map)
    high   = _make_df(high_map)
    volume = _make_df(vol_map)
    ath_df = pd.Series(ath_dict, name="ATH", dtype=float).to_frame()

    log(f"  close: {close.shape} | high: {high.shape} | vol: {volume.shape} | ath: {ath_df.shape}")

    if close.empty:
        log("ERROR: close DataFrame empty — something went wrong")
        sys.exit(1)

    # 7. Save Parquet files
    log("Saving Parquet files...")
    close.to_parquet(CACHE_DIR  / "close.parquet")
    high.to_parquet(CACHE_DIR   / "high.parquet")
    volume.to_parquet(CACHE_DIR / "volume.parquet")
    ath_df.to_parquet(CACHE_DIR / "ath.parquet")

    for fname in ["close.parquet", "high.parquet", "volume.parquet", "ath.parquet"]:
        mb = (CACHE_DIR / fname).stat().st_size / 1_048_576
        log(f"  {fname}: {mb:.1f} MB")

    # 8. Meta JSON
    total_min = (time.monotonic() - t_total) / 60
    meta = {
        "build_date"         : date.today().isoformat(),
        "build_time_utc"     : datetime.utcnow().strftime("%H:%M:%S"),
        "build_duration_min" : round(total_min, 1),
        "symbols_total"      : len(symbols),
        "symbols_fetched"    : len(ath_dict),
        "symbols_failed"     : len([f for f in failed if f not in
                                    [s for s in symbols if not _get_key(s, instrument_map)]]),
        "not_in_master"      : len(symbols) - len([s for s in symbols
                                                   if _get_key(s, instrument_map)]),
        "failed_symbols"     : sorted(failed),          # ← ALL failed (no [:50] cap — Issue 3 fix)
        "data_start_full"    : "2000-01-01",
        "data_start_recent"  : start_recent.strftime("%Y-%m-%d"),
        "data_end"           : today_str,
        "recent_months"      : RECENT_MONTHS,
        "source"             : "Upstox V3 (daily candles)",
        "symbol_source"      : "NSE EQUITY_L.csv (direct download)",
        "extra_symbols"      : EXTRA_SYMBOLS,
        "decades_per_symbol" : 3,
        "decade_ranges"      : decade_ranges,
        "total_api_calls"    : len(ath_dict) * 3,
        "close_shape"        : list(close.shape),
        "high_shape"         : list(high.shape),
        "volume_shape"       : list(volume.shape),
        "ath_count"          : len(ath_df),
    }
    with open(CACHE_DIR / "cache_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    log("=" * 55)
    log(f"✅ UPSTOX CACHE BUILD COMPLETE")
    log(f"   Symbols  : {meta['symbols_fetched']}/{meta['symbols_total']} fetched")
    log(f"   API calls: {meta['total_api_calls']:,}")
    log(f"   Time     : {total_min:.1f} min")
    log("=" * 55)


if __name__ == "__main__":
    build_cache()
