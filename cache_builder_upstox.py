"""
cache_builder_upstox.py
=======================
Upstox V3 API se full history cache build karta hai.
GitHub Actions pe daily 6:02 PM IST (12:32 UTC) pe chalta hai.

── ROOT CAUSE FIX: Upstox T+1 Delay ────────────────────────
  Upstox /v3/historical-candle/days/1/ API genuinely T+1 delay
  hai — same trading day ka candle KABHI nahi milta is endpoint
  se, chahe to_date = tomorrow pass karo ya nahi.

  CONFIRMED: 11:50 PM IST pe bhi Monday ka data nahi aata —
  sirf Friday (prev trading day) tak ka data milta hai.

  FIX: Upstox Market Quote API use karo today's data ke liye.
    GET /v3/market-quote/quotes?symbol=NSE_EQ%7C...
  
  Market close (3:30 PM IST) ke baad:
    - ohlc.close = official closing price for today ✅
    - ohlc.high  = day's high ✅
    - volume     = total day volume ✅
  
  Flow:
    1. Historical fetch → data up to yesterday (as before)
    2. Market Quote fetch → today's OHLCV in batches of 100
    3. If today not in index AND volume > 0 → append today's row
    4. Update ATH if today's high > historical max
──────────────────────────────────────────────────────────────
"""

import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

from upstox_auto_auth import get_token_from_env, _mask, _safe_log

# ── Config ────────────────────────────────────────────────────
GITHUB_BASE    = "https://raw.githubusercontent.com/prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main"
CACHE_DIR      = Path("cache_upstox")
RECENT_MONTHS  = 40
EXTRA_SYMBOLS  = ["GOLDBEES.NS", "SILVERBEES.NS"]
NSE_EQUITY_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

_UPSTOX_CANDLE      = "https://api.upstox.com/v3/historical-candle"
_UPSTOX_OHLC_URL    = "https://api.upstox.com/v3/market-quote/ohlc"   # ← FIXED endpoint
_QUOTE_BATCH_SIZE   = 100   # symbols per request (URL length safe)

# ── Why OHLC endpoint, not quotes endpoint ───────────────────
# /v3/market-quote/quotes  → ohlc.close = PREVIOUS SESSION close ❌
# /v3/market-quote/ohlc    → live_ohlc  = current session  ✅
#                             prev_ohlc  = previous session
#
# Field selection logic (based on IST time when script runs):
#   live_ohlc.volume > 0  → market traded today → use live_ohlc ✅
#   live_ohlc.volume = 0  → next session not started (12:10 AM case)
#                         → use prev_ohlc = last completed session ✅
#
# Response key format: API returns "NSE_EQ:RELIANCE" (colon, not pipe)
# Must normalize: raw_key.replace(":", "|") before lookup


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Symbol loading ────────────────────────────────────────────
def load_symbols() -> list:
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
        df.to_csv(CACHE_DIR / "EQUITY_L.csv", index=False)
        log(f"  NSE EQUITY_L.csv: {len(symbols):,} EQ stocks")

    except Exception as e:
        log(f"  NSE download failed ({type(e).__name__}) — GitHub fallback...")
        try:
            df  = pd.read_csv(f"{GITHUB_BASE}/NSE_EQ_ALL.csv")
            symbols = (df["Symbol"].astype(str).str.strip() + ".NS").tolist()
            log(f"  GitHub fallback: {len(symbols):,} symbols")
        except Exception as e2:
            raise RuntimeError(f"Symbol load failed: {type(e2).__name__}") from None

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
    Ek decade ka historical data fetch karo.
    NOTE: This endpoint has T+1 delay — today's candle NOT available here.
    Today's data is handled separately via _fetch_today_quotes().
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
            # Strip timezone: keep date as-is (IST date, not UTC conversion)
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
            return df[["open", "high", "low", "close", "volume"]]

        except ValueError:
            raise
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


# ── TODAY's data via OHLC Market Quote API ───────────────────
def _fetch_today_quotes(
    symbols        : list,
    instrument_map : dict,
    access_token   : str,
    today          : date,
) -> dict:
    """
    Upstox OHLC Market Quote API se today's OHLCV fetch karo.

    Historical candle API has T+1 delay — same-day candle NEVER available.
    OHLC Quote API (/v3/market-quote/ohlc?interval=1d) is the correct fix:
      live_ohlc = current trading session (today, if market has traded)
      prev_ohlc = previous trading session (yesterday)

    Field selection:
      live_ohlc.volume > 0  → market traded today → use live_ohlc ✅
      live_ohlc.volume = 0  → 12:10 AM case (next session not started)
                            → use prev_ohlc = last completed session ✅

    IMPORTANT: Response keys use COLON not pipe:
      Sent:     "NSE_EQ|RELIANCE"
      Returned: "NSE_EQ:RELIANCE"   ← must normalize before lookup!

    Params passed via requests params dict (not manual %7C encoding).

    Returns: { "RELIANCE.NS": {"close":..., "high":..., "volume":...}, ... }
    """
    if today.weekday() >= 5:
        log(f"  Today ({today}) is weekend — skipping quotes")
        return {}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json",
    }

    sym_key_pairs = []
    for sym in symbols:
        key = _get_key(sym, instrument_map)
        if key:
            sym_key_pairs.append((sym, key))

    log(f"  Fetching OHLC quotes for {len(sym_key_pairs):,} symbols "
        f"in batches of {_QUOTE_BATCH_SIZE} via /v3/market-quote/ohlc...")

    today_data = {}
    n_batches  = (len(sym_key_pairs) + _QUOTE_BATCH_SIZE - 1) // _QUOTE_BATCH_SIZE
    ok_count   = 0
    skip_count = 0
    t0         = time.monotonic()

    for b_idx in range(0, len(sym_key_pairs), _QUOTE_BATCH_SIZE):
        batch    = sym_key_pairs[b_idx : b_idx + _QUOTE_BATCH_SIZE]
        batch_no = b_idx // _QUOTE_BATCH_SIZE + 1

        # Build lookup: both pipe and colon variants → sym
        # API response uses COLON ("NSE_EQ:RELIANCE"), we send PIPE ("NSE_EQ|RELIANCE")
        key_to_sym = {}
        for sym, key in batch:
            key_to_sym[key]                    = sym   # NSE_EQ|RELIANCE
            key_to_sym[key.replace("|", ":")] = sym   # NSE_EQ:RELIANCE ← API response format

        # Use requests params dict — requests handles URL encoding automatically
        keys_str = ",".join(key for _, key in batch)
        params   = {"instrument_key": keys_str, "interval": "1d"}

        try:
            resp = requests.get(_UPSTOX_OHLC_URL, headers=headers,
                                params=params, timeout=30)

            if resp.status_code == 429:
                time.sleep(5)
                resp = requests.get(_UPSTOX_OHLC_URL, headers=headers,
                                    params=params, timeout=30)

            if resp.status_code in (401, 403):
                raise ValueError(f"Token invalid (HTTP {resp.status_code})")

            resp.raise_for_status()
            data = resp.json().get("data", {})

            for raw_key, quote in data.items():
                # Normalize: response key may be "NSE_EQ:RELIANCE" or "NSE_EQ|RELIANCE"
                sym = key_to_sym.get(raw_key)
                if not sym:
                    # Try alternate separator just in case
                    sym = key_to_sym.get(raw_key.replace(":", "|"))
                if not sym:
                    continue

                # ── FIELD SELECTION: live vs prev ──────────────
                # live_ohlc = current trading session (today)
                # prev_ohlc = previous session (yesterday)
                #
                # At 12:10 AM: live_ohlc.volume = 0 (session not started)
                #   → use prev_ohlc which = today's just-closed session ✅
                # At 8 PM same day: live_ohlc.volume > 0 (today's data) ✅
                live  = quote.get("live_ohlc", {}) or {}
                prev  = quote.get("prev_ohlc", {}) or {}

                live_vol   = float(live.get("volume") or 0)
                live_close = float(live.get("close")  or 0)

                if live_vol > 0 and live_close > 0:
                    # Market has traded today — use live session
                    chosen     = live
                    chosen_src = "live_ohlc"
                else:
                    # Session not started (midnight case) — use prev session
                    chosen     = prev
                    chosen_src = "prev_ohlc"

                close  = float(chosen.get("close")  or 0)
                high   = float(chosen.get("high")   or 0)
                volume = float(chosen.get("volume") or 0)

                if close <= 0 or volume <= 0:
                    skip_count += 1
                    continue

                today_data[sym] = {
                    "close":  close,
                    "high":   high,
                    "volume": volume,
                    "_src":   chosen_src,
                }
                ok_count += 1

        except ValueError:
            raise
        except Exception as e:
            log(f"  Quote batch {batch_no}/{n_batches} failed: {type(e).__name__}: {e}")

        time.sleep(0.2)

        if batch_no % 5 == 0 or batch_no == n_batches:
            elapsed = time.monotonic() - t0
            log(
                f"  Quote batches: {batch_no}/{n_batches} | "
                f"Got: {ok_count} | Skipped: {skip_count} | "
                f"Time: {elapsed:.1f}s"
            )

    # Report which source was used (live vs prev)
    live_count = sum(1 for v in today_data.values() if v.get("_src") == "live_ohlc")
    prev_count = sum(1 for v in today_data.values() if v.get("_src") == "prev_ohlc")
    log(f"  Today's quotes: {ok_count} symbols | "
        f"live_ohlc: {live_count} | prev_ohlc: {prev_count} | "
        f"skipped: {skip_count}")
    return today_data


def _apply_today_quotes(
    close     : pd.DataFrame,
    high      : pd.DataFrame,
    volume    : pd.DataFrame,
    ath_dict  : dict,
    today_data: dict,
    today     : date,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Today's market quote data ko close/high/volume DataFrames mein append karo.
    ATH bhi update karo agar today's high > historical max.
    """
    if not today_data:
        log("  No today's quote data to apply.")
        return close, high, volume, ath_dict

    today_ts = pd.Timestamp(today)

    # Check karo ki today already index mein hai (shouldn't be, but safety)
    if today_ts in close.index:
        log(f"  Today ({today}) already in index — skipping quote append")
        return close, high, volume, ath_dict

    # Build today's rows — only for symbols present in our DataFrames
    close_today = {}
    high_today  = {}
    vol_today   = {}

    for sym, q in today_data.items():
        if sym in close.columns:
            close_today[sym] = q["close"]
        if sym in high.columns:
            high_today[sym]  = q["high"]
        if sym in volume.columns:
            vol_today[sym]   = q["close"] * q["volume"]   # value of volume traded

        # Update ATH
        if sym in ath_dict and q["high"] > ath_dict[sym]:
            ath_dict[sym] = q["high"]

    if not close_today:
        log("  Today's data has no overlap with cache columns — skipping")
        return close, high, volume, ath_dict

    # Create single-row DataFrames for today
    new_row_close  = pd.DataFrame(close_today, index=[today_ts])
    new_row_high   = pd.DataFrame(high_today,  index=[today_ts])
    new_row_volume = pd.DataFrame(vol_today,   index=[today_ts])

    # Reindex to match existing columns (fills NaN for symbols without data)
    new_row_close  = new_row_close.reindex(columns=close.columns)
    new_row_high   = new_row_high.reindex(columns=high.columns)
    new_row_volume = new_row_volume.reindex(columns=volume.columns)

    # Concat
    close  = pd.concat([close,  new_row_close],  axis=0).sort_index()
    high   = pd.concat([high,   new_row_high],   axis=0).sort_index()
    volume = pd.concat([volume, new_row_volume], axis=0).sort_index()

    log(f"  ✅ Today ({today}) appended: {len(close_today)} close | {len(high_today)} high | {len(vol_today)} volume values")
    return close, high, volume, ath_dict


# ── Sequential bulk fetch — 3 decades per symbol ─────────────
def fetch_all_sequential(
    symbols        : list,
    instrument_map : dict,
    access_token   : str,
    decade_ranges  : list,
    end_date       : datetime,
) -> tuple[dict, dict, dict, dict, list]:
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
            decade_dfs    = []
            token_expired = False

            for from_d, to_d in decade_ranges:
                try:
                    df = _fetch_one_decade(instrument_key, access_token, from_d, to_d)
                    if df is not None and not df.empty:
                        decade_dfs.append(df)
                except ValueError:
                    log("Token expired mid-download — stopping.")
                    token_expired = True
                    break
                except Exception:
                    pass

                time.sleep(0.05)

            if token_expired:
                raise RuntimeError(
                    "Upstox token expired mid-download. Re-run to get a fresh token."
                )

            if decade_dfs:
                merged = pd.concat(decade_dfs).sort_index()
                merged = merged[~merged.index.duplicated(keep="last")]

                ath_dict[sym] = float(merged["high"].max())

                df_r = merged[merged.index >= start_recent]
                if not df_r.empty:
                    idx = pd.to_datetime(df_r.index)
                    close_map[sym] = pd.Series(df_r["close"].values,                  index=idx)
                    high_map[sym]  = pd.Series(df_r["high"].values,                   index=idx)
                    vol_map[sym]   = pd.Series((df_r["close"]*df_r["volume"]).values, index=idx)
            else:
                failed.append(sym)

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


def verify_data_freshness(close: pd.DataFrame, today: date) -> bool:
    if close.empty:
        return False
    last_date = close.index[-1].date()
    log(f"  Last date in cache : {last_date}")
    log(f"  Today's date       : {today}")
    if last_date >= today:
        log(f"  ✅ TODAY'S DATA CONFIRMED in cache ({last_date})")
        return True
    else:
        log(f"  ⚠️  Today's data ({today}) NOT in cache — last: {last_date}")
        log(f"     Possible reason: holiday/weekend, or all-zero volume (market holiday)")
        return False


# ── Main ──────────────────────────────────────────────────────
def build_cache():
    log("=" * 58)
    log("MOMN CACHE BUILDER — UPSTOX VERSION")
    log("=" * 58)
    CACHE_DIR.mkdir(exist_ok=True)
    t_total = time.monotonic()

    # 1. Auth
    log("Authenticating with Upstox...")
    access_token = get_token_from_env()
    log(f"  Token received: {_mask(access_token)} ✅")

    # 2. Symbols
    symbols = load_symbols()

    # 3. Instrument master
    instrument_map = load_instrument_map()

    # 4. Build decade ranges
    today        = date.today()
    today_str    = today.strftime("%Y-%m-%d")
    tomorrow_str = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    decade_ranges = [
        ("2000-01-01", "2009-12-31"),
        ("2010-01-01", "2019-12-31"),
        ("2020-01-01", tomorrow_str),
    ]
    end_date = datetime.combine(today, datetime.min.time())

    log(f"Today              : {today_str}")
    log(f"Decade ranges      : {[f'{f}→{t}' for f,t in decade_ranges]}")
    log(f"NOTE: Historical API has T+1 delay — today's data via Market Quote API")

    # 5. Historical fetch (up to yesterday)
    ath_dict, close_map, high_map, vol_map, failed = fetch_all_sequential(
        symbols, instrument_map, access_token, decade_ranges, end_date
    )

    # 6. Assemble DataFrames
    log("Assembling DataFrames...")
    start_recent = end_date - relativedelta(months=RECENT_MONTHS)

    def _make_df(data_map):
        if not data_map:
            return pd.DataFrame()
        df = pd.DataFrame(data_map)
        df = df.sort_index()
        df = df.dropna(how='all')
        return df.loc[:, ~df.columns.duplicated()]

    close  = _make_df(close_map)
    high   = _make_df(high_map)
    volume = _make_df(vol_map)

    log(f"  After historical fetch: close {close.shape} | high {high.shape} | vol {volume.shape}")

    if close.empty:
        log("ERROR: close DataFrame empty after historical fetch")
        sys.exit(1)

    # 7. TODAY's data via Market Quote API ──────────────────────
    # Upstox historical API has T+1 delay. Today's candle is NOT
    # available in /days/1/ endpoint on the same day.
    # We fetch today's OHLCV via Market Quote API instead.
    log("=" * 40)
    log("Fetching TODAY's data via Market Quote API...")
    log(f"  (Historical API ends at: {close.index[-1].date()})")
    log("=" * 40)

    today_data = {}
    try:
        today_data = _fetch_today_quotes(symbols, instrument_map, access_token, today)
    except ValueError as e:
        log(f"  ⚠️  Market quote fetch: token error — {e}")
    except Exception as e:
        log(f"  ⚠️  Market quote fetch failed: {type(e).__name__}: {e}")
        log(f"  Proceeding with historical data only.")

    if today_data:
        close, high, volume, ath_dict = _apply_today_quotes(
            close, high, volume, ath_dict, today_data, today
        )
    else:
        log(f"  No today's data applied (weekend / holiday / API issue).")

    # Rebuild ath_df after potential ATH updates
    ath_df = pd.Series(ath_dict, name="ATH", dtype=float).to_frame()

    log(f"  Final: close {close.shape} | high {high.shape} | vol {volume.shape} | ath {ath_df.shape}")

    # 8. Data freshness check
    log("Verifying data freshness...")
    today_present      = verify_data_freshness(close, today)
    last_date_in_cache = close.index[-1].date() if not close.empty else None

    # 9. Save Parquet
    log("Saving Parquet files...")
    close.to_parquet(CACHE_DIR  / "close.parquet")
    high.to_parquet(CACHE_DIR   / "high.parquet")
    volume.to_parquet(CACHE_DIR / "volume.parquet")
    ath_df.to_parquet(CACHE_DIR / "ath.parquet")

    for fname in ["close.parquet", "high.parquet", "volume.parquet", "ath.parquet"]:
        mb = (CACHE_DIR / fname).stat().st_size / 1_048_576
        log(f"  {fname}: {mb:.1f} MB")

    # 10. Meta JSON
    total_min = (time.monotonic() - t_total) / 60
    meta = {
        "build_date"                 : today.isoformat(),
        "build_time_utc"             : datetime.utcnow().strftime("%H:%M:%S"),
        "build_duration_min"         : round(total_min, 1),
        "symbols_total"              : len(symbols),
        "symbols_fetched"            : len(ath_dict),
        "symbols_failed"             : len([f for f in failed if f not in
                                            [s for s in symbols if not _get_key(s, instrument_map)]]),
        "not_in_master"              : len(symbols) - len([s for s in symbols
                                                           if _get_key(s, instrument_map)]),
        "failed_symbols"             : sorted(failed),
        "data_start_full"            : "2000-01-01",
        "data_start_recent"          : start_recent.strftime("%Y-%m-%d"),
        "data_end"                   : today_str,
        "last_date_in_cache"         : str(last_date_in_cache),
        "today_data_present"         : today_present,
        "today_quotes_fetched"       : len(today_data),
        "recent_months"              : RECENT_MONTHS,
        "source"                     : "Upstox V3 (historical candle + market quote for today)",
        "today_data_source"          : "Upstox Market Quote API (/v3/market-quote/quotes)",
        "upstox_to_date"             : tomorrow_str,
        "symbol_source"              : "NSE EQUITY_L.csv (direct)",
        "extra_symbols"              : EXTRA_SYMBOLS,
        "decades_per_symbol"         : 3,
        "decade_ranges"              : decade_ranges,
        "total_api_calls_historical" : len(ath_dict) * 3,
        "total_api_calls_quotes"     : (len(symbols) + _QUOTE_BATCH_SIZE - 1) // _QUOTE_BATCH_SIZE,
        "close_shape"                : list(close.shape),
        "high_shape"                 : list(high.shape),
        "volume_shape"               : list(volume.shape),
        "ath_count"                  : len(ath_df),
    }
    with open(CACHE_DIR / "cache_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    log("=" * 58)
    log("✅ UPSTOX CACHE BUILD COMPLETE")
    log(f"   Symbols       : {meta['symbols_fetched']}/{meta['symbols_total']} fetched")
    log(f"   Today quotes  : {meta['today_quotes_fetched']} symbols")
    log(f"   Last date     : {last_date_in_cache}")
    log(f"   Today present : {'✅ YES' if today_present else '⚠️  NO (holiday/weekend?)'}")
    log(f"   Time          : {total_min:.1f} min")
    log("=" * 58)


if __name__ == "__main__":
    build_cache()
