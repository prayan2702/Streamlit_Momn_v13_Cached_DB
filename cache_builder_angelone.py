"""
cache_builder_angelone.py
=========================
Angel One SmartAPI V2 se full history cache build karta hai.
GitHub Actions pe daily 12:47 UTC = 6:17 PM IST pe chalta hai.

── 5-DAY ROLLING CACHE ──────────────────────────────────────
  cache_angelone/
    cache_index.json         ← {"dates": [...], "latest": "YYYY-MM-DD"}
    2026-04-14/
      close.parquet, high.parquet, volume.parquet, ath.parquet, cache_meta.json
    (max 5 dirs — 6th build pe oldest auto-pruned)
──────────────────────────────────────────────────────────────

── BUG FIX: Angel One todate ────────────────────────────────
  `todate = "YYYY-MM-DD 15:30"` — market close time explicitly
  specify karta hai. get_date_ranges() mein upper bound = today+1
  taaki last chunk mein today ka data guaranteed mile.
──────────────────────────────────────────────────────────────

Key design:
  • SmartAPI getCandleData (ONE_DAY interval) = max 2000 days per call
  • Full history from 2000-01-01 → ~5 API calls per symbol
  • Rate limit: 3 req/sec → sleep 0.34s between calls
  • ATH = concat(all chunks).high.max() → correct 2000-to-today ATH
  • Recent 40M = slice from merged data
  • Session refresh: har SESSION_REFRESH_EVERY symbols pe auto-refresh
  • Consecutive failure guard: 10+ fail → session refresh

GitHub Secrets required:
  ANGELONE_CLIENT_ID | ANGELONE_API_KEY | ANGELONE_PASSWORD | ANGELONE_TOTP_SECRET
"""

import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

try:
    from SmartApi import SmartConnect
    import pyotp
except ImportError:
    print("ERROR: SmartApi/pyotp not installed. pip install smartapi-python pyotp", flush=True)
    sys.exit(1)

from cache_rolling import save_rolling_cache, MAX_CACHED_DAYS

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
CACHE_DIR      = Path("cache_angelone")
RECENT_MONTHS  = 40
EXTRA_SYMBOLS  = ["GOLDBEES.NS", "SILVERBEES.NS"]
NSE_EQUITY_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
GITHUB_BASE    = "https://raw.githubusercontent.com/prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main"

MAX_DAYS_PER_CALL      = 2000
RATE_LIMIT_SLEEP       = 0.34
SESSION_REFRESH_EVERY  = 200
CONSECUTIVE_FAIL_LIMIT = 10

SCRIPMASTER_URL = (
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
)


# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def _mask(s: str) -> str:
    if not s:
        return "***"
    return s[:4] + "*" * max(0, len(s) - 6) + s[-2:]


# ══════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════
def get_angelone_client():
    client_id   = os.environ.get("ANGELONE_CLIENT_ID",   "").strip()
    api_key     = os.environ.get("ANGELONE_API_KEY",     "").strip()
    password    = os.environ.get("ANGELONE_PASSWORD",    "").strip()
    totp_secret = os.environ.get("ANGELONE_TOTP_SECRET", "").strip()

    for var, val in [
        ("ANGELONE_CLIENT_ID",   client_id),
        ("ANGELONE_API_KEY",     api_key),
        ("ANGELONE_PASSWORD",    password),
        ("ANGELONE_TOTP_SECRET", totp_secret),
    ]:
        if not val:
            raise RuntimeError(f"Missing GitHub Secret: {var}")

    log(f"  Client ID: {_mask(client_id)} | API Key: {_mask(api_key)}")

    creds = {
        "api_key":     api_key,
        "client_id":   client_id,
        "password":    password,
        "totp_secret": totp_secret,
    }

    try:
        smart_api = SmartConnect(api_key=api_key)
        totp_val  = pyotp.TOTP(totp_secret).now()
        data      = smart_api.generateSession(client_id, password, totp_val)
        if data.get("status") is False:
            raise ValueError(f"SmartAPI session failed: {data.get('message', 'Unknown error')}")
        log("  Angel One session created ✅")
        return smart_api, creds
    except Exception as e:
        raise RuntimeError(f"Angel One auth failed: {e}") from None


def _refresh_session(creds: dict):
    log("  Refreshing Angel One session...")
    try:
        smart_api = SmartConnect(api_key=creds["api_key"])
        totp_val  = pyotp.TOTP(creds["totp_secret"]).now()
        data      = smart_api.generateSession(creds["client_id"], creds["password"], totp_val)
        if data.get("status") is False:
            raise ValueError(data.get("message", "session refresh failed"))
        log("  Session refreshed ✅")
        return smart_api
    except Exception as e:
        raise RuntimeError(f"Session refresh failed: {e}") from None


# ══════════════════════════════════════════════════════════════
# SYMBOL LOADING
# ══════════════════════════════════════════════════════════════
def load_symbols() -> list:
    log("Downloading EQUITY_L.csv from NSE...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0",
        "Referer":    "https://www.nseindia.com/",
        "Accept":     "text/html,*/*",
    }
    try:
        import io as _io
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=15)
        time.sleep(1)
        resp = session.get(NSE_EQUITY_URL, headers=headers, timeout=30)
        resp.raise_for_status()

        df = pd.read_csv(_io.StringIO(resp.text), skipinitialspace=True)
        df.columns = [c.strip() for c in df.columns]
        if "SERIES" in df.columns:
            df = df[df["SERIES"].str.strip() == "EQ"].copy()
        df["SYMBOL"] = df["SYMBOL"].str.strip().str.upper()
        symbols = (df["SYMBOL"] + ".NS").tolist()
        df.to_csv(CACHE_DIR / "EQUITY_L.csv", index=False)
        log(f"  NSE EQUITY_L.csv: {len(symbols):,} EQ stocks")

    except Exception as e:
        log(f"  NSE download failed ({e}) — GitHub fallback...")
        try:
            df_gh   = pd.read_csv(f"{GITHUB_BASE}/NSE_EQ_ALL.csv")
            symbols = (df_gh["Symbol"].astype(str).str.strip() + ".NS").tolist()
            log(f"  GitHub fallback: {len(symbols):,} symbols")
        except Exception as e2:
            raise RuntimeError(f"Symbol load failed: {e2}") from None

    for s in EXTRA_SYMBOLS:
        if s not in symbols:
            symbols.append(s)
    log(f"  Total: {len(symbols):,} (+ GOLDBEES & SILVERBEES)")
    return symbols


# ══════════════════════════════════════════════════════════════
# INSTRUMENT MAP
# ══════════════════════════════════════════════════════════════
def load_instrument_map() -> dict:
    log("Loading Angel One scripmaster (NSE EQ instruments)...")
    try:
        resp = requests.get(SCRIPMASTER_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        df   = pd.DataFrame(data)

        mask = (
            (df["exch_seg"].str.upper() == "NSE") &
            (df["instrumenttype"].str.upper().isin(["", "EQ", "-"]) |
             df["symbol"].str.upper().str.endswith("-EQ"))
        )
        df_nse = df[mask].copy()
        df_nse["tradingsymbol"] = df_nse["symbol"].str.upper().str.replace("-EQ", "", regex=False)
        mapping = dict(zip(df_nse["tradingsymbol"], df_nse["token"].astype(str)))
        log(f"  {len(mapping):,} NSE EQ instruments loaded")
        return mapping

    except Exception as e:
        raise RuntimeError(f"Scripmaster load failed: {e}") from None


def _get_token(symbol: str, instrument_map: dict) -> str | None:
    clean = symbol.replace(".NS", "").replace(".BO", "").upper().strip()
    return instrument_map.get(clean)


# ══════════════════════════════════════════════════════════════
# DATE RANGE CHUNKS (2000-day windows)
# ══════════════════════════════════════════════════════════════
def get_date_ranges(
    start_str : str = "2000-01-01",
    max_days  : int = MAX_DAYS_PER_CALL,
) -> list[tuple[str, str]]:
    """
    2000-01-01 se aaj tak ke liye 2000-day chunks banao.
    FIXED: upper bound = today + 1 day taaki last chunk mein today guaranteed ho.
    """
    ranges  = []
    current = datetime.strptime(start_str, "%Y-%m-%d").date()
    upper_bound = date.today() + timedelta(days=1)

    while current <= upper_bound:
        end_d = min(current + timedelta(days=max_days - 1), upper_bound)
        ranges.append((current.strftime("%Y-%m-%d"), end_d.strftime("%Y-%m-%d")))
        current = end_d + timedelta(days=1)

    return ranges


# ══════════════════════════════════════════════════════════════
# SINGLE FETCH (one symbol, one 2000-day chunk)
# ══════════════════════════════════════════════════════════════
def _fetch_one_chunk(
    smart_api : SmartConnect,
    token     : str,
    from_date : str,
    to_date   : str,
    retries   : int = 3,
) -> pd.DataFrame | None:
    """Ek 2000-day chunk ka daily candle data fetch karo."""
    historic_param = {
        "exchange":    "NSE",
        "symboltoken": token,
        "interval":    "ONE_DAY",
        "fromdate":    f"{from_date} 09:15",
        "todate":      f"{to_date} 15:30",
    }
    delay = 1.0

    _RATE_LIMIT_KEYWORDS = ("rate", "limit", "exceed", "too many", "throttl", "access denied", "session")
    _SESSION_KEYWORDS    = ("invalid token", "session", "auth", "unauthorized", "expired")

    for attempt in range(retries):
        try:
            resp = smart_api.getCandleData(historic_param)

            if not resp or resp.get("status") is False:
                err = resp.get("message", "No data") if resp else "No response"
                _err_lower = err.lower()
                if any(kw in _err_lower for kw in _SESSION_KEYWORDS):
                    raise ValueError(f"Session error: {err}")
                if any(kw in _err_lower for kw in _RATE_LIMIT_KEYWORDS):
                    time.sleep(delay * (2 ** attempt))
                    continue
                return None

            raw_data = resp.get("data", [])
            if not raw_data:
                return None

            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df = pd.DataFrame(raw_data, columns=columns)
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            return df[['open', 'high', 'low', 'close', 'volume']]

        except ValueError:
            raise
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay * (2 ** attempt))

    return None


# ══════════════════════════════════════════════════════════════
# SEQUENTIAL BULK FETCH
# ══════════════════════════════════════════════════════════════
def fetch_all_sequential(
    symbols        : list,
    instrument_map : dict,
    smart_api,
    date_ranges    : list,
    end_date       : datetime,
    creds          : dict | None = None,
) -> tuple[dict, dict, dict, dict, list]:

    start_recent     = end_date - relativedelta(months=RECENT_MONTHS)
    total            = len(symbols)
    not_found        = 0
    ath_dict         = {}
    close_map        = {}
    high_map         = {}
    vol_map          = {}
    failed           = []
    consecutive_fail = 0
    t0               = time.monotonic()

    for i, sym in enumerate(symbols):

        # ── Proactive session refresh ─────────────────────────
        if i > 0 and i % SESSION_REFRESH_EVERY == 0 and creds:
            log(f"  Proactive session refresh at [{i}/{total}]...")
            try:
                smart_api = _refresh_session(creds)
            except RuntimeError as e:
                log(f"  ❌ Session refresh failed: {e}")

        token = _get_token(sym, instrument_map)
        if not token:
            not_found += 1
            failed.append(sym)
        else:
            chunks     = []
            sess_error = False

            for from_d, to_d in date_ranges:
                try:
                    df = _fetch_one_chunk(smart_api, token, from_d, to_d)
                    if df is not None and not df.empty:
                        chunks.append(df)
                except ValueError:
                    sess_error = True
                    break
                except Exception:
                    pass
                time.sleep(RATE_LIMIT_SLEEP)

            if sess_error and creds:
                log(f"  Session error at [{i+1}/{total}] — refreshing...")
                try:
                    smart_api        = _refresh_session(creds)
                    consecutive_fail = 0
                    chunks = []
                    for from_d, to_d in date_ranges:
                        try:
                            df = _fetch_one_chunk(smart_api, token, from_d, to_d)
                            if df is not None and not df.empty:
                                chunks.append(df)
                        except Exception:
                            pass
                        time.sleep(RATE_LIMIT_SLEEP)
                except RuntimeError as e:
                    log(f"  ❌ {e}")

            if chunks:
                merged = pd.concat(chunks).sort_index()
                merged = merged[~merged.index.duplicated(keep="last")]
                ath_dict[sym] = float(merged["high"].max())

                df_r = merged[merged.index >= start_recent]
                if not df_r.empty:
                    idx = pd.to_datetime(df_r.index)
                    close_map[sym] = pd.Series(df_r["close"].values, index=idx)
                    high_map[sym]  = pd.Series(df_r["high"].values,  index=idx)
                    vol_map[sym]   = pd.Series((df_r["close"] * df_r["volume"]).values, index=idx)
                consecutive_fail = 0
            else:
                failed.append(sym)
                consecutive_fail += 1

                if consecutive_fail >= CONSECUTIVE_FAIL_LIMIT and creds:
                    log(
                        f"  ⚠️  {consecutive_fail} consecutive failures at [{i+1}/{total}] "
                        f"— refreshing session + retrying batch..."
                    )
                    retry_batch = failed[-consecutive_fail:]
                    del failed[-consecutive_fail:]

                    try:
                        smart_api        = _refresh_session(creds)
                        consecutive_fail = 0
                        retried_ok       = 0

                        for r_sym in retry_batch:
                            r_token = _get_token(r_sym, instrument_map)
                            if not r_token:
                                failed.append(r_sym)
                                continue
                            r_chunks = []
                            for from_d, to_d in date_ranges:
                                try:
                                    df = _fetch_one_chunk(smart_api, r_token, from_d, to_d)
                                    if df is not None and not df.empty:
                                        r_chunks.append(df)
                                except Exception:
                                    pass
                                time.sleep(RATE_LIMIT_SLEEP)

                            if r_chunks:
                                merged = pd.concat(r_chunks).sort_index()
                                merged = merged[~merged.index.duplicated(keep="last")]
                                ath_dict[r_sym] = float(merged["high"].max())
                                df_r = merged[merged.index >= start_recent]
                                if not df_r.empty:
                                    idx = pd.to_datetime(df_r.index)
                                    close_map[r_sym] = pd.Series(df_r["close"].values, index=idx)
                                    high_map[r_sym]  = pd.Series(df_r["high"].values,  index=idx)
                                    vol_map[r_sym]   = pd.Series(
                                        (df_r["close"] * df_r["volume"]).values, index=idx
                                    )
                                retried_ok += 1
                            else:
                                failed.append(r_sym)

                        log(f"  ✅ Retry complete: {retried_ok}/{len(retry_batch)} recovered")

                    except RuntimeError as e:
                        log(f"  ❌ {e} — {len(retry_batch)} symbols permanently failed")
                        failed.extend(retry_batch)
                        break

        if i % 50 == 0 or i == total - 1:
            elapsed   = time.monotonic() - t0
            remaining = (total - i - 1) * (elapsed / max(i + 1, 1))
            log(
                f"  [{i+1}/{total}] {int((i+1)/total*100)}% | "
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
        log(f"     Possible reason: holiday/weekend, or Angel One data delay")
        return False


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def build_cache():
    log("=" * 58)
    log("MOMN CACHE BUILDER — ANGEL ONE VERSION")
    log(f"Rolling cache: max {MAX_CACHED_DAYS} days stored")
    log("=" * 58)
    CACHE_DIR.mkdir(exist_ok=True)
    t_total = time.monotonic()

    # 1. Auth
    log("Authenticating with Angel One SmartAPI...")
    smart_api, creds = get_angelone_client()

    # 2. Symbols
    symbols = load_symbols()

    # 3. Instrument map
    instrument_map = load_instrument_map()

    # 4. Date ranges
    today      = date.today()
    today_str  = today.strftime("%Y-%m-%d")
    end_date   = datetime.combine(today, datetime.min.time())

    date_ranges = get_date_ranges("2000-01-01", MAX_DAYS_PER_CALL)
    last_chunk  = date_ranges[-1]

    log(f"Today              : {today_str}")
    log(f"Last chunk range   : {last_chunk[0]} → {last_chunk[1]} 15:30 (includes today)")
    log(f"Date chunks        : {len(date_ranges)} total")
    for r in date_ranges:
        log(f"  {r[0]} → {r[1]}")
    log(f"Total API calls (est): {len(symbols)} × {len(date_ranges)} = ~{len(symbols)*len(date_ranges):,}")

    # 5. Fetch
    ath_dict, close_map, high_map, vol_map, failed = fetch_all_sequential(
        symbols, instrument_map, smart_api, date_ranges, end_date, creds=creds,
    )

    # 6. Assemble DataFrames
    log("Assembling DataFrames...")
    start_recent = end_date - relativedelta(months=RECENT_MONTHS)

    def _make_df(data_map):
        if not data_map:
            return pd.DataFrame()
        df = pd.DataFrame(data_map)
        df = df.sort_index()
        df = df.dropna(how="all")
        return df.loc[:, ~df.columns.duplicated()]

    close  = _make_df(close_map)
    high   = _make_df(high_map)
    volume = _make_df(vol_map)
    ath_df = pd.Series(ath_dict, name="ATH", dtype=float).to_frame()

    log(f"  close: {close.shape} | high: {high.shape} | vol: {volume.shape} | ath: {ath_df.shape}")

    if close.empty:
        log("ERROR: close DataFrame empty")
        sys.exit(1)

    close  = close.sort_index().dropna(how="all").ffill()
    volume = volume.sort_index().dropna(how="all").ffill()
    high   = high.sort_index()

    # 7. Freshness check
    log("Verifying data freshness...")
    today_present      = verify_data_freshness(close, today)
    last_date_in_cache = close.index[-1].date() if not close.empty else None

    # 8. Build meta
    total_min = (time.monotonic() - t_total) / 60
    meta = {
        "build_date"          : today_str,
        "build_time_utc"      : datetime.utcnow().strftime("%H:%M:%S"),
        "build_duration_min"  : round(total_min, 1),
        "symbols_total"       : len(symbols),
        "symbols_fetched"     : len(ath_dict),
        "symbols_failed"      : len([f for f in failed if _get_token(f, instrument_map)]),
        "not_in_master"       : len([s for s in symbols if not _get_token(s, instrument_map)]),
        "failed_symbols"      : sorted(failed),
        "data_start_full"     : "2000-01-01",
        "data_start_recent"   : start_recent.strftime("%Y-%m-%d"),
        "data_end"            : today_str,
        "last_date_in_cache"  : str(last_date_in_cache),
        "today_data_present"  : today_present,
        "recent_months"       : RECENT_MONTHS,
        "source"              : "Angel One SmartAPI V2 (ONE_DAY candles)",
        "symbol_source"       : "NSE EQUITY_L.csv (direct)",
        "extra_symbols"       : EXTRA_SYMBOLS,
        "chunks_per_symbol"   : len(date_ranges),
        "max_days_per_chunk"  : MAX_DAYS_PER_CALL,
        "date_ranges"         : date_ranges,
        "last_chunk_todate"   : f"{last_chunk[1]} 15:30",
        "total_api_calls"     : len(ath_dict) * len(date_ranges),
        "rate_limit"          : "3 req/sec",
        "close_shape"         : list(close.shape),
        "high_shape"          : list(high.shape),
        "volume_shape"        : list(volume.shape),
        "ath_count"           : len(ath_df),
    }

    # 9. Rolling dated cache save
    available_dates = save_rolling_cache(
        CACHE_DIR, today_str, close, high, volume, ath_df, meta, log
    )

    log("=" * 58)
    log("✅ ANGEL ONE CACHE BUILD COMPLETE")
    log(f"   Symbols       : {meta['symbols_fetched']}/{meta['symbols_total']} fetched")
    log(f"   API calls     : ~{meta['total_api_calls']:,}")
    log(f"   Last date     : {last_date_in_cache}")
    log(f"   Today present : {'✅ YES' if today_present else '⚠️  NO (holiday/weekend?)'}")
    log(f"   Cached dates  : {available_dates}")
    log(f"   Time          : {total_min:.1f} min")
    log("=" * 58)


if __name__ == "__main__":
    build_cache()
