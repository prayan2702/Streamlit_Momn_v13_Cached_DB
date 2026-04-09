"""
cache_builder_angelone.py
=========================
Angel One SmartAPI V2 se full history cache build karta hai.
GitHub Actions pe daily 9:30 PM IST pe chalta hai.

Key design:
  • SmartAPI getCandleData (ONE_DAY interval) = max 2000 days per call
  • Full history from 2000-01-01 → ~5 API calls per symbol
  • Rate limit: 3 req/sec → sleep 0.34s between calls
  • ATH = concat(all chunks).high.max() → correct 2000-to-today ATH
  • Recent 40M = slice from merged data → close/high/volume parquet
  • Cache dir: cache_angelone/ (alag folder — Upstox/YFinance se alag!)
  • Session refresh: har SESSION_REFRESH_EVERY symbols pe auto-refresh
    (Angel One sessions ~45-60 min baad expire hoti hain)
  • Consecutive failure guard: 10+ symbols fail → immediate session refresh

SECURITY: API key, password, TOTP secret kabhi log nahi hote.

GitHub Secrets required:
  ANGELONE_CLIENT_ID    — your Angel One client ID (R/T number)
  ANGELONE_API_KEY      — SmartAPI app ka API key
  ANGELONE_PASSWORD     — login password
  ANGELONE_TOTP_SECRET  — TOTP authenticator secret (Enable TOTP pe milta hai)

Static IP note:
  SmartAPI Add App mein "Primary Static IP" required hai.
  GitHub Actions ka IP dynamic hota hai.
  FREE SOLUTION: SmartAPI app mein "127.0.0.1" enter karo.
  Historical data endpoints (getCandleData) IP-check enforce nahi karte —
  sirf order placement ke liye strict hai. Data-only apps ke liye koi issue nahi.
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

# ── Try SmartApi import ────────────────────────────────────────
try:
    from SmartApi import SmartConnect
    import pyotp
except ImportError:
    print("ERROR: SmartApi/pyotp not installed.", flush=True)
    print("  pip install smartapi-python pyotp", flush=True)
    sys.exit(1)

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
CACHE_DIR      = Path("cache_angelone")   # ← Separate folder
RECENT_MONTHS  = 40                       # Same as Upstox — calculations.py compatible
EXTRA_SYMBOLS  = ["GOLDBEES.NS", "SILVERBEES.NS"]
NSE_EQUITY_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
GITHUB_BASE    = "https://raw.githubusercontent.com/prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main"

# Angel One SmartAPI — ONE_DAY candle max days per call = 2000
MAX_DAYS_PER_CALL = 2000
RATE_LIMIT_SLEEP  = 0.34   # 1/3 sec = 3 req/sec (Angel One limit)

# Session management
# Angel One sessions expire ~45-60 min baad. 200 symbols @ ~5 chunks x 0.34s = ~5.6 min/200
# Proactive refresh har 200 symbols pe (well within 45 min window)
SESSION_REFRESH_EVERY  = 200
# Agar 10 consecutive symbols fail ho jaayein -> session likely dead -> turant refresh
CONSECUTIVE_FAIL_LIMIT = 10

# Scripmaster URL (Angel One instrument master)
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
    """
    GitHub Secrets se credentials lo + SmartAPI session banao.
    Credentials kabhi log nahi hote.
    """
    client_id   = os.environ.get("ANGELONE_CLIENT_ID", "").strip()
    api_key     = os.environ.get("ANGELONE_API_KEY",   "").strip()
    password    = os.environ.get("ANGELONE_PASSWORD",  "").strip()
    totp_secret = os.environ.get("ANGELONE_TOTP_SECRET", "").strip()

    for var, val in [
        ("ANGELONE_CLIENT_ID",   client_id),
        ("ANGELONE_API_KEY",     api_key),
        ("ANGELONE_PASSWORD",    password),
        ("ANGELONE_TOTP_SECRET", totp_secret),
    ]:
        if not val:
            raise RuntimeError(
                f"Missing GitHub Secret: {var}. "
                "Repo Settings → Secrets and variables → Actions mein add karo."
            )

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
            msg = data.get("message", "Unknown error")
            raise ValueError(f"SmartAPI session failed: {msg}")
        log("  Angel One session created ✅")
        return smart_api, creds   # creds returned for mid-run session refresh
    except Exception as e:
        raise RuntimeError(f"Angel One auth failed: {e}") from None


# ══════════════════════════════════════════════════════════════
# SYMBOL LOADING
# ══════════════════════════════════════════════════════════════
def load_symbols() -> list:
    """NSE EQUITY_L.csv + GOLDBEES + SILVERBEES."""
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
# INSTRUMENT MAP (Angel One Scripmaster)
# ══════════════════════════════════════════════════════════════
def load_instrument_map() -> dict:
    """
    Angel One scripmaster se NSE EQ token map banao.
    Returns: { "RELIANCE": "2885", "TCS": "11536", ... }
    """
    log("Loading Angel One scripmaster (NSE EQ instruments)...")
    try:
        resp = requests.get(SCRIPMASTER_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        df   = pd.DataFrame(data)

        # NSE EQ only — both "NSE" exchange and "EQ" instrument type
        mask = (
            (df["exch_seg"].str.upper() == "NSE") &
            (df["instrumenttype"].str.upper().isin(["", "EQ", "-"]) |
             df["symbol"].str.upper().str.endswith("-EQ"))
        )
        df_nse = df[mask].copy()

        # tradingsymbol → token
        df_nse["tradingsymbol"] = df_nse["symbol"].str.upper().str.replace("-EQ", "", regex=False)
        mapping = dict(zip(df_nse["tradingsymbol"], df_nse["token"].astype(str)))
        log(f"  {len(mapping):,} NSE EQ instruments loaded")
        return mapping

    except Exception as e:
        raise RuntimeError(f"Scripmaster load failed: {e}") from None


def _get_token(symbol: str, instrument_map: dict) -> str | None:
    """RELIANCE.NS → token string, or None if not found."""
    clean = symbol.replace(".NS", "").replace(".BO", "").upper().strip()
    return instrument_map.get(clean)


# ══════════════════════════════════════════════════════════════
# DATE RANGE CHUNKS (2000-day windows)
# ══════════════════════════════════════════════════════════════
def get_date_ranges(
    start_str: str = "2000-01-01",
    max_days: int = MAX_DAYS_PER_CALL,
) -> list[tuple[str, str]]:
    """
    2000-01-01 se aaj tak ke liye 2000-day chunks banao.
    Example: [("2000-01-01","2005-06-28"), ("2005-06-29","2010-12-25"), ...]
    Returns list of (from_date, to_date) strings "YYYY-MM-DD".
    """
    ranges  = []
    current = datetime.strptime(start_str, "%Y-%m-%d").date()
    today   = date.today()

    while current <= today:
        end     = min(current + timedelta(days=max_days - 1), today)
        ranges.append((current.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
        current = end + timedelta(days=1)

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
    """
    Ek 2000-day chunk ka daily candle data fetch karo.
    Timestamps: "YYYY-MM-DD 09:15" format (SmartAPI requirement).
    Rate limit: caller ke paas sleep hoga (0.34s between calls).
    """
    historic_param = {
        "exchange":    "NSE",
        "symboltoken": token,
        "interval":    "ONE_DAY",
        "fromdate":    f"{from_date} 09:15",
        "todate":      f"{to_date} 15:30",
    }
    delay = 1.0

    for attempt in range(retries):
        try:
            resp = smart_api.getCandleData(historic_param)

            if not resp or resp.get("status") is False:
                err = resp.get("message", "No data") if resp else "No response"
                # Session/auth error keywords — Angel One returns various messages
                _err_lower = err.lower()
                if any(kw in _err_lower for kw in (
                    "token", "session", "unauthorized", "invalid user",
                    "access denied", "jwt", "auth", "login",
                )):
                    raise ValueError(f"Session expired: {err}")
                # No data for this period = normal for newly listed stocks
                return None

            candles = resp.get("data", [])
            if not candles:
                return None

            # candle format: [timestamp_str, open, high, low, close, volume]
            df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Strip timezone if present
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)

            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
            df = df[["open", "high", "low", "close", "volume"]].astype(float)
            return df

        except ValueError:
            raise  # Session error — caller handles

        except Exception:
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                return None

    return None


# ══════════════════════════════════════════════════════════════
# BULK SEQUENTIAL FETCH
# ══════════════════════════════════════════════════════════════
def _refresh_session(creds: dict) -> SmartConnect:
    """
    Fresh Angel One session banao.
    creds = {"api_key", "client_id", "password", "totp_secret"}
    Session expire hone pe fetch_all_sequential call karta hai.
    """
    log("  ♻️  Refreshing Angel One session (proactive / post-failure)...")
    for attempt in range(3):
        try:
            obj      = SmartConnect(api_key=creds["api_key"])
            totp_val = pyotp.TOTP(creds["totp_secret"]).now()
            data     = obj.generateSession(creds["client_id"], creds["password"], totp_val)
            if data.get("status") is False:
                raise ValueError(data.get("message", "Session failed"))
            log("  ✅ Session refreshed successfully")
            return obj
        except Exception as e:
            log(f"  ⚠️  Session refresh attempt {attempt+1}/3 failed: {e}")
            if attempt < 2:
                time.sleep(5)
    raise RuntimeError("Angel One session refresh failed 3 times — aborting.")


def fetch_all_sequential(
    symbols        : list,
    instrument_map : dict,
    smart_api      : SmartConnect,
    date_ranges    : list[tuple[str, str]],
    end_date       : datetime,
    creds          : dict = None,
) -> tuple[dict, dict, dict, dict, list]:
    """
    Sequential fetch — same pattern as cache_builder_upstox.py.

    for each symbol:
        for each 2000-day chunk (5-6 chunks):
            _fetch_one_chunk()
            time.sleep(0.34)   ← 3 req/sec = Angel One limit

    Session refresh strategy (Angel One sessions expire ~45-60 min):
      • Proactive: har SESSION_REFRESH_EVERY (200) symbols pe refresh
      • Reactive:  CONSECUTIVE_FAIL_LIMIT (10) consecutive failures pe refresh
      • Reactive:  ValueError (explicit session error) pe turant refresh

    Total calls: ~2000 symbols × 5 chunks = ~10,000 calls
    ETA @ 3 req/sec: ~55-60 minutes (within GitHub Actions 6hr limit)

    Returns: ath_dict, close_map, high_map, vol_map, failed
    """
    start_recent    = end_date - relativedelta(months=RECENT_MONTHS)
    total           = len(symbols)
    not_found       = 0
    ath_dict        = {}
    close_map       = {}
    high_map        = {}
    vol_map         = {}
    failed          = []
    t0              = time.monotonic()
    consecutive_fail = 0   # consecutive symbol-level failures counter

    log(f"Starting fetch: {total:,} symbols × {len(date_ranges)} chunks = ~{total * len(date_ranges):,} API calls")
    log(f"Rate: 3 req/sec → ETA: ~{(total * len(date_ranges) / 3) / 60:.0f} minutes")
    log(f"Session refresh: every {SESSION_REFRESH_EVERY} symbols | consecutive-fail limit: {CONSECUTIVE_FAIL_LIMIT}")

    for i, sym in enumerate(symbols):

        # ── Proactive session refresh every SESSION_REFRESH_EVERY symbols ──
        if i > 0 and i % SESSION_REFRESH_EVERY == 0 and creds:
            log(f"  [{i}/{total}] Proactive session refresh...")
            try:
                smart_api = _refresh_session(creds)
                consecutive_fail = 0   # reset after successful refresh
            except RuntimeError as e:
                log(f"  ❌ {e}")
                break   # Can't continue without a session

        token = _get_token(sym, instrument_map)

        if not token:
            not_found += 1
            failed.append(sym)
            continue

        chunk_dfs    = []
        session_expired = False

        # ── Fetch each 2000-day chunk ─────────────────────────
        for from_d, to_d in date_ranges:
            try:
                df = _fetch_one_chunk(smart_api, token, from_d, to_d)
                if df is not None and not df.empty:
                    chunk_dfs.append(df)
            except ValueError as e:
                # Explicit session-expired signal from _fetch_one_chunk
                log(f"  ⚠️  Session error detected mid-chunk ({e}) — refreshing...")
                session_expired = True
                break
            except Exception:
                pass  # Is chunk ka data nahi mila — skip

            time.sleep(RATE_LIMIT_SLEEP)   # 3 req/sec

        # ── Reactive refresh on explicit session error ─────────
        if session_expired and creds:
            try:
                smart_api = _refresh_session(creds)
                consecutive_fail = 0
                # Retry current symbol with fresh session
                chunk_dfs = []
                for from_d, to_d in date_ranges:
                    try:
                        df = _fetch_one_chunk(smart_api, token, from_d, to_d)
                        if df is not None and not df.empty:
                            chunk_dfs.append(df)
                    except Exception:
                        pass
                    time.sleep(RATE_LIMIT_SLEEP)
            except RuntimeError as e:
                log(f"  ❌ {e}")
                break

        if chunk_dfs:
            # Merge all chunks
            merged = pd.concat(chunk_dfs).sort_index()
            merged = merged[~merged.index.duplicated(keep="last")]

            # ATH — full 2000-today max
            ath_dict[sym] = float(merged["high"].max())

            # Recent slice — last RECENT_MONTHS only
            df_r = merged[merged.index >= start_recent]
            if not df_r.empty:
                idx = pd.to_datetime(df_r.index)
                close_map[sym] = pd.Series(df_r["close"].values, index=idx)
                high_map[sym]  = pd.Series(df_r["high"].values,  index=idx)
                vol_map[sym]   = pd.Series(
                    (df_r["close"] * df_r["volume"]).values, index=idx
                )
            consecutive_fail = 0   # success → reset counter
        else:
            failed.append(sym)
            consecutive_fail += 1

            # ── Reactive refresh on consecutive failures ───────
            if consecutive_fail >= CONSECUTIVE_FAIL_LIMIT and creds:
                log(
                    f"  ⚠️  {consecutive_fail} consecutive failures at [{i+1}/{total}] "
                    f"— session likely expired. Refreshing + retrying failed batch..."
                )
                # In {consecutive_fail} symbols failed = failed[-consecutive_fail:]
                # Unhe failed list se nikalo aur retry karo
                retry_batch = failed[-consecutive_fail:]
                del failed[-consecutive_fail:]

                try:
                    smart_api = _refresh_session(creds)
                    consecutive_fail = 0

                    # ── Retry each symbol from the failed batch ──
                    log(f"  🔄 Retrying {len(retry_batch)} symbols from failed batch...")
                    retried_ok = 0
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
                    failed.extend(retry_batch)   # wapas failed mein
                    break

        # ── Progress log every 50 symbols ─────────────────────
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


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def build_cache():
    log("=" * 58)
    log("MOMN CACHE BUILDER — ANGEL ONE VERSION")
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

    # 4. Date ranges (2000-day chunks from 2000-01-01 → today)
    end_date    = datetime.combine(date.today(), datetime.min.time())
    today_str   = end_date.strftime("%Y-%m-%d")
    date_ranges = get_date_ranges("2000-01-01", MAX_DAYS_PER_CALL)
    log(f"Date chunks ({len(date_ranges)} total):")
    for r in date_ranges:
        log(f"  {r[0]} → {r[1]}")
    log(f"Total API calls (estimate): {len(symbols)} × {len(date_ranges)} = ~{len(symbols)*len(date_ranges):,}")

    # 5. Sequential fetch
    ath_dict, close_map, high_map, vol_map, failed = fetch_all_sequential(
        symbols, instrument_map, smart_api, date_ranges, end_date,
        creds=creds,   # session refresh ke liye
    )

    # 6. Assemble DataFrames (same pattern as Upstox builder)
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
        log("ERROR: close DataFrame empty — check auth/tokens")
        sys.exit(1)

    # ROOT CAUSE FIX — same as Upstox builder
    # Trailing NaN rows → calculations.py iloc[-1] = NaN → AWAY_ATH blank
    close  = close.sort_index().dropna(how="all").ffill()
    volume = volume.sort_index().dropna(how="all").ffill()
    high   = high.sort_index()

    # 7. Save Parquet
    log("Saving Parquet files to cache_angelone/...")
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
        "build_date"          : date.today().isoformat(),
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
        "recent_months"       : RECENT_MONTHS,
        "source"              : "Angel One SmartAPI V2 (ONE_DAY candles)",
        "symbol_source"       : "NSE EQUITY_L.csv (direct download)",
        "extra_symbols"       : EXTRA_SYMBOLS,
        "chunks_per_symbol"   : len(date_ranges),
        "max_days_per_chunk"  : MAX_DAYS_PER_CALL,
        "date_ranges"         : date_ranges,
        "total_api_calls"     : len(ath_dict) * len(date_ranges),
        "rate_limit"          : "3 req/sec",
        "close_shape"         : list(close.shape),
        "high_shape"          : list(high.shape),
        "volume_shape"        : list(volume.shape),
        "ath_count"           : len(ath_df),
    }
    with open(CACHE_DIR / "cache_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    log("=" * 58)
    log("✅ ANGEL ONE CACHE BUILD COMPLETE")
    log(f"   Symbols  : {meta['symbols_fetched']}/{meta['symbols_total']} fetched")
    log(f"   API calls: ~{meta['total_api_calls']:,}")
    log(f"   Time     : {total_min:.1f} min")
    log("=" * 58)


if __name__ == "__main__":
    build_cache()
