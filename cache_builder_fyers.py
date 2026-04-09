"""
cache_builder_fyers.py
======================
Fyers API v3 se full history cache build karta hai.
GitHub Actions pe daily 10:30 PM IST pe chalta hai.

Key design:
  • fyers.history() — resolution "D", date_format "1"
  • 1D resolution: max 366 days per call
  • Data available from: 2017-07-03 (Fyers server limit)
  • Full history: Jul 2017 → today = ~9 years = ~10 chunks per symbol
  • Symbol format: NSE:RELIANCE-EQ (NOT RELIANCE.NS)
  • Rate limit: not officially documented, conservative 0.2s sleep = ~5 req/sec
  • ATH = max from 2017-07-03 to today (Fyers CANNOT provide pre-2017 data)
  • Recent 40M = slice from merged data → close/high/volume parquet
  • Cache dir: cache_fyers/ (alag folder!)

IMPORTANT LIMITATION:
  Fyers historical data starts from 2017-07-03 ONLY.
  Upstox/Angel One = 2000 se data. Fyers = 2017 se.
  ATH ke liye: stocks jinka ATH 2017 se pehle tha, unka ATH
  thoda understated hoga. Screener accuracy ke liye Upstox ya
  Angel One cache prefer karein agar 2000-se-ATH chahiye.

SECURITY: client_id, secret_id, TOTP secret kabhi log nahi hote.

GitHub Secrets required:
  FYERS_APP_ID      — e.g., "XY12345-100"
  FYERS_SECRET_ID   — secret key from myapi.fyers.in
  FYERS_USERNAME    — Fyers login ID (e.g., XY12345)
  FYERS_PIN         — 4-digit Fyers PIN
  FYERS_TOTP_SECRET — TOTP secret from Fyers (Enable TOTP section)
  FYERS_REDIRECT_URI — registered redirect URI (e.g., https://127.0.0.1)

No static IP required — Fyers uses OAuth, not IP whitelisting.
"""

import hashlib
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

# ── Try fyers_apiv3 import ─────────────────────────────────────
try:
    from fyers_apiv3 import fyersModel
    from fyers_apiv3.fyersModel import SessionModel
    import pyotp
except ImportError:
    print("ERROR: fyers-apiv3 / pyotp not installed.", flush=True)
    print("  pip install fyers-apiv3 pyotp", flush=True)
    sys.exit(1)

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
CACHE_DIR      = Path("cache_fyers")
RECENT_MONTHS  = 40
EXTRA_SYMBOLS  = ["GOLDBEES.NS", "SILVERBEES.NS"]
NSE_EQUITY_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
GITHUB_BASE    = "https://raw.githubusercontent.com/prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main"

# Fyers: 1D resolution = max 366 days per call
MAX_DAYS_PER_CALL = 366

# Fyers data available from: 2017-07-03
FYERS_DATA_START = "2017-07-03"

# Conservative sleep: ~5 req/sec (Fyers rate limit not officially stated)
RATE_LIMIT_SLEEP = 0.2

# Automated auth endpoints (no browser needed for GitHub Actions)
_BASE_URL    = "https://api-t2.fyers.in/vagator/v2"
_BASE_URL_2  = "https://api-t1.fyers.in/api/v3"
_URL_OTP     = _BASE_URL   + "/send_login_otp"
_URL_TOTP    = _BASE_URL   + "/verify_otp"
_URL_PIN     = _BASE_URL   + "/verify_pin"
_URL_TOKEN   = _BASE_URL_2 + "/token"
_URL_AUTHCODE= _BASE_URL_2 + "/validate-authcode"


# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def _mask(s: str) -> str:
    if not s: return "***"
    return s[:3] + "*" * max(0, len(s) - 5) + s[-2:]


# ══════════════════════════════════════════════════════════════
# AUTOMATED TOTP AUTH (no browser — for GitHub Actions)
# ══════════════════════════════════════════════════════════════
def get_fyers_client_automated() -> "fyersModel.FyersModel":
    """
    Fully automated Fyers auth using TOTP — no browser needed.
    Uses Fyers vagator v2 + token API endpoints directly.
    Credentials env vars se aate hain (GitHub Secrets).
    """
    app_id       = os.environ.get("FYERS_APP_ID",       "").strip()
    secret_id    = os.environ.get("FYERS_SECRET_ID",    "").strip()
    username     = os.environ.get("FYERS_USERNAME",     "").strip()
    pin          = os.environ.get("FYERS_PIN",          "").strip()
    totp_secret  = os.environ.get("FYERS_TOTP_SECRET",  "").strip()
    redirect_uri = os.environ.get("FYERS_REDIRECT_URI", "https://127.0.0.1").strip()

    for var, val in [
        ("FYERS_APP_ID",    app_id),
        ("FYERS_SECRET_ID", secret_id),
        ("FYERS_USERNAME",  username),
        ("FYERS_PIN",       pin),
        ("FYERS_TOTP_SECRET", totp_secret),
    ]:
        if not val:
            raise RuntimeError(
                f"Missing GitHub Secret: {var}. "
                "Repo Settings → Secrets and variables → Actions mein add karo."
            )

    log(f"  App ID: {_mask(app_id)} | Username: {_mask(username)}")

    headers = {"Content-Type": "application/json"}

    # ── Step 1: Send login OTP ────────────────────────────────
    r1 = requests.post(
        _URL_OTP,
        json={"fy_id": username, "app_id": "2"},
        headers=headers, timeout=15
    )
    r1.raise_for_status()
    d1 = r1.json()
    if d1.get("s") not in ("ok", "OK") and d1.get("code", 0) not in (200, 0):
        raise RuntimeError(f"Step 1 (OTP) failed: {d1}")
    request_key_1 = d1.get("request_key", "")
    log("  Step 1 (send OTP) ✅")

    # ── Step 2: Verify TOTP ───────────────────────────────────
    totp_val = pyotp.TOTP(totp_secret).now()
    r2 = requests.post(
        _URL_TOTP,
        json={
            "request_key":   request_key_1,
            "identity_type": "totp",
            "identifier":    totp_val,
        },
        headers=headers, timeout=15
    )
    r2.raise_for_status()
    d2 = r2.json()
    if d2.get("s") not in ("ok", "OK") and d2.get("code", 0) not in (200, 0):
        raise RuntimeError(f"Step 2 (TOTP) failed: {d2}")
    request_key_2 = d2.get("request_key", "")
    log("  Step 2 (verify TOTP) ✅")

    # ── Step 3: Verify PIN ────────────────────────────────────
    sha_pin = hashlib.sha256(pin.encode()).hexdigest()
    r3 = requests.post(
        _URL_PIN,
        json={
            "request_key":   request_key_2,
            "identity_type": "pin",
            "identifier":    sha_pin,
        },
        headers=headers, timeout=15
    )
    r3.raise_for_status()
    d3 = r3.json()
    if d3.get("s") not in ("ok", "OK") and d3.get("code", 0) not in (200, 0):
        raise RuntimeError(f"Step 3 (PIN) failed: {d3}")
    access_token_temp = d3.get("data", {}).get("access_token", "")
    log("  Step 3 (verify PIN) ✅")

    # ── Step 4: Get auth code ─────────────────────────────────
    # Extract numeric app part: "XY12345-100" → app_type="100", app_id_num="XY12345"
    if "-" in app_id:
        _parts   = app_id.split("-")
        app_type = _parts[-1]
        app_id_num = "-".join(_parts[:-1])
    else:
        app_id_num = app_id
        app_type   = "100"

    r4 = requests.post(
        _URL_TOKEN,
        json={
            "fyers_id":      username,
            "app_id":        app_id_num,
            "redirect_uri":  redirect_uri,
            "appType":       app_type,
            "code_challenge":"",
            "state":         "None",
            "scope":         "",
            "nonce":         "",
            "response_type": "code",
            "create_cookie": True,
        },
        headers={**headers, "Authorization": f"Bearer {access_token_temp}"},
        timeout=15,
    )
    r4.raise_for_status()
    d4 = r4.json()
    if d4.get("s") not in ("ok", "OK") and d4.get("code", 0) not in (200, 0):
        raise RuntimeError(f"Step 4 (token) failed: {d4}")
    # Auth code is in the URL: extract from redirect URL
    auth_url = d4.get("Url", "") or d4.get("url", "")
    if "auth_code=" in auth_url:
        auth_code = auth_url.split("auth_code=")[-1].split("&")[0]
    elif "code=" in auth_url:
        auth_code = auth_url.split("code=")[-1].split("&")[0]
    else:
        raise RuntimeError(f"Auth code not found in URL: {auth_url[:100]}")
    log("  Step 4 (get auth code) ✅")

    # ── Step 5: Validate auth code → access token ─────────────
    session = SessionModel(
        client_id    = app_id,
        secret_key   = secret_id,
        redirect_uri = redirect_uri,
        response_type= "code",
        grant_type   = "authorization_code",
    )
    session.set_token(auth_code)
    resp5 = session.generate_token()

    if resp5.get("s") != "ok":
        raise RuntimeError(f"Step 5 (validate) failed: {resp5}")

    raw_token   = resp5["access_token"]
    access_token= f"{app_id}:{raw_token}"
    log(f"  Step 5 (access token): {_mask(raw_token)} ✅")

    # ── Build FyersModel ──────────────────────────────────────
    fyers = fyersModel.FyersModel(
        client_id = app_id,
        token     = access_token,
        is_async  = False,
        log_path  = "",
    )
    log("Fyers FyersModel ready ✅")
    return fyers


# ══════════════════════════════════════════════════════════════
# SYMBOL FORMAT CONVERSION
# ══════════════════════════════════════════════════════════════
def ns_to_fyers(symbol: str) -> str:
    """
    Convert .NS symbol to Fyers format.
    RELIANCE.NS → NSE:RELIANCE-EQ
    GOLDBEES.NS → NSE:GOLDBEES-EQ
    """
    clean = symbol.replace(".NS", "").replace(".BO", "").upper().strip()
    return f"NSE:{clean}-EQ"


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
# DATE CHUNKS (366-day windows from 2017-07-03)
# ══════════════════════════════════════════════════════════════
def get_date_ranges(max_days: int = MAX_DAYS_PER_CALL) -> list[tuple[str, str]]:
    """
    2017-07-03 se aaj tak ke liye 366-day chunks.
    Returns: [("2017-07-03","2018-07-04"), ...]
    """
    ranges  = []
    current = datetime.strptime(FYERS_DATA_START, "%Y-%m-%d").date()
    today   = date.today()

    while current <= today:
        end     = min(current + timedelta(days=max_days - 1), today)
        ranges.append((current.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
        current = end + timedelta(days=1)

    return ranges


# ══════════════════════════════════════════════════════════════
# SINGLE SYMBOL, SINGLE CHUNK FETCH
# ══════════════════════════════════════════════════════════════
def _fetch_one_chunk(
    fyers      : "fyersModel.FyersModel",
    fyers_sym  : str,
    from_date  : str,
    to_date    : str,
    retries    : int = 3,
) -> pd.DataFrame | None:
    """
    Ek 366-day chunk ka daily candle data fetch karo.
    Response: {'s': 'ok', 'candles': [[epoch, o, h, l, c, v], ...]}
    """
    data = {
        "symbol":      fyers_sym,
        "resolution":  "D",
        "date_format": "1",       # "1" = YYYY-MM-DD string format
        "range_from":  from_date,
        "range_to":    to_date,
        "cont_flag":   "1",
    }
    delay = 1.0

    for attempt in range(retries):
        try:
            resp = fyers.history(data=data)

            if not resp or resp.get("s") not in ("ok", "OK"):
                code = resp.get("code", 0) if resp else 0
                msg  = resp.get("message", "unknown") if resp else "no response"

                # Token expired
                if code in (-300, -301) or "token" in str(msg).lower():
                    raise ValueError(f"Token expired: {msg}")

                # Symbol not found / no data — normal for newly listed stocks
                if code in (-100, -101, -50, 10000):
                    return None

                return None  # other errors — skip

            candles = resp.get("candles", [])
            if not candles:
                return None

            # candle = [epoch_seconds, open, high, low, close, volume]
            df = pd.DataFrame(
                candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
            # date_format=1 returns YYYY-MM-DD strings, but epoch too sometimes
            # Handle both:
            if df["timestamp"].dtype == object:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            else:
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

            # Strip timezone if present
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)

            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
            return df[["open", "high", "low", "close", "volume"]].astype(float)

        except ValueError:
            raise  # Token expired — caller handles

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
def fetch_all_sequential(
    symbols     : list,
    fyers       : "fyersModel.FyersModel",
    date_ranges : list[tuple[str, str]],
    end_date    : datetime,
) -> tuple[dict, dict, dict, dict, list]:
    """
    Sequential fetch — same pattern as Upstox & Angel One builders.

    for each symbol:
        for each 366-day chunk (~10 chunks from 2017):
            _fetch_one_chunk()
            time.sleep(0.2)   ← ~5 req/sec

    Total calls: ~2000 symbols × 10 chunks = ~20,000 calls
    ETA @ 5 req/sec: ~67 minutes (within GitHub Actions 6hr limit)

    Returns: ath_dict, close_map, high_map, vol_map, failed
    """
    start_recent = end_date - relativedelta(months=RECENT_MONTHS)
    total        = len(symbols)
    not_mapped   = 0
    ath_dict     = {}
    close_map    = {}
    high_map     = {}
    vol_map      = {}
    failed       = []
    t0           = time.monotonic()

    log(f"Starting: {total:,} symbols × {len(date_ranges)} chunks ≈ {total*len(date_ranges):,} calls")
    log(f"Data from: {FYERS_DATA_START} (Fyers limit) | Rate: ~5 req/sec")
    log(f"ETA: ~{(total * len(date_ranges) / 5) / 60:.0f} minutes")

    for i, sym in enumerate(symbols):
        fyers_sym = ns_to_fyers(sym)   # RELIANCE.NS → NSE:RELIANCE-EQ
        chunk_dfs = []
        tok_err   = False

        for from_d, to_d in date_ranges:
            try:
                df = _fetch_one_chunk(fyers, fyers_sym, from_d, to_d)
                if df is not None and not df.empty:
                    chunk_dfs.append(df)
            except ValueError:
                log(f"Fyers token expired at symbol {i+1}/{total} — stopping.")
                tok_err = True
                break
            except Exception:
                pass
            time.sleep(RATE_LIMIT_SLEEP)

        if tok_err:
            raise RuntimeError(
                "Fyers token expired mid-download. "
                "Re-run the GitHub Actions workflow."
            )

        if chunk_dfs:
            merged = pd.concat(chunk_dfs).sort_index()
            merged = merged[~merged.index.duplicated(keep="last")]

            ath_dict[sym] = float(merged["high"].max())

            df_r = merged[merged.index >= start_recent]
            if not df_r.empty:
                idx = pd.to_datetime(df_r.index)
                close_map[sym] = pd.Series(df_r["close"].values, index=idx)
                high_map[sym]  = pd.Series(df_r["high"].values,  index=idx)
                vol_map[sym]   = pd.Series(
                    (df_r["close"] * df_r["volume"]).values, index=idx
                )
        else:
            failed.append(sym)

        if i % 50 == 0 or i == total - 1:
            elapsed   = time.monotonic() - t0
            remaining = (total - i - 1) * (elapsed / max(i + 1, 1))
            log(
                f"  [{i+1}/{total}] {int((i+1)/total*100)}% | "
                f"✅ {len(ath_dict)} | ❌ {len(failed)} | "
                f"ETA: {remaining/60:.1f}min"
            )

    log(
        f"Fetch complete: {len(ath_dict)}/{total} | "
        f"Failed: {len(failed)} | Time: {(time.monotonic()-t0)/60:.1f}min"
    )
    return ath_dict, close_map, high_map, vol_map, failed


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def build_cache():
    log("=" * 58)
    log("MOMN CACHE BUILDER — FYERS VERSION")
    log("=" * 58)
    CACHE_DIR.mkdir(exist_ok=True)
    t_total = time.monotonic()

    # 1. Auth (fully automated — no browser)
    log("Authenticating with Fyers API v3 (automated TOTP)...")
    fyers = get_fyers_client_automated()

    # 2. Symbols
    symbols = load_symbols()

    # 3. Date ranges (366-day chunks from 2017-07-03)
    end_date    = datetime.combine(date.today(), datetime.min.time())
    today_str   = end_date.strftime("%Y-%m-%d")
    date_ranges = get_date_ranges(MAX_DAYS_PER_CALL)
    log(f"Date chunks ({len(date_ranges)} total): {date_ranges[0][0]} → {date_ranges[-1][1]}")
    log(f"NOTE: Fyers data starts {FYERS_DATA_START} (pre-2017 ATH not available)")

    # 4. Fetch
    ath_dict, close_map, high_map, vol_map, failed = fetch_all_sequential(
        symbols, fyers, date_ranges, end_date
    )

    # 5. Assemble DataFrames
    log("Assembling DataFrames...")
    start_recent = end_date - relativedelta(months=RECENT_MONTHS)

    def _make_df(data_map):
        if not data_map:
            return pd.DataFrame()
        df = pd.DataFrame(data_map).sort_index().dropna(how="all")
        return df.loc[:, ~df.columns.duplicated()]

    close  = _make_df(close_map)
    high   = _make_df(high_map)
    volume = _make_df(vol_map)
    ath_df = pd.Series(ath_dict, name="ATH", dtype=float).to_frame()

    log(f"  close: {close.shape} | high: {high.shape} | vol: {volume.shape} | ath: {ath_df.shape}")

    if close.empty:
        log("ERROR: close DataFrame empty — check auth/tokens")
        sys.exit(1)

    # ROOT CAUSE FIX — trailing NaN rows
    close  = close.sort_index().dropna(how="all").ffill()
    volume = volume.sort_index().dropna(how="all").ffill()
    high   = high.sort_index()

    # 6. Save Parquet
    log("Saving Parquet files to cache_fyers/...")
    close.to_parquet(CACHE_DIR  / "close.parquet")
    high.to_parquet(CACHE_DIR   / "high.parquet")
    volume.to_parquet(CACHE_DIR / "volume.parquet")
    ath_df.to_parquet(CACHE_DIR / "ath.parquet")

    for fname in ["close.parquet", "high.parquet", "volume.parquet", "ath.parquet"]:
        mb = (CACHE_DIR / fname).stat().st_size / 1_048_576
        log(f"  {fname}: {mb:.1f} MB")

    # 7. Meta JSON
    total_min = (time.monotonic() - t_total) / 60
    meta = {
        "build_date"          : date.today().isoformat(),
        "build_time_utc"      : datetime.utcnow().strftime("%H:%M:%S"),
        "build_duration_min"  : round(total_min, 1),
        "symbols_total"       : len(symbols),
        "symbols_fetched"     : len(ath_dict),
        "symbols_failed"      : len(failed),
        "failed_symbols"      : sorted(failed),
        "data_start_full"     : FYERS_DATA_START,
        "data_start_recent"   : start_recent.strftime("%Y-%m-%d"),
        "data_end"            : today_str,
        "recent_months"       : RECENT_MONTHS,
        "source"              : "Fyers API v3 (1D candles, data from 2017-07-03)",
        "symbol_source"       : "NSE EQUITY_L.csv",
        "extra_symbols"       : EXTRA_SYMBOLS,
        "chunks_per_symbol"   : len(date_ranges),
        "max_days_per_chunk"  : MAX_DAYS_PER_CALL,
        "date_ranges"         : date_ranges,
        "total_api_calls_est" : len(ath_dict) * len(date_ranges),
        "rate_limit"          : "~5 req/sec (0.2s sleep)",
        "ath_note"            : "ATH only from 2017-07-03 (Fyers server limit)",
        "close_shape"         : list(close.shape),
        "high_shape"          : list(high.shape),
        "volume_shape"        : list(volume.shape),
        "ath_count"           : len(ath_df),
    }
    with open(CACHE_DIR / "cache_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    log("=" * 58)
    log("✅ FYERS CACHE BUILD COMPLETE")
    log(f"   Symbols  : {meta['symbols_fetched']}/{meta['symbols_total']} fetched")
    log(f"   API calls: ~{meta['total_api_calls_est']:,}")
    log(f"   Time     : {total_min:.1f} min")
    log(f"   ⚠️ ATH data from {FYERS_DATA_START} (pre-2017 ATH not available)")
    log("=" * 58)


if __name__ == "__main__":
    build_cache()
