"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (LIVE) | Angel One (LIVE) | Zerodha (placeholder)

── BUG FIXES: end-date handling ──────────────────────────────
  1. YFinance: `end` parameter is EXCLUSIVE.
     Old: no end → inconsistent; or end=today → only gets yesterday.
     Fix: always pass end = tomorrow explicitly.

  2. Upstox live fetch: API to_date is inclusive but extending
     to tomorrow ensures today's candle is always requested
     (Upstox caps to latest available automatically).

  3. Angel One live fetch: todate = "today 15:30" explicitly
     specifies market close time — already correct, no change.

  4. All fetchers now display `Last trading day: DD-Mon-YYYY`
     so user can see exactly which date's data was fetched.
──────────────────────────────────────────────────────────────

ANGEL ONE SPEED OPTIMIZATION (v2):
  - ThreadPoolExecutor se parallel requests (5 workers)
  - Token Bucket Rate Limiter (3 req/sec strictly enforce)
"""

import time
import threading
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from upstox_auth import get_upstox_access_token
from angelone_auth import get_angelone_client

# ─────────────────────────────────────────────────────────────
# SECTION A — UPSTOX INSTRUMENT MASTER
# ─────────────────────────────────────────────────────────────
_INSTRUMENT_MAP = None

def _load_instrument_map() -> dict:
    global _INSTRUMENT_MAP
    if _INSTRUMENT_MAP is not None:
        return _INSTRUMENT_MAP
    if "upstox_instrument_map" in st.session_state:
        _INSTRUMENT_MAP = st.session_state["upstox_instrument_map"]
        return _INSTRUMENT_MAP

    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    try:
        st.sidebar.info("Downloading Upstox instrument master...")
        df   = pd.read_csv(url, compression="gzip", low_memory=False)
        mask = df["instrument_key"].astype(str).str.startswith("NSE_EQ|")
        df   = df[mask].copy()
        mapping = dict(zip(df["tradingsymbol"].astype(str).str.upper(), df["instrument_key"]))
        _INSTRUMENT_MAP = mapping
        st.session_state["upstox_instrument_map"] = mapping
        st.sidebar.success(f"Instrument master loaded - {len(mapping):,} NSE EQ symbols")
        return mapping
    except Exception as e:
        st.sidebar.error(f"Instrument master load failed: {e}")
        return {}

def _get_instrument_key(symbol_ns: str, instrument_map: dict):
    clean = symbol_ns.replace(".NS", "").replace(".BO", "").upper().strip()
    return instrument_map.get(clean)

# ─────────────────────────────────────────────────────────────
# SECTION B — UPSTOX TOKEN VALIDATION
# ─────────────────────────────────────────────────────────────
def _validate_token(access_token: str) -> bool:
    url = "https://api.upstox.com/v3/historical-candle/NSE_EQ%7CINE002A01018/days/1/2025-01-10/2025-01-01"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        return resp.status_code not in (401, 403)
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────
# SECTION C — UPSTOX SINGLE SYMBOL FETCHER (V3)
# ─────────────────────────────────────────────────────────────
def _fetch_upstox_history_live(
    instrument_key : str,
    access_token   : str,
    start_date     : datetime,
    end_date       : datetime,
    retries        : int = 2
):
    encoded_key   = instrument_key.replace("|", "%7C")
    from_date_str = start_date.strftime("%Y-%m-%d")

    # ── FIX: use tomorrow as to_date ─────────────────────────
    # Upstox to_date is inclusive. Using tomorrow guarantees
    # today's candle is requested — API caps to latest available.
    api_end_date  = end_date + timedelta(days=1)
    to_date_str   = api_end_date.strftime("%Y-%m-%d")

    url = f"https://api.upstox.com/v3/historical-candle/{encoded_key}/days/1/{to_date_str}/{from_date_str}"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    delay = 1.0
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 429:
                time.sleep(delay * 2); delay *= 2; continue
            if resp.status_code in (401, 403):
                raise ValueError(f"Token invalid (HTTP {resp.status_code})")
            resp.raise_for_status()
            payload = resp.json()
            candles = payload.get("data", {}).get("candles", [])

            if not candles:
                return None

            df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
            return df[["open", "high", "low", "close", "volume"]]

        except ValueError:
            raise
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(delay); delay *= 2
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay); delay *= 2
    return None

# ─────────────────────────────────────────────────────────────
# SECTION F — YFINANCE FETCHER
# ─────────────────────────────────────────────────────────────
def _download_yfinance_chunk(symbols, start_date, end_date=None, max_retries=3, delay=2.0):
    """
    yfinance chunk download.

    end_date parameter:
      - yfinance end is EXCLUSIVE (like Python range).
      - Pass end_date = tomorrow to include today's data.
      - If None, defaults to tomorrow automatically.
    """
    if end_date is None:
        # Default: always include today by using tomorrow as exclusive end
        end_date = datetime.now() + timedelta(days=1)

    for attempt in range(max_retries):
        try:
            return yf.download(
                symbols,
                start=start_date,
                end=end_date,           # ← FIXED: tomorrow so today is included
                progress=False,
                auto_adjust=True,
                threads=True,
                multi_level_index=False,
            )
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay); delay *= 2
            else:
                raise e


def fetch_yfinance(symbols, start_date, chunk_size, progress_bar, status_text):
    """
    YFinance bulk fetcher.
    Always fetches up to today (end = tomorrow, exclusive = today included).
    """
    close_chunks, high_chunks, volume_chunks, failed_symbols = [], [], [], []
    total = len(symbols)

    # ── FIX: explicit tomorrow end so today's close is included ──
    end_tomorrow = datetime.now() + timedelta(days=1)

    for k in range(0, total, chunk_size):
        progress = min((k + chunk_size) / total, 1.0)
        chunk    = symbols[k:k + chunk_size]
        for attempt in range(3):
            try:
                raw = _download_yfinance_chunk(chunk, start_date, end_date=end_tomorrow)
                close_chunks.append(raw['Close'])
                high_chunks.append(raw['High'])
                volume_chunks.append(raw['Close'] * raw['Volume'])
                break
            except Exception:
                if attempt == 2:
                    failed_symbols.extend(chunk)
        progress_bar.progress(progress)
        status_text.text(f"YFinance: {int(progress*100)}%")
        time.sleep(1.5)

    progress_bar.progress(1.0)
    close  = pd.concat(close_chunks,  axis=1) if close_chunks  else pd.DataFrame()
    high   = pd.concat(high_chunks,   axis=1) if high_chunks   else pd.DataFrame()
    volume = pd.concat(volume_chunks, axis=1) if volume_chunks else pd.DataFrame()
    for df in (close, high, volume):
        df.index = pd.to_datetime(df.index)

    # ── Show data freshness info ──────────────────────────────
    if not close.empty:
        last_date = close.index[-1].date()
        today     = date.today()
        if last_date >= today:
            status_text.text(f"✅ Data up-to-date | Last trading day: {close.index[-1].strftime('%d-%b-%Y')}")
        else:
            status_text.text(
                f"⚠️ Last trading day: {close.index[-1].strftime('%d-%b-%Y')} "
                f"(today: {today} — holiday/weekend?)"
            )

    return close, high, volume, failed_symbols

# ─────────────────────────────────────────────────────────────
# SECTION G — TRAILING NaN TRIMMER (SHARED UTILITY)
# ─────────────────────────────────────────────────────────────
def _trim_trailing_nan(close: pd.DataFrame, high: pd.DataFrame, volume: pd.DataFrame):
    """
    Trailing all-NaN rows hatata hai.

    Problem: pd.bdate_range mein aaj ka date include hota hai,
    lekin broker API ka today's candle market close ke baad available hota hai.
    Agar user ne aaj select kiya aur data nahi aaya to last row = all NaN.

    Fix: Last row jahan SABHI symbols NaN hain, usse drop karo.
    Calculations automatically last valid trading day use kar lenge.
    """
    if close.empty:
        return close, high, volume
    last_valid_idx = close.dropna(how='all').index
    if len(last_valid_idx) == 0:
        return close, high, volume
    trim_to = last_valid_idx[-1]
    return close.loc[:trim_to], high.loc[:trim_to], volume.loc[:trim_to]


# ─────────────────────────────────────────────────────────────
# SECTION H — UPSTOX BULK FETCHER (LIVE)
# ─────────────────────────────────────────────────────────────
UPSTOX_MAX_LOOKBACK_MONTHS = 120


def fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    """
    Upstox live bulk fetcher.

    end_date from UI is used for bdate_range index construction.
    But for API call, we extend by +1 day to guarantee today's candle.
    The _trim_trailing_nan call handles any all-NaN rows gracefully.
    """
    _token_data = st.session_state.get("upstox_token_data", {})
    access_token = _token_data.get("access_token", "") if isinstance(_token_data, dict) else ""
    if not access_token:
        try:
            access_token = get_upstox_access_token(sidebar=False) or ""
        except Exception:
            access_token = ""
    if not access_token:
        progress_bar.progress(0.0)
        st.error("Please complete Upstox login in the sidebar first, then retry.")
        st.stop()

    status_text.text("Validating Upstox token...")
    if not _validate_token(access_token):
        st.session_state.pop("upstox_token_data", None)
        st.error("Token expired. Please re-login from sidebar and retry.")
        st.stop()
    st.sidebar.success("Token validated OK")

    upstox_start = end_date - relativedelta(months=UPSTOX_MAX_LOOKBACK_MONTHS)
    if start_date < upstox_start:
        start_date = upstox_start

    instrument_map = _load_instrument_map()
    if not instrument_map:
        st.error("Could not load Upstox instrument master.")
        st.stop()

    close_map, high_map, vol_map = {}, {}, {}
    failed, not_found = [], 0
    total = len(symbols)

    for i, sym in enumerate(symbols):
        progress = (i + 1) / total
        instrument_key = _get_instrument_key(sym, instrument_map)
        if not instrument_key:
            not_found += 1
            failed.append(sym)
        else:
            try:
                # _fetch_upstox_history_live internally uses end_date+1 for to_date
                df = _fetch_upstox_history_live(instrument_key, access_token, start_date, end_date)
                if df is not None and not df.empty:
                    idx = pd.to_datetime(df.index)
                    close_map[sym] = pd.Series(df['close'].values, index=idx)
                    high_map[sym]  = pd.Series(df['high'].values, index=idx)
                    vol_map[sym]   = pd.Series((df['close']*df['volume']).values, index=idx)
                else:
                    failed.append(sym)
            except ValueError:
                st.session_state.pop("upstox_token_data", None)
                st.error("Token expired mid-download. Re-login from sidebar and retry.")
                st.stop()
            except Exception:
                failed.append(sym)

        if i % 10 == 0 or i == total - 1:
            progress_bar.progress(progress)
            status_text.text(f"Upstox: {int(progress*100)}% | Fetched: {len(close_map)} | Failed: {len(failed)}")
        time.sleep(0.05)

    progress_bar.progress(1.0)

    # Use end_date (original, not +1) for bdate_range — today's date included
    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)

    # Trim any all-NaN trailing rows (e.g. if run during market hours)
    close, high, volume = _trim_trailing_nan(close, high, volume)

    if not close.empty:
        last_date = close.index[-1].date()
        today     = date.today()
        if last_date >= today:
            status_text.text(
                f"✅ Data up-to-date | {len(close_map)}/{total} fetched | "
                f"Last trading day: {close.index[-1].strftime('%d-%b-%Y')}"
            )
        else:
            status_text.text(
                f"⚠️ {len(close_map)}/{total} fetched | "
                f"Last trading day: {close.index[-1].strftime('%d-%b-%Y')} "
                f"(today: {today} — holiday/weekend?)"
            )

    return close, high, volume, failed


# ═════════════════════════════════════════════════════════════
# SECTION I.5 — ANGEL ONE BULK FETCHER (LIVE) — OPTIMIZED v2
# ═════════════════════════════════════════════════════════════

_ANGELONE_INSTRUMENT_MAP = None

def _load_angelone_instrument_map():
    global _ANGELONE_INSTRUMENT_MAP
    if _ANGELONE_INSTRUMENT_MAP is not None:
        return _ANGELONE_INSTRUMENT_MAP

    if "angelone_instrument_map" in st.session_state:
        _ANGELONE_INSTRUMENT_MAP = st.session_state["angelone_instrument_map"]
        return _ANGELONE_INSTRUMENT_MAP

    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        st.sidebar.info("Downloading Angel One instrument master...")
        response = requests.get(url, timeout=15)
        data = response.json()
        mapping = {}
        for item in data:
            if item['exch_seg'] == 'NSE' and item['symbol'].endswith('-EQ'):
                clean_symbol = item['symbol'].replace('-EQ', '').upper()
                mapping[clean_symbol] = item['token']
        _ANGELONE_INSTRUMENT_MAP = mapping
        st.session_state["angelone_instrument_map"] = mapping
        st.sidebar.success(f"Angel One master loaded - {len(mapping):,} NSE EQ symbols")
        return mapping
    except Exception as e:
        st.sidebar.error(f"Angel One master load failed: {e}")
        return {}


# ── Token Bucket Rate Limiter (Thread-Safe) ─────────────────
class _TokenBucket:
    def __init__(self, max_rate: float = 3.0):
        self._rate      = max_rate
        self._tokens    = max_rate
        self._last_time = time.monotonic()
        self._lock      = threading.Lock()

    def acquire(self):
        while True:
            with self._lock:
                now     = time.monotonic()
                elapsed = now - self._last_time
                self._tokens    = min(self._rate, self._tokens + elapsed * self._rate)
                self._last_time = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            time.sleep(0.05)


_RATE_LIMIT_CODES    = {"AG8001", "AB1010", "AB2010", "AB1004"}
_RATE_LIMIT_KEYWORDS = ("rate", "limit", "exceed", "too many", "throttl", "access denied")

def _fetch_angelone_history_live(client, token: str, start_date: datetime, end_date: datetime, retries=4):
    """
    Angel One single-symbol fetch.
    todate = "end_date 15:30" explicitly requests market close time — includes today's candle.
    """
    historicParam = {
        "exchange":    "NSE",
        "symboltoken": token,
        "interval":    "ONE_DAY",
        "fromdate":    start_date.strftime("%Y-%m-%d 09:15"),
        "todate":      end_date.strftime("%Y-%m-%d 15:30"),   # ← explicit 15:30 = today's close
    }

    delay = 2.0
    for attempt in range(retries):
        try:
            resp = client.getCandleData(historicParam)

            if resp.get('status') and resp.get('data'):
                columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                df = pd.DataFrame(resp['data'], columns=columns)
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
                df.set_index('timestamp', inplace=True)
                return df[['open', 'high', 'low', 'close', 'volume']]

            error_code = str(resp.get('errorcode', '') or resp.get('error_code', ''))
            error_msg  = str(resp.get('message', '') or resp.get('msg', '')).lower()
            is_rate_limit = (
                error_code in _RATE_LIMIT_CODES
                or any(kw in error_msg for kw in _RATE_LIMIT_KEYWORDS)
            )

            if is_rate_limit:
                time.sleep(delay * (2 ** attempt))
                continue

            return None

        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay * (2 ** attempt))

    return None


def _angelone_worker(sym, token, client, start_date, end_date, rate_limiter):
    rate_limiter.acquire()
    time.sleep(0.05)
    df = _fetch_angelone_history_live(client, token, start_date, end_date)
    return sym, df


_ANGELONE_LAST_RUN_TIME: float = 0.0
_ANGELONE_COOLDOWN_SECS: int   = 30

def fetch_angelone(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    """
    Angel One bulk fetcher.
    todate = "end_date 15:30" → explicitly covers today's market close.
    """
    global _ANGELONE_LAST_RUN_TIME

    client = st.session_state.get("angelone_client", None)
    if not client:
        progress_bar.progress(0.0)
        st.error("Please complete Angel One login in the sidebar first, then retry.")
        st.stop()

    elapsed = time.monotonic() - _ANGELONE_LAST_RUN_TIME
    if elapsed < _ANGELONE_COOLDOWN_SECS and _ANGELONE_LAST_RUN_TIME > 0:
        wait = int(_ANGELONE_COOLDOWN_SECS - elapsed)
        for remaining in range(wait, 0, -1):
            status_text.text(
                f"Angel One cooldown: {remaining}s wait to avoid rate-limit "
                f"(previous run just {int(elapsed)}s ago)"
            )
            time.sleep(1)

    angelone_start = end_date - timedelta(days=2000)
    if start_date < angelone_start:
        st.sidebar.info(
            f"Angel One API Limit: Date capped to {angelone_start.strftime('%d-%m-%Y')} "
            f"(Max 2000 days per request)"
        )
        start_date = angelone_start

    status_text.text("Angel One Token Validated. Fetching Master...")
    instrument_map = _load_angelone_instrument_map()
    if not instrument_map:
        st.error("Could not load Angel One instrument master.")
        st.stop()

    tasks     = []
    failed    = []
    not_found = 0

    for sym in symbols:
        token = instrument_map.get(sym.upper().replace('.NS', ''))
        if not token:
            not_found += 1
            failed.append(sym)
        else:
            tasks.append((sym, token))

    total         = len(symbols)
    fetched_count = 0
    close_map, high_map, vol_map = {}, {}, {}

    MAX_WORKERS  = 2
    rate_limiter = _TokenBucket(max_rate=1.5)

    status_text.text(f"Angel One: Fetching {len(tasks)} symbols...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(_angelone_worker, sym, tok, client, start_date, end_date, rate_limiter): sym
            for sym, tok in tasks
        }

        for future in as_completed(future_map):
            sym_result, df = future.result()
            fetched_count += 1

            if df is not None and not df.empty:
                idx = pd.to_datetime(df.index)
                close_map[sym_result] = pd.Series(df['close'].values,                  index=idx)
                high_map[sym_result]  = pd.Series(df['high'].values,                   index=idx)
                vol_map[sym_result]   = pd.Series((df['close'] * df['volume']).values, index=idx)
            else:
                failed.append(sym_result)

            if fetched_count % 5 == 0 or fetched_count == len(tasks):
                progress = (fetched_count + not_found) / total
                progress_bar.progress(min(progress, 1.0))
                status_text.text(
                    f"Angel One: {int(progress * 100)}% | "
                    f"Fetched: {len(close_map)} | Failed: {len(failed)}"
                )

    _ANGELONE_LAST_RUN_TIME = time.monotonic()

    progress_bar.progress(1.0)

    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)

    if close.empty:
        st.error("No data fetched from Angel One. Try re-logging in and retry.")
        st.stop()

    # Trim any all-NaN trailing rows
    close, high, volume = _trim_trailing_nan(close, high, volume)

    if not close.empty:
        last_date = close.index[-1].date()
        today     = date.today()
        if last_date >= today:
            status_text.text(
                f"✅ Data up-to-date | {len(close_map)}/{total} fetched | "
                f"Last trading day: {close.index[-1].strftime('%d-%b-%Y')}"
            )
        else:
            status_text.text(
                f"⚠️ {len(close_map)}/{total} fetched | "
                f"Last trading day: {close.index[-1].strftime('%d-%b-%Y')} "
                f"(today: {today} — holiday/weekend?)"
            )

    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION J — ZERODHA (Mock)
# ─────────────────────────────────────────────────────────────
def fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    status_text.text("Zerodha (MOCK) is not implemented yet.")
    st.stop()

# ─────────────────────────────────────────────────────────────
# SECTION K — UNIFIED ENTRY POINT
# ─────────────────────────────────────────────────────────────
def fetch_data(api_source, symbols, start_date, end_date,
               chunk_size, progress_bar, status_text) -> tuple:
    if api_source == "YFinance":
        return fetch_yfinance(symbols, start_date, chunk_size, progress_bar, status_text)
    elif api_source == "Upstox":
        return fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    elif api_source == "Angel One":
        return fetch_angelone(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    elif api_source == "Zerodha":
        return fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    else:
        raise ValueError(f"Unknown api_source: {api_source!r}")
