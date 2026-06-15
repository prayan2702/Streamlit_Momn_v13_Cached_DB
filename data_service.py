"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (LIVE) | Angel One (LIVE) | Zerodha (placeholder)

── BUG FIX: Upstox T+1 Delay ─────────────────────────────────
  Upstox /v3/historical-candle/days/1/ API has genuine T+1 delay.
  Even at 11:50 PM IST, today's candle is NOT available there.
  Fix: After historical fetch, call Market Quote API to get
  today's OHLCV:
    GET /v3/market-quote/quotes?symbol=NSE_EQ%7CKEY1,...
  ohlc.close after 3:30 PM IST = today's official closing price.

── BUG FIX: YFinance end-date ────────────────────────────────
  yfinance end is EXCLUSIVE. end=today → only yesterday's data.
  Fix: end = tomorrow explicitly (inclusive of today).

── BUG FIX: Angel One ────────────────────────────────────────
  todate = "today 15:30" already correct — explicit market close
  time ensures today's candle is returned.
──────────────────────────────────────────────────────────────

ANGEL ONE SPEED OPTIMIZATION (v2):
  - ThreadPoolExecutor se parallel requests (2 workers)
  - Token Bucket Rate Limiter (1.5 req/sec)
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
# SECTION C — UPSTOX SINGLE SYMBOL FETCHER (V3 historical)
# ─────────────────────────────────────────────────────────────
def _fetch_upstox_history_live(
    instrument_key : str,
    access_token   : str,
    start_date     : datetime,
    end_date       : datetime,
    retries        : int = 2
):
    """
    Historical candle fetch for a single symbol.
    NOTE: T+1 delay — today's candle not available here.
    Today's data is supplemented by _fetch_upstox_today_quotes().
    """
    encoded_key   = instrument_key.replace("|", "%7C")
    from_date_str = start_date.strftime("%Y-%m-%d")
    # Still use tomorrow as safety (won't change T+1 behaviour but future-proof)
    to_date_str   = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

    url     = f"https://api.upstox.com/v3/historical-candle/{encoded_key}/days/1/{to_date_str}/{from_date_str}"
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
            candles = resp.json().get("data", {}).get("candles", [])

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
# ─────────────────────────────────────────────────────────────
# SECTION D — UPSTOX TODAY's DATA via Intraday API (days/1)
# ─────────────────────────────────────────────────────────────
# /v3/historical-candle/intraday/{key}/days/1
# Returns full current-day candle: [ts, open, high, low, CLOSE, volume, oi]
# Per-symbol call — no batch comma-encoding issues, no T+1 delay
_UPSTOX_INTRADAY_URL = "https://api.upstox.com/v3/historical-candle/intraday"


def _fetch_one_intraday_live(instrument_key: str, access_token: str, retries: int = 2) -> dict | None:
    """Single symbol intraday days/1 call. Returns {close, high, volume} or None."""
    encoded = instrument_key.replace("|", "%7C")
    url     = f"{_UPSTOX_INTRADAY_URL}/{encoded}/days/1"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    delay   = 1.0
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 429:
                time.sleep(delay * 2); delay *= 2; continue
            if resp.status_code in (401, 403):
                raise ValueError(f"Token invalid (HTTP {resp.status_code})")
            if resp.status_code == 200:
                candles = resp.json().get("data", {}).get("candles", [])
                if candles:
                    c = candles[0]
                    close  = float(c[4])
                    high   = float(c[2])
                    volume = float(c[5])
                    if close > 0 and volume > 0:
                        return {"close": close, "high": high, "volume": volume}
            return None
        except ValueError:
            raise
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay); delay *= 2
    return None


def _fetch_upstox_today_quotes(
    symbols        : list,
    instrument_map : dict,
    access_token   : str,
    target_date,
    status_text    = None,
) -> dict:
    """
    Upstox Intraday days/1 API se today OHLCV fetch karo.
    Per symbol — no batch/encoding issues.
    Returns empty dict if market open or weekend.
    """
    from datetime import date as _date
    today = _date.today()
    if hasattr(target_date, 'year') and target_date < today:
        return {}
    if today.weekday() >= 5:
        return {}
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    # Skip if intraday (market open and still running)
    if 9 < ist_now.hour < 15 or (ist_now.hour == 15 and ist_now.minute < 30):
        if status_text:
            status_text.text(f"Upstox: Market open ({ist_now.strftime('%H:%M' )} IST) — skipping today top-up")
        return {}

    sym_key_pairs = [(s, _get_instrument_key(s, instrument_map)) for s in symbols]
    sym_key_pairs = [(s, k) for s, k in sym_key_pairs if k]
    total      = len(sym_key_pairs)
    today_data = {}
    ok, skip   = 0, 0
    if status_text:
        status_text.text(f"Upstox: Fetching today intraday for {total:,} symbols...")

    for i, (sym, key) in enumerate(sym_key_pairs):
        try:
            q = _fetch_one_intraday_live(key, access_token)
            if q:
                today_data[sym] = q; ok += 1
            else:
                skip += 1
        except ValueError:
            raise
        except Exception:
            skip += 1
        time.sleep(0.05)
        if status_text and ((i + 1) % 200 == 0 or i == total - 1):
            status_text.text(f"Upstox: Intraday {i+1}/{total} | Got: {ok} | Skip: {skip}")

    return today_data


def _apply_today_quotes_to_maps(close_map, high_map, vol_map, today_data, today) -> None:
    """Add today intraday data into existing Series maps (in-place)."""
    if not today_data:
        return
    today_ts = pd.Timestamp(today)
    for sym, q in today_data.items():
        c, h, v = q["close"], q["high"], q["close"] * q["volume"]
        for mp, val in [(close_map, c), (high_map, h), (vol_map, v)]:
            if sym in mp:
                s = mp[sym]
                if today_ts not in s.index:
                    mp[sym] = pd.concat([s, pd.Series([val], index=[today_ts])])
            else:
                mp[sym] = pd.Series([val], index=[today_ts])



# ─────────────────────────────────────────────────────────────
# SECTION F — YFINANCE FETCHER
# ─────────────────────────────────────────────────────────────
def _download_yfinance_chunk(symbols, start_date, end_date=None, max_retries=3, delay=2.0):
    """
    yfinance chunk download.
    end_date is EXCLUSIVE — pass tomorrow to include today.
    If None, defaults to tomorrow automatically.
    """
    if end_date is None:
        end_date = datetime.now() + timedelta(days=1)

    for attempt in range(max_retries):
        try:
            return yf.download(
                symbols,
                start=start_date,
                end=end_date,
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
    end = tomorrow (exclusive) → includes today's data.
    """
    close_chunks, high_chunks, volume_chunks, failed_symbols = [], [], [], []
    total = len(symbols)
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

    if not close.empty:
        last_date = close.index[-1].date()
        today_d   = date.today()
        if last_date >= today_d:
            status_text.text(f"✅ YFinance up-to-date | Last: {close.index[-1].strftime('%d-%b-%Y')}")
        else:
            status_text.text(
                f"⚠️ YFinance Last: {close.index[-1].strftime('%d-%b-%Y')} "
                f"(today: {today_d} — holiday/weekend?)"
            )

    return close, high, volume, failed_symbols


# ─────────────────────────────────────────────────────────────
# SECTION G — TRAILING NaN TRIMMER
# ─────────────────────────────────────────────────────────────
def _trim_trailing_nan(close: pd.DataFrame, high: pd.DataFrame, volume: pd.DataFrame):
    """Drop trailing rows where ALL symbols are NaN."""
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

    Two-phase approach to overcome T+1 delay:
    Phase 1: Historical candle API → data up to YESTERDAY
    Phase 2: Market Quote API → TODAY's OHLCV (after market close)
    """
    _token_data  = st.session_state.get("upstox_token_data", {})
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

    # ── Phase 1: Historical fetch (up to yesterday) ────────────
    close_map, high_map, vol_map = {}, {}, {}
    failed, not_found = [], 0
    total = len(symbols)

    status_text.text(f"Upstox: Phase 1 — Historical fetch (up to yesterday)...")

    for i, sym in enumerate(symbols):
        progress       = (i + 1) / total * 0.80   # 80% progress bar for historical
        instrument_key = _get_instrument_key(sym, instrument_map)
        if not instrument_key:
            not_found += 1
            failed.append(sym)
        else:
            try:
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
            progress_bar.progress(min(progress, 0.80))
            status_text.text(
                f"Upstox Phase 1: {int((i+1)/total*100)}% | "
                f"Fetched: {len(close_map)} | Failed: {len(failed)}"
            )
        time.sleep(0.05)

    # ── Phase 2: Today's data via Market Quote API ─────────────
    # Upstox historical candle API has T+1 delay — today's candle
    # is not available. Market Quote API gives real-time data.
    progress_bar.progress(0.82)
    status_text.text("Upstox: Phase 2 — Fetching today's data via Market Quote API...")

    today_data = {}
    try:
        today_data = _fetch_upstox_today_quotes(
            symbols, instrument_map, access_token,
            end_date.date() if hasattr(end_date, 'date') else end_date,
            status_text=status_text,
        )
        if today_data:
            _apply_today_quotes_to_maps(close_map, high_map, vol_map, today_data, date.today())
            progress_bar.progress(0.88)
            status_text.text(
                f"Upstox: Today's quotes added for {len(today_data)} symbols"
            )
        else:
            status_text.text(
                "Upstox: No today's quotes (market open / weekend / holiday — using historical only)"
            )
    except ValueError:
        # Token error from market quote API
        status_text.text("Upstox: Market quote fetch: token issue — using historical data")
    except Exception as e:
        status_text.text(f"Upstox: Market quote fetch failed ({type(e).__name__}) — using historical")

    progress_bar.progress(0.92)

    # ── Assemble final DataFrames ───────────────────────────────
    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)

    # Trim any all-NaN trailing rows
    close, high, volume = _trim_trailing_nan(close, high, volume)
    progress_bar.progress(1.0)

    if not close.empty:
        last_date = close.index[-1].date()
        today_d   = date.today()
        if last_date >= today_d:
            status_text.text(
                f"✅ Upstox up-to-date | {len(close_map)}/{total} fetched | "
                f"Today's quotes: {len(today_data)} | "
                f"Last: {close.index[-1].strftime('%d-%b-%Y')}"
            )
        else:
            status_text.text(
                f"⚠️ Upstox {len(close_map)}/{total} fetched | "
                f"Last: {close.index[-1].strftime('%d-%b-%Y')} "
                f"(today: {today_d} — holiday/weekend?)"
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
    todate = "today 15:30" explicitly — today's candle returned after market close.
    """
    historicParam = {
        "exchange":    "NSE",
        "symboltoken": token,
        "interval":    "ONE_DAY",
        "fromdate":    start_date.strftime("%Y-%m-%d 09:15"),
        "todate":      end_date.strftime("%Y-%m-%d 15:30"),   # explicit 15:30 = today's close
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
    """Angel One bulk fetcher — todate = "today 15:30" gives today's candle."""
    global _ANGELONE_LAST_RUN_TIME

    client = st.session_state.get("angelone_client", None)
    if not client:
        progress_bar.progress(0.0)
        st.error("Please complete Angel One login in the sidebar first, then retry.")
        st.stop()

    elapsed_cool = time.monotonic() - _ANGELONE_LAST_RUN_TIME
    if elapsed_cool < _ANGELONE_COOLDOWN_SECS and _ANGELONE_LAST_RUN_TIME > 0:
        wait = int(_ANGELONE_COOLDOWN_SECS - elapsed_cool)
        for remaining in range(wait, 0, -1):
            status_text.text(f"Angel One cooldown: {remaining}s (prev run {int(elapsed_cool)}s ago)")
            time.sleep(1)

    angelone_start = end_date - timedelta(days=2000)
    if start_date < angelone_start:
        st.sidebar.info(
            f"Angel One: Date capped to {angelone_start.strftime('%d-%m-%Y')} (max 2000 days)"
        )
        start_date = angelone_start

    status_text.text("Angel One: Fetching instrument master...")
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
        st.error("No data fetched from Angel One.")
        st.stop()

    close, high, volume = _trim_trailing_nan(close, high, volume)

    if not close.empty:
        last_date = close.index[-1].date()
        today_d   = date.today()
        if last_date >= today_d:
            status_text.text(
                f"✅ Angel One up-to-date | {len(close_map)}/{total} fetched | "
                f"Last: {close.index[-1].strftime('%d-%b-%Y')}"
            )
        else:
            status_text.text(
                f"⚠️ Angel One {len(close_map)}/{total} | "
                f"Last: {close.index[-1].strftime('%d-%b-%Y')} "
                f"(today: {today_d} — holiday/weekend?)"
            )

    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION J — ZERODHA (Mock)
# ─────────────────────────────────────────────────────────────
def fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    status_text.text("Zerodha (MOCK) not implemented yet.")
    st.stop()


# ═════════════════════════════════════════════════════════════
# SECTION J2 — TRADINGVIEW LIVE FETCHER (Direct WebSocket)
# =============================================================
# pip install tvDatafeed ka login broken hai (TradingView API change).
# Direct WebSocket implementation — same as cache_builder_tradingview.py
# Login: requests.Session() + User-Agent header + homepage GET then POST
# WebSocket: wss://data.tradingview.com/socket.io/websocket
# Symbol: NSE:INFY
# =============================================================

import json as _json
import random as _random
import re as _re
import string as _string
from websocket import create_connection as _create_connection

TV_MAX_BARS   = 5000
TV_WS_TIMEOUT = 30  # seconds


def _get_tv_token(tv_username: str = "", tv_password: str = "") -> str:
    """
    TradingView auth token — proper session-based login.
    pip version ka login broken tha (no User-Agent, no cookie prefetch).
    """
    if not tv_username or not tv_password:
        return "unauthorized_user_token"
    session = requests.Session()
    session.headers.update({
        "Referer": "https://www.tradingview.com",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    })
    try:
        session.get("https://www.tradingview.com/", timeout=15)
        resp = session.post(
            "https://www.tradingview.com/accounts/signin/",
            data={"username": tv_username, "password": tv_password, "remember": "on"},
            timeout=15,
        )
        return resp.json()["user"]["auth_token"]
    except Exception:
        return "unauthorized_user_token"


def _tv_msg(func: str, params: list) -> str:
    body = _json.dumps({"m": func, "p": params}, separators=(",", ":"))
    return f"~m~{len(body)}~m~{body}"


def _tv_parse_df(raw_data: str):
    """Raw WebSocket stream se OHLCV DataFrame banao."""
    try:
        out = _re.search(r'"s":\[(.+?)\}]', raw_data).group(1)
        rows, vol_ok = [], True
        for xi in out.split(',{"'):
            xi = _re.split(r"[\[:|,\]]", xi)
            try:
                # UTC timestamp → tz-naive datetime (date-only precision)
                ts = pd.Timestamp(float(xi[4]), unit="s").normalize()
            except (ValueError, IndexError):
                continue
            row = [ts]
            for i in range(5, 10):
                if not vol_ok and i == 9:
                    row.append(0.0)
                    continue
                try:
                    row.append(float(xi[i]))
                except (ValueError, IndexError):
                    vol_ok = False
                    row.append(0.0)
            rows.append(row)
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume"])
        df = df.set_index("datetime").sort_index()
        df = df[~df.index.duplicated(keep="last")]
        df.columns = [c.lower() for c in df.columns]
        return df
    except AttributeError:
        return None


def _fetch_tv_single(token: str, symbol_ns: str, n_bars: int = TV_MAX_BARS, retries: int = 2):
    """
    Single symbol ka daily OHLCV TradingView WebSocket se fetch karo.
    token: _get_tv_token() ka output
    symbol_ns: 'INFY.NS' ya 'INFY' -- .NS strip hoga
    Returns: pd.DataFrame or None
    """
    clean     = symbol_ns.replace(".NS", "").replace(".BO", "").upper().strip()
    tv_symbol = f"NSE:{clean}"

    for attempt in range(retries):
        ws = None
        try:
            ws = _create_connection(
                "wss://data.tradingview.com/socket.io/websocket",
                headers=_json.dumps({"Origin": "https://data.tradingview.com"}),
                timeout=TV_WS_TIMEOUT,
            )
            sess  = "qs_" + "".join(_random.choice(_string.ascii_lowercase) for _ in range(12))
            csess = "cs_" + "".join(_random.choice(_string.ascii_lowercase) for _ in range(12))

            for func, params in [
                ("set_auth_token",       [token]),
                ("chart_create_session", [csess, ""]),
                ("quote_create_session", [sess]),
                ("quote_set_fields",     [sess, "ch", "chp", "current_session", "description",
                                          "local_description", "language", "exchange", "fractional",
                                          "is_tradable", "lp", "lp_time", "minmov", "minmove2",
                                          "original_name", "pricescale", "pro_name", "short_name",
                                          "type", "update_mode", "volume", "currency_code", "rchp", "rtc"]),
                ("quote_add_symbols",    [sess, tv_symbol, {"flags": ["force_permission"]}]),
                ("quote_fast_symbols",   [sess, tv_symbol]),
                ("resolve_symbol",       [csess, "symbol_1",
                                          f'={{"symbol":"{tv_symbol}","adjustment":"splits","session":"regular"}}']),
                ("create_series",        [csess, "s1", "s1", "symbol_1", "1D", n_bars]),
                ("switch_timezone",      [csess, "exchange"]),
            ]:
                ws.send(_tv_msg(func, params))

            # recv loop — series_completed aane tak wait karo
            # Pehle exception pe break mat karo — incomplete data milega
            raw             = ""
            series_done     = False
            recv_errors     = 0
            MAX_RECV_ERRORS = 8   # ping/pong + keepalive messages ignore karo

            while not series_done:
                try:
                    chunk = ws.recv()
                    if chunk:
                        raw += chunk + "\n"
                        if "series_completed" in chunk:
                            series_done = True
                        elif "symbol_error" in chunk or "critical_error" in chunk:
                            break  # symbol not found
                        # ping/pong (~m~X~m~~h~) — silently ignore
                except Exception:
                    recv_errors += 1
                    if recv_errors >= MAX_RECV_ERRORS:
                        break  # genuine connection issue

            try:
                ws.close()
            except Exception:
                pass

            df = _tv_parse_df(raw)
            if df is None or df.empty:
                if attempt < retries - 1:
                    time.sleep(2.0)
                    continue
                return None
            needed = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
            return df[needed]

        except Exception as e:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
            err = str(e).lower()
            if attempt < retries - 1:
                time.sleep(5.0 if ("timeout" in err or "connection" in err) else 2.0)
            else:
                return None
    return None


def fetch_tradingview(
    symbols, start_date, end_date, chunk_size, progress_bar, status_text,
    tv_username: str = "", tv_password: str = ""
):
    """
    TradingView (tvDatafeed) bulk fetcher.

    - Credentials: st.session_state se pick karta hai ya parameter se.
    - n_bars = 5000 (max) — ~13+ years daily data.
    - start_date ke baad ka data slice karta hai final DataFrame mein.
    - Rate limit: 0.2s sleep between symbols.

    Returns: (close_df, high_df, volume_df, failed_symbols)
    """
    # Credentials priority: Streamlit secrets > session_state > parameter > anonymous
    # Pehle secrets try karo (most reliable on Streamlit Cloud)
    if not tv_username:
        try:
            tv_username = st.secrets["tradingview"]["username"]
        except Exception:
            tv_username = ""
    if not tv_password:
        try:
            tv_password = st.secrets["tradingview"]["password"]
        except Exception:
            tv_password = ""

    # Secrets nahi mili to session_state se try karo (sidebar input)
    if not tv_username:
        tv_username = (
            st.session_state.get("tv_username", "")
            or st.session_state.get("tv_username_input", "")
            or ""
        )
    if not tv_password:
        tv_password = (
            st.session_state.get("tv_password", "")
            or st.session_state.get("tv_password_input", "")
            or ""
        )

    # Initialize — get auth token (proper session-based login)
    status_text.text("TradingView: Login ho raha hai...")
    tv_token = _get_tv_token(tv_username, tv_password)
    # Note: TradingView login server-side pe Cloudflare se block hota hai.
    # Anonymous mode mein bhi 5000 bars milte hain — data fetch hota hai.
    # Login warning show mat karo — confusing hai user ke liye.

    total = len(symbols)
    close_map, high_map, vol_map = {}, {}, {}
    failed = []

    status_text.text(f"TradingView: {total:,} symbols fetch ho rahe hain (NSE daily)...")

    ath_map = {}   # sym → lifetime ATH from full 5000-bar history

    for i, sym in enumerate(symbols):
        progress = (i + 1) / total
        try:
            # Full 5000 bars fetch karo — maximum available history (~13 yrs)
            df_full = _fetch_tv_single(tv_token, sym, n_bars=TV_MAX_BARS)
            if df_full is not None and not df_full.empty:
                # Normalize index — timezone strip karo
                df_full.index = pd.to_datetime(df_full.index)
                if df_full.index.tz is not None:
                    df_full.index = df_full.index.tz_localize(None)

                # ── ATH: FULL history ka max high (slice se PEHLE) ──
                ath_val = float(df_full["high"].max()) if "high" in df_full.columns else float(df_full["close"].max())
                ath_map[sym] = ath_val

                # ── Recent slice: start_date ke baad ──────────────
                start_ts = pd.Timestamp(start_date)
                if start_ts.tz is not None:
                    start_ts = start_ts.tz_localize(None)
                df = df_full[df_full.index >= start_ts]
                if not df.empty and "close" in df.columns:
                    idx = df.index
                    close_map[sym] = pd.Series(df["close"].values, index=idx)
                    high_map[sym]  = pd.Series(df["high"].values,  index=idx) if "high" in df.columns else pd.Series(dtype=float)
                    vol_map[sym]   = pd.Series(
                        (df["close"] * df["volume"]).values, index=idx
                    ) if "volume" in df.columns else pd.Series(dtype=float)
                else:
                    failed.append(sym)
            else:
                failed.append(sym)
        except Exception:
            failed.append(sym)

        if i % 10 == 0 or i == total - 1:
            progress_bar.progress(min(progress, 1.0))
            status_text.text(
                f"TradingView: {int(progress * 100)}% | "
                f"Fetched: {len(close_map)} | Failed: {len(failed)}"
            )
        time.sleep(0.5)  # rate limit — 0.2 se badhakar 0.5s (TradingView throttle avoid)

    # ── Retry failed symbols once with longer sleep ─────────────
    if failed:
        status_text.text(f"TradingView: Retrying {len(failed)} failed symbols...")
        still_failed = []
        for sym in failed:
            time.sleep(2.0)  # longer sleep for retry
            try:
                df_full = _fetch_tv_single(tv_token, sym, n_bars=TV_MAX_BARS, retries=1)
                if df_full is not None and not df_full.empty:
                    df_full.index = pd.to_datetime(df_full.index)
                    if df_full.index.tz is not None:
                        df_full.index = df_full.index.tz_localize(None)
                    ath_val = float(df_full["high"].max()) if "high" in df_full.columns else float(df_full["close"].max())
                    ath_map[sym] = ath_val
                    start_ts = pd.Timestamp(start_date)
                    if start_ts.tz is not None:
                        start_ts = start_ts.tz_localize(None)
                    df = df_full[df_full.index >= start_ts]
                    if not df.empty and "close" in df.columns:
                        idx = df.index
                        close_map[sym] = pd.Series(df["close"].values, index=idx)
                        high_map[sym]  = pd.Series(df["high"].values,  index=idx) if "high" in df.columns else pd.Series(dtype=float)
                        vol_map[sym]   = pd.Series(
                            (df["close"] * df["volume"]).values, index=idx
                        ) if "volume" in df.columns else pd.Series(dtype=float)
                    else:
                        still_failed.append(sym)
                else:
                    still_failed.append(sym)
            except Exception:
                still_failed.append(sym)
        failed = still_failed

    progress_bar.progress(1.0)

    # Assemble DataFrames
    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()},  index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},   index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},    index=all_idx)

    close, high, volume = _trim_trailing_nan(close, high, volume)

    # ── ATH inject: synthetic 2000-01-01 row mein lifetime max high ──
    # calculations.py mein `ATH = high.max()` use hota hai.
    # Is row se ensure hota hai ki lookback se pehle ka ATH bhi capture ho.
    if ath_map and not high.empty:
        ath_series = pd.Series(ath_map).reindex(high.columns)
        ath_row    = pd.DataFrame(
            [ath_series.values],
            columns=high.columns,
            index=[pd.Timestamp("2000-01-01")],
        )
        high = pd.concat([ath_row, high]).sort_index()
        high = high.loc[:, ~high.columns.duplicated()]

    if not close.empty:
        last_date = close.index[-1].date()
        today_d   = date.today()
        if last_date >= today_d:
            status_text.text(
                f"✅ TradingView up-to-date | {len(close_map)}/{total} fetched | "
                f"Last: {close.index[-1].strftime('%d-%b-%Y')}"
            )
        else:
            status_text.text(
                f"⚠️ TradingView {len(close_map)}/{total} | "
                f"Last: {close.index[-1].strftime('%d-%b-%Y')} "
                f"(today: {today_d} — holiday/weekend?)"
            )

    return close, high, volume, failed


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
    elif api_source == "TradingView":
        return fetch_tradingview(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    elif api_source == "Zerodha":
        return fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    else:
        raise ValueError(f"Unknown api_source: {api_source!r}")
