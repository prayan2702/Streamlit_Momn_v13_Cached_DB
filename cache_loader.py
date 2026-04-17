"""
cache_loader.py
===============
Streamlit app se call hota hai — YFinance pre-cached data loader.
GitHub raw URLs se Parquet files load karta hai (cache/ folder).

5-Day Rolling Cache Support:
  - cache_index.json se available dates list karo
  - cache_date=None → latest date auto-select
  - cache_date="YYYY-MM-DD" → specific date load

ATH Logic (unchanged):
  ATH row ko high DataFrame mein inject karta hai — calculations.py mein
  koi change nahi karna padta, high.max() automatically correct ATH deta hai.

Usage:
    from cache_loader import load_cache, get_cache_meta, get_cache_age_days,
                            get_cache_status_html, list_available_dates
    dates = list_available_dates()          # ["2026-04-14", "2026-04-15", ...]
    close, high, volume = load_cache()      # latest
    close, high, volume = load_cache("2026-04-14")  # specific date
"""

import json
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, date

# ── GitHub raw base URL ───────────────────────────────────────
_GITHUB_CACHE = (
    "https://raw.githubusercontent.com/"
    "prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main/cache"
)

_INDEX_URL = f"{_GITHUB_CACHE}/cache_index.json"


# ── Available dates ───────────────────────────────────────────
@st.cache_data(ttl=900, show_spinner=False)
def list_available_dates() -> list:
    """
    cache_index.json se available dates list karo (oldest → latest).
    ttl=900 (15 min) — daily build ke baad auto-refresh.
    """
    try:
        r = requests.get(_INDEX_URL, timeout=10)
        r.raise_for_status()
        data = r.json()
        return sorted(data.get("dates", []))
    except Exception:
        return []


def get_latest_date() -> str | None:
    dates = list_available_dates()
    return dates[-1] if dates else None


def _resolve_date(cache_date: str | None) -> str | None:
    """cache_date=None → latest. Invalid date → latest."""
    dates = list_available_dates()
    if not dates:
        return None
    if cache_date and cache_date in dates:
        return cache_date
    return dates[-1]


# ── Meta ──────────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def get_cache_meta(cache_date: str | None = None) -> dict:
    """
    cache_meta.json load karo for given date (None = latest).
    ttl=1800 → 30 min cached.
    """
    d = _resolve_date(cache_date)
    if not d:
        return {"error": "No cache available"}
    url = f"{_GITHUB_CACHE}/{d}/cache_meta.json"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def get_cache_age_days(cache_date: str | None = None) -> float:
    """Cache kitne din purana hai (build_date se aaj)."""
    meta = get_cache_meta(cache_date)
    if not meta or "build_date" not in meta:
        return 999.0
    try:
        build = datetime.strptime(meta["build_date"], "%Y-%m-%d").date()
        return float((date.today() - build).days)
    except Exception:
        return 999.0


def is_cache_fresh(max_days: int = 3, cache_date: str | None = None) -> bool:
    return get_cache_age_days(cache_date) <= max_days


def get_cache_status_html(cache_date: str | None = None) -> str:
    """
    Sidebar display ke liye cache status HTML card.
    Green = 0-1 din | Amber = 2-3 din | Red = 4+ din
    """
    meta   = get_cache_meta(cache_date)
    age    = get_cache_age_days(cache_date)
    dates  = list_available_dates()
    loaded = _resolve_date(cache_date) or "N/A"

    if "error" in meta:
        return f"""
        <div style="background:#fee2e2;border:1px solid #fca5a5;border-left:4px solid #dc2626;
                    border-radius:10px;padding:12px 16px;font-size:13px;color:#7f1d1d;margin:10px 0;">
          ❌ <b>Cache load failed:</b> {meta['error']}<br>
          <span style="font-size:11px;">YFinance live fetch use karo Step 2 mein.</span>
        </div>"""

    if not meta or "build_date" not in meta:
        return """
        <div style="background:#fef3c7;border:1px solid #fcd34d;border-left:4px solid #d97706;
                    border-radius:10px;padding:12px 16px;font-size:13px;color:#92400e;margin:10px 0;">
          ⚠️ <b>Cache not found yet.</b> GitHub Actions pehli baar chalegi raat mein.
        </div>"""

    if age <= 1:
        color, bdr, text, icon = "#dcfce7", "#86efac", "#15803d", "✅"
        freshness = "Fresh (aaj ka)"
    elif age <= 3:
        color, bdr, text, icon = "#fef3c7", "#fcd34d", "#92400e", "⚠️"
        freshness = f"{int(age)} din purana"
    else:
        color, bdr, text, icon = "#fee2e2", "#fca5a5", "#7f1d1d", "❌"
        freshness = f"{int(age)} din purana — stale!"

    fetched  = meta.get("symbols_fetched", "?")
    build_dt = meta.get("build_date", "?")
    src      = meta.get("source", "YFinance")
    sym_src  = meta.get("symbol_source", "NSE EQUITY_L.csv")
    n_dates  = len(dates)
    is_hist  = (cache_date and cache_date != dates[-1]) if dates else False

    return f"""
    <div style="background:{color};border:1px solid {bdr};border-left:4px solid {bdr};
                border-radius:10px;padding:12px 16px;font-size:13px;color:{text};margin:10px 0;">
      {icon} <b>YFinance Pre-cached</b> &nbsp;·&nbsp; {freshness}
      {'&nbsp;·&nbsp; <b>📜 Historical</b>' if is_hist else ''}<br>
      <span style="font-size:11.5px;margin-top:4px;display:block;">
        📅 Loaded date: <b>{loaded}</b> &nbsp;·&nbsp;
        📋 Symbols: <b>{fetched:,}</b><br>
        📡 Source: <b>{src}</b> &nbsp;·&nbsp;
        📋 <b>{sym_src}</b> (+ GOLDBEES &amp; SILVERBEES)<br>
        🗂️ Cached dates: <b>{n_dates}/5</b> available
        &nbsp;·&nbsp; ⚡ Load time: <b>&lt;5 sec</b>
      </span>
    </div>"""


# ── Main load function ────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_cache(cache_date: str | None = None):
    """
    GitHub se YFinance Parquet files load karo.

    Args:
        cache_date: "YYYY-MM-DD" ya None (latest auto-select).

    Returns:
        (close_df, high_with_ath_df, volume_df)
        — directly build_dfStats() mein pass karo.

    Files loaded (from cache/{date}/ subfolder):
        close.parquet   → recent 40 months close prices
        high.parquet    → recent 40 months high prices
        volume.parquet  → recent 40 months close×volume
        ath.parquet     → ALL TIME HIGH (2000 to today max)
    """
    d = _resolve_date(cache_date)
    if not d:
        raise FileNotFoundError("No cache available. Run GitHub Actions workflow first.")

    base = f"{_GITHUB_CACHE}/{d}"

    close  = pd.read_parquet(f"{base}/close.parquet")
    high   = pd.read_parquet(f"{base}/high.parquet")
    volume = pd.read_parquet(f"{base}/volume.parquet")
    ath    = pd.read_parquet(f"{base}/ath.parquet")

    # ── Timezone strip ─────────────────────────────────────────
    for df in (close, high, volume):
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df.index = df.index.tz_localize(None)

    # ── ATH row inject into high DataFrame ────────────────────
    # calculations.py mein `ATH = high.max()` use hota hai.
    # 2000-01-01 pe synthetic row inject karte hain jisme ATH value hoti hai.
    # Isse high.max() = correct all-time-high (2000 se aaj tak).
    # calculations.py mein ZERO change needed.
    ath_series = ath["ATH"].reindex(high.columns)
    ath_row    = pd.DataFrame(
        [ath_series.values],
        columns=high.columns,
        index=[pd.Timestamp("2000-01-01")],
    )
    high_with_ath = pd.concat([ath_row, high]).sort_index()

    # ── Duplicate columns remove ───────────────────────────────
    close         = close.loc[:,         ~close.columns.duplicated()]
    high_with_ath = high_with_ath.loc[:, ~high_with_ath.columns.duplicated()]
    volume        = volume.loc[:,         ~volume.columns.duplicated()]

    return close, high_with_ath, volume
