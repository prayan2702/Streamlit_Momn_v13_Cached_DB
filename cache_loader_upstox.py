"""
cache_loader_upstox.py
======================
Streamlit app se call hota hai — Upstox pre-cached data loader.
GitHub raw URLs se Parquet files load karta hai (cache_upstox/ folder).

5-Day Rolling Cache Support:
  - cache_index.json se available dates list karo
  - cache_date=None → latest date auto-select
  - cache_date="YYYY-MM-DD" → specific date load

Folder map:
  YFinance cache  → cache/
  Upstox cache    → cache_upstox/    ← YE FILE
  Angel One cache → cache_angelone/

ATH Logic (unchanged):
  ATH row inject into high — calculations.py mein koi change nahi.
  Trailing NaN rows drop + ffill (holiday fix) — same as before.

Usage:
    from cache_loader_upstox import load_cache, get_cache_meta,
                                    get_cache_age_days, get_cache_status_html,
                                    list_available_dates
    dates = list_available_dates()
    close, high, volume = load_cache()           # latest
    close, high, volume = load_cache("2026-04-14")  # specific
"""

import requests
import pandas as pd
import streamlit as st
from datetime import datetime, date

# ── GitHub raw base URL — Upstox cache folder ─────────────────
_GITHUB_CACHE = (
    "https://raw.githubusercontent.com/"
    "prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main/cache_upstox"
)

_INDEX_URL = f"{_GITHUB_CACHE}/cache_index.json"


# ── Available dates ───────────────────────────────────────────
@st.cache_data(ttl=900, show_spinner=False)
def list_available_dates() -> list:
    """
    cache_upstox/cache_index.json se available dates list karo (oldest → latest).
    ttl=900 (15 min).
    """
    try:
        r = requests.get(_INDEX_URL, timeout=10)
        r.raise_for_status()
        return sorted(r.json().get("dates", []))
    except Exception:
        return []


def get_latest_date() -> str | None:
    dates = list_available_dates()
    return dates[-1] if dates else None


def _resolve_date(cache_date: str | None) -> str | None:
    dates = list_available_dates()
    if not dates:
        return None
    if cache_date and cache_date in dates:
        return cache_date
    return dates[-1]


# ── Meta ──────────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def get_cache_meta(cache_date: str | None = None) -> dict:
    """cache_upstox/{date}/cache_meta.json load karo."""
    d = _resolve_date(cache_date)
    if not d:
        return {"error": "No Upstox cache available"}
    url = f"{_GITHUB_CACHE}/{d}/cache_meta.json"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def get_cache_age_days(cache_date: str | None = None) -> float:
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
    """Upstox cache status HTML card for sidebar display."""
    meta   = get_cache_meta(cache_date)
    age    = get_cache_age_days(cache_date)
    dates  = list_available_dates()
    loaded = _resolve_date(cache_date) or "N/A"

    if "error" in meta:
        return f"""
        <div style="background:#fee2e2;border:1px solid #fca5a5;border-left:4px solid #dc2626;
                    border-radius:10px;padding:12px 16px;font-size:13px;color:#7f1d1d;margin:10px 0;">
          ❌ <b>Upstox Cache load failed:</b> {meta['error']}<br>
          <span style="font-size:11px;">daily_cache_upstox.yml workflow run karo.</span>
        </div>"""

    if not meta or "build_date" not in meta:
        return """
        <div style="background:#fef3c7;border:1px solid #fcd34d;border-left:4px solid #d97706;
                    border-radius:10px;padding:12px 16px;font-size:13px;color:#92400e;margin:10px 0;">
          ⚠️ <b>Upstox Cache not found yet.</b> daily_cache_upstox.yml pehli baar chalegi.
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

    fetched   = meta.get("symbols_fetched", "?")
    build_dt  = meta.get("build_date", "?")
    src       = meta.get("source", "Upstox V3")
    sym_src   = meta.get("symbol_source", "NSE EQUITY_L.csv")
    failed_ct = meta.get("symbols_failed", 0)
    n_dates   = len(dates)
    is_hist   = (cache_date and cache_date != dates[-1]) if dates else False

    return f"""
    <div style="background:{color};border:1px solid {bdr};border-left:4px solid {bdr};
                border-radius:10px;padding:12px 16px;font-size:13px;color:{text};margin:10px 0;">
      {icon} <b>Upstox Pre-cached</b> &nbsp;·&nbsp; {freshness}
      {'&nbsp;·&nbsp; <b>📜 Historical</b>' if is_hist else ''}<br>
      <span style="font-size:11.5px;margin-top:4px;display:block;">
        📅 Loaded date: <b>{loaded}</b> &nbsp;·&nbsp;
        📋 Symbols: <b>{fetched:,}</b> &nbsp;·&nbsp;
        ❌ Failed: <b>{failed_ct}</b><br>
        📡 Source: <b>{src}</b> &nbsp;·&nbsp;
        📋 <b>{sym_src}</b> (+ GOLDBEES &amp; SILVERBEES)<br>
        🗂️ Cached dates: <b>{n_dates}/5</b> available
        &nbsp;·&nbsp; ⚡ Load time: <b>&lt;10 sec</b>
      </span>
    </div>"""


# ── Main load function ────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_cache(cache_date: str | None = None):
    """
    GitHub se Upstox Parquet files load karo.

    Args:
        cache_date: "YYYY-MM-DD" ya None (latest auto-select).

    Returns:
        (close_df, high_with_ath_df, volume_df)

    Files loaded (from cache_upstox/{date}/ subfolder):
        close.parquet   → recent 40 months close prices  (Upstox V3, split-adjusted)
        high.parquet    → recent 40 months high prices
        volume.parquet  → recent 40 months close×volume
        ath.parquet     → ALL TIME HIGH (2000 to today max)
    """
    d = _resolve_date(cache_date)
    if not d:
        raise FileNotFoundError("No Upstox cache available. Run daily_cache_upstox.yml first.")

    base = f"{_GITHUB_CACHE}/{d}"

    close  = pd.read_parquet(f"{base}/close.parquet")
    high   = pd.read_parquet(f"{base}/high.parquet")
    volume = pd.read_parquet(f"{base}/volume.parquet")
    ath    = pd.read_parquet(f"{base}/ath.parquet")

    # ── Timezone strip ─────────────────────────────────────────
    for df in (close, high, volume):
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df.index = df.index.tz_localize(None)

    # ── ROOT CAUSE FIX: Holiday / weekend last-row NaN ────────
    # Upstox T+1 delay + market holidays → last row NaN.
    # Fix: trailing all-NaN rows drop karo + ffill.
    close  = close.sort_index().dropna(how='all').ffill()
    volume = volume.sort_index().dropna(how='all').ffill()
    high   = high.sort_index()

    # ── ATH row inject into high DataFrame ────────────────────
    # calculations.py mein `ATH = high.max()` — synthetic 2000-01-01 row.
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
