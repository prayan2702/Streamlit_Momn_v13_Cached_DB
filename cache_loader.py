"""
cache_loader.py
===============
Streamlit app se call hota hai.
GitHub raw URLs se Parquet files load karta hai.
ATH row ko high DataFrame mein inject karta hai — calculations.py mein
koi change nahi karna padta, high.max() automatically correct ATH deta hai.

Usage (Step 2 mein):
    from cache_loader import load_cache, get_cache_meta, get_cache_age_days

    meta = get_cache_meta()
    close, high, volume = load_cache()
    # direct feed into build_dfStats(close, high, volume, dates, method)
"""

import json
import requests
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime, date

# ── GitHub raw base URL ───────────────────────────────────────
_GITHUB_CACHE = (
    "https://raw.githubusercontent.com/"
    "prayan2702/Streamlit-momn/refs/heads/main/cache"
)

_META_URL   = f"{_GITHUB_CACHE}/cache_meta.json"
_CLOSE_URL  = f"{_GITHUB_CACHE}/close.parquet"
_HIGH_URL   = f"{_GITHUB_CACHE}/high.parquet"
_VOL_URL    = f"{_GITHUB_CACHE}/volume.parquet"
_ATH_URL    = f"{_GITHUB_CACHE}/ath.parquet"


# ── Meta (lightweight — ~1 KB JSON) ──────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def get_cache_meta() -> dict:
    """
    cache_meta.json load karo.
    ttl=1800 → 30 min cached, phir fresh check.
    """
    try:
        r = requests.get(_META_URL, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def get_cache_age_days() -> float:
    """Cache kitne din purana hai."""
    meta = get_cache_meta()
    if not meta or "build_date" not in meta:
        return 999.0
    try:
        build = datetime.strptime(meta["build_date"], "%Y-%m-%d").date()
        return float((date.today() - build).days)
    except Exception:
        return 999.0


def is_cache_fresh(max_days: int = 3) -> bool:
    """Cache max_days se zyada purana nahi hai?"""
    return get_cache_age_days() <= max_days


def get_cache_status_html() -> str:
    """
    Step 1 mein dikhane ke liye status card HTML.
    Green = 0-1 din | Amber = 2-3 din | Red = 4+ din
    """
    meta = get_cache_meta()
    age  = get_cache_age_days()

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

    return f"""
    <div style="background:{color};border:1px solid {bdr};border-left:4px solid {bdr};
                border-radius:10px;padding:12px 16px;font-size:13px;color:{text};margin:10px 0;">
      {icon} <b>Pre-cached Data</b> &nbsp;·&nbsp; {freshness}<br>
      <span style="font-size:11.5px;margin-top:4px;display:block;">
        📅 Build date: <b>{build_dt}</b> &nbsp;·&nbsp;
        📋 Symbols: <b>{fetched:,}</b> &nbsp;·&nbsp;
        📡 Source: <b>{src}</b><br>
        ⚡ "Pre-cached (Instant)" select karo → <b>&lt;5 sec</b> mein data ready
      </span>
    </div>"""


# ── Main load function ────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_cache():
    """
    GitHub se 4 Parquet files load karo:
      close.parquet   → recent 40 months close prices
      high.parquet    → recent 40 months high prices
      volume.parquet  → recent 40 months close×volume
      ath.parquet     → ALL TIME HIGH (2000 to today max)

    ATH ko high DataFrame mein inject karta hai taaki
    calculations.py mein high.max() = correct ATH aaye.

    Returns:
      (close_df, high_with_ath_df, volume_df)
      — directly build_dfStats() mein pass karo

    Raises:
      Exception agar koi file load nahi hua
    """
    # ── Load all 4 files ─────────────────────────────────────
    close  = pd.read_parquet(_CLOSE_URL)
    high   = pd.read_parquet(_HIGH_URL)
    volume = pd.read_parquet(_VOL_URL)
    ath    = pd.read_parquet(_ATH_URL)

    # ── Index timezone strip karo ─────────────────────────────
    for df in (close, high, volume):
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df.index = df.index.tz_localize(None)

    # ── ATH row inject into high DataFrame ────────────────────
    # Reason: calculations.py mein `ATH = high.max()` use hota hai.
    # Hum 2000-01-01 timestamp pe ek synthetic row add karte hain
    # jisme ATH value hoti hai. Isse high.max() automatically
    # 2000-se-aaj-tak ka correct ATH deta hai.
    # calculations.py mein ZERO change.

    # ATH series ko high ke columns se align karo
    ath_series = ath["ATH"].reindex(high.columns)

    # Synthetic row — 2000-01-01
    ath_row = pd.DataFrame(
        [ath_series.values],
        columns=high.columns,
        index=[pd.Timestamp("2000-01-01")],
    )
    # Concat + sort
    high_with_ath = pd.concat([ath_row, high]).sort_index()

    # Duplicate columns remove karo (safety)
    close         = close.loc[:,         ~close.columns.duplicated()]
    high_with_ath = high_with_ath.loc[:, ~high_with_ath.columns.duplicated()]
    volume        = volume.loc[:,         ~volume.columns.duplicated()]

    return close, high_with_ath, volume
