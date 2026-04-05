"""
cache_loader_upstox.py
======================
cache_loader.py ka Upstox version.
GitHub raw URLs se Parquet files load karta hai — cache_upstox/ folder se.
(YFinance cache = cache/, Upstox cache = cache_upstox/ — alag folders!)

Usage (Step 2 mein):
    from cache_loader_upstox import load_cache, get_cache_meta, get_cache_age_days, get_cache_status_html

    meta = get_cache_meta()
    close, high, volume = load_cache()
"""

import json
import requests
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime, date

# ── GitHub raw base URL — Upstox cache folder ─────────────────
_GITHUB_CACHE = (
    "https://raw.githubusercontent.com/"
    "prayan2702/Streamlit_Momn_v13_Cached_DB/refs/heads/main/cache_upstox"
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
    cache_upstox/cache_meta.json load karo.
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
    return get_cache_age_days() <= max_days


def get_cache_status_html() -> str:
    """
    Step 1 mein dikhane ke liye Upstox cache status card HTML.
    Green = 0-1 din | Amber = 2-3 din | Red = 4+ din
    """
    meta = get_cache_meta()
    age  = get_cache_age_days()

    if "error" in meta:
        return f"""
        <div style="background:#fee2e2;border:1px solid #fca5a5;border-left:4px solid #dc2626;
                    border-radius:10px;padding:12px 16px;font-size:13px;color:#7f1d1d;margin:10px 0;">
          ❌ <b>Upstox Cache load failed:</b> {meta['error']}<br>
          <span style="font-size:11px;">Cache abhi build nahi hua — daily_cache_upstox.yml workflow run karo.</span>
        </div>"""

    if not meta or "build_date" not in meta:
        return """
        <div style="background:#fef3c7;border:1px solid #fcd34d;border-left:4px solid #d97706;
                    border-radius:10px;padding:12px 16px;font-size:13px;color:#92400e;margin:10px 0;">
          ⚠️ <b>Upstox Cache not found yet.</b> GitHub Actions daily_cache_upstox.yml pehli baar chalegi.
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

    return f"""
    <div style="background:{color};border:1px solid {bdr};border-left:4px solid {bdr};
                border-radius:10px;padding:12px 16px;font-size:13px;color:{text};margin:10px 0;">
      {icon} <b>Upstox Pre-cached Data</b> &nbsp;·&nbsp; {freshness}<br>
      <span style="font-size:11.5px;margin-top:4px;display:block;">
        📅 Build date: <b>{build_dt}</b> &nbsp;·&nbsp;
        📋 Symbols: <b>{fetched:,}</b> &nbsp;·&nbsp;
        ❌ Failed: <b>{failed_ct}</b><br>
        📡 Data: <b>{src}</b> &nbsp;·&nbsp;
        📋 Symbol list: <b>{sym_src}</b> (+ GOLDBEES &amp; SILVERBEES)<br>
        ⚡ "Pre-cached Upstox (Instant)" select karo → <b>&lt;10 sec</b> mein data ready
      </span>
    </div>"""


# ── Main load function ────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_cache():
    """
    GitHub se cache_upstox/ folder ke 4 Parquet files load karo:
      close.parquet   → recent 40 months close prices  (Upstox V3 — split/bonus adjusted)
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

    # ── ROOT CAUSE FIX: Holiday / weekend last-row NaN ────────
    # `cache_builder_upstox.py` mein `pd.bdate_range` weekdays include
    # karta hai — lekin Indian market holidays (Good Friday, Diwali etc.)
    # ko nahi jaanta. Agar last bdate entry ek market holiday hai, to
    # Upstox ka us date pe koi data nahi hoga → reindex() → last row = NaN.
    # `calculations.py`: `Close = data12M.iloc[-1]` → all NaN (AWAY_ATH bhi)
    # roc12M NaN nahi aata kyunki getAbsReturns() andar ffill/bfill karta hai.
    #
    # Fix: trailing all-NaN rows drop karo, phir forward-fill karo.
    # Isse existing cache bhi sahi kaam karta hai — bina rebuild ke!
    close  = close.sort_index().dropna(how='all').ffill()
    volume = volume.sort_index().dropna(how='all').ffill()
    # high: sort only (ATH row inject baad mein hogi; dropna mat karo)
    high   = high.sort_index()

    # ── ATH row inject into high DataFrame ────────────────────
    # calculations.py mein `ATH = high.max()` use hota hai.
    # Hum 2000-01-01 timestamp pe ek synthetic row add karte hain
    # jisme ATH value hoti hai. Isse high.max() automatically
    # 2000-se-aaj-tak ka correct ATH deta hai.
    ath_series = ath["ATH"].reindex(high.columns)

    ath_row = pd.DataFrame(
        [ath_series.values],
        columns=high.columns,
        index=[pd.Timestamp("2000-01-01")],
    )
    high_with_ath = pd.concat([ath_row, high]).sort_index()

    # Duplicate columns remove karo (safety)
    close         = close.loc[:,         ~close.columns.duplicated()]
    high_with_ath = high_with_ath.loc[:, ~high_with_ath.columns.duplicated()]
    volume        = volume.loc[:,         ~volume.columns.duplicated()]

    return close, high_with_ath, volume
