#!/usr/bin/env python3
"""
scripts/fetch_fii_data.py
==========================
Run by fetch_fii_data.yml — Mon-Fri 7 PM IST, Saturday 9 AM IST

Data source priority:
  1. NSE India official API  (primary)
  2. NSE alternate endpoint  (fallback 1)
  3. Stale previous data     (fallback 2 — keeps last known values)

Output → fii_data.json:
{
  "last_updated":     "2026-07-14",
  "last_updated_ts":  "2026-07-14T19:05:12",
  "source":           "nse_api",
  "fii_net_30d":      8542,       # FII net buy/sell (Cr) — rolling 30 trading days
  "fii_net_5d":       1203,       # FII net (Cr) — last 5 trading days
  "fii_buy_30d":      145200,
  "fii_sell_30d":     136658,
  "dii_net_30d":     -2341,       # DII net (Cr) — negative = DII buying when FII selling
  "dii_net_5d":       -890,
  "fii_ema5_gt_ema20": true,      # FII 5-day EMA > 20-day EMA (trend direction)
  "signal":           "BULL",     # BULL | MILD_BULL | NEUTRAL | BEAR
  "score":            1.0,        # 0.0 | 0.5 | 1.0  (used in S_FII signal)
  "stale":            false,      # true if using previous cached data
  "consecutive_bull_days": 3      # helps avoid whipsaw signals
}

Score mapping (from SOP v2026.07/08):
  FII 30D net > +5,000 Cr   → BULL      → 1.0 pt
  FII 30D net  0 to +5,000  → MILD_BULL → 0.5 pt
  FII 30D net -5,000 to 0   → NEUTRAL   → 0.0 pt
  FII 30D net < -5,000      → BEAR      → 0.0 pt  (no negative score, floor at 0)
"""

import json
import os
import datetime
import time
import requests

# ── Constants ─────────────────────────────────────────────────────────────────
FII_DATA_FILE = "fii_data.json"
TODAY         = datetime.date.today().isoformat()
NOW_TS        = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

# NSE API headers — required to avoid 403
NSE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}

# NSE API endpoints (in priority order)
NSE_ENDPOINTS = [
    # Primary: NSE FII/DII activity page API
    "https://www.nseindia.com/api/fiidiiTradeReact",
    # Fallback: NSE market activity
    "https://www.nseindia.com/api/market-data-pre-open?key=FO",
]

# ── Load previous data (for stale fallback + history) ────────────────────────
def load_previous() -> dict:
    try:
        if os.path.exists(FII_DATA_FILE):
            with open(FII_DATA_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


# ── NSE session setup (required — NSE needs cookie from homepage first) ────────
def get_nse_session() -> requests.Session:
    """Create a session with NSE cookies. Required for API calls."""
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    try:
        # Warm up — get cookies from NSE homepage first
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(1)
    except Exception as e:
        print(f"NSE session warmup failed: {e}")
    return session


# ── Fetch FII/DII from NSE primary API ────────────────────────────────────────
def fetch_nse_fii_primary(session: requests.Session) -> dict | None:
    """
    Fetch from https://www.nseindia.com/api/fiidiiTradeReact
    Returns processed dict or None on failure.
    """
    try:
        r = session.get(
            "https://www.nseindia.com/api/fiidiiTradeReact",
            timeout=15,
        )
        if r.status_code != 200:
            print(f"NSE primary API: HTTP {r.status_code}")
            return None

        data = r.json()
        # NSE returns list of daily entries, most recent first
        # Each entry: {"date":"14-Jul-2026","fiiNet":1203.45,"diiNet":-234.12,...}
        if not isinstance(data, list) or len(data) == 0:
            print(f"NSE primary: unexpected format {type(data)}")
            return None

        print(f"NSE primary: {len(data)} days of data received")

        # Extract rolling windows
        entries_30d = data[:30]  # last 30 trading days
        entries_5d  = data[:5]   # last 5 trading days

        def safe_net(entry, key="fiiNet"):
            try: return float(entry.get(key, 0) or 0)
            except: return 0.0

        fii_net_30d = round(sum(safe_net(e, "fiiNet") for e in entries_30d), 2)
        fii_net_5d  = round(sum(safe_net(e, "fiiNet") for e in entries_5d),  2)
        fii_buy_30d = round(sum(safe_net(e, "fiiBuy")  for e in entries_30d), 2)
        fii_sell_30d= round(sum(safe_net(e, "fiiSell") for e in entries_30d), 2)
        dii_net_30d = round(sum(safe_net(e, "diiNet") for e in entries_30d), 2)
        dii_net_5d  = round(sum(safe_net(e, "diiNet") for e in entries_5d),  2)

        # EMA 5 vs EMA 20 on daily FII net
        def ema(values, period):
            if len(values) < period:
                return sum(values) / len(values) if values else 0
            k = 2 / (period + 1)
            ema_val = sum(values[:period]) / period
            for v in values[period:]:
                ema_val = v * k + ema_val * (1 - k)
            return ema_val

        daily_fii_nets = [safe_net(e, "fiiNet") for e in data[:30]]
        ema5  = ema(daily_fii_nets[:5],  5)
        ema20 = ema(daily_fii_nets[:20], 20)
        ema5_gt_ema20 = ema5 > ema20

        return {
            "source":        "nse_api",
            "fii_net_30d":   fii_net_30d,
            "fii_net_5d":    fii_net_5d,
            "fii_buy_30d":   fii_buy_30d,
            "fii_sell_30d":  fii_sell_30d,
            "dii_net_30d":   dii_net_30d,
            "dii_net_5d":    dii_net_5d,
            "ema5_gt_ema20": ema5_gt_ema20,
            "raw_entries":   len(data),
        }

    except Exception as e:
        print(f"NSE primary fetch error: {e}")
        return None


# ── Fallback: NSE FII/DII alternate endpoint ──────────────────────────────────
def fetch_nse_fii_alternate(session: requests.Session) -> dict | None:
    """
    Alternate: Parse NSE FII/DII page data from a different endpoint.
    Uses nse market stats which sometimes has FII data embedded.
    """
    try:
        r = session.get(
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
            timeout=15,
        )
        if r.status_code != 200:
            return None
        # This endpoint doesn't have FII data directly, but confirms NSE connectivity
        # If we reach here, NSE is accessible — try the FII specific endpoint differently
        r2 = session.get(
            "https://www.nseindia.com/api/fiidiiTradeReact?type=fii",
            timeout=15,
        )
        if r2.status_code == 200:
            data = r2.json()
            if isinstance(data, list) and data:
                return fetch_nse_fii_primary(session)  # reuse parser with fresh session
        return None
    except Exception as e:
        print(f"NSE alternate fetch error: {e}")
        return None


# ── Score computation ──────────────────────────────────────────────────────────
def compute_score(fii_net_30d: float, prev: dict) -> tuple[str, float]:
    """
    Convert FII 30D net (Cr) to S_FII signal + score.
    From SOP v2026.07/08:
      > +5,000 Cr → BULL      → 1.0 pt
      0 to +5,000 → MILD_BULL → 0.5 pt
      -5,000 to 0 → NEUTRAL   → 0.0 pt
      < -5,000    → BEAR      → 0.0 pt (no negative, floor 0)
    """
    if fii_net_30d > 5000:
        signal, score = "BULL", 1.0
    elif fii_net_30d > 0:
        signal, score = "MILD_BULL", 0.5
    elif fii_net_30d > -5000:
        signal, score = "NEUTRAL", 0.0
    else:
        signal, score = "BEAR", 0.0

    return signal, score


def compute_consecutive_bull_days(raw_entries_count: int, prev: dict,
                                  signal: str) -> int:
    """Track consecutive bull days for whipsaw protection."""
    prev_signal = prev.get("signal", "NEUTRAL")
    prev_consec = prev.get("consecutive_bull_days", 0)
    if signal in ("BULL", "MILD_BULL"):
        return prev_consec + 1 if prev_signal in ("BULL", "MILD_BULL") else 1
    return 0


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    prev = load_previous()
    print(f"Previous data: {prev.get('last_updated','none')} | "
          f"score={prev.get('score','?')} | signal={prev.get('signal','?')}")

    # Try fetching fresh data
    fetched = None
    session = get_nse_session()

    print("Trying NSE primary API...")
    fetched = fetch_nse_fii_primary(session)

    if fetched is None:
        print("Primary failed. Trying NSE alternate...")
        time.sleep(2)
        fetched = fetch_nse_fii_alternate(session)

    if fetched is None:
        print("⚠️  Both NSE endpoints failed. Using stale previous data.")
        out = {**prev, "stale": True, "last_updated": TODAY,
               "last_updated_ts": NOW_TS, "fetch_failed": True}
        # Preserve previous score but mark as stale
        with open(FII_DATA_FILE, "w") as f:
            json.dump(out, f, indent=2)
        print(f"Stale data written: {out}")
        return

    # Compute signal + score
    fii_30d  = fetched["fii_net_30d"]
    signal, score = compute_score(fii_30d, prev)
    consec   = compute_consecutive_bull_days(fetched.get("raw_entries", 0), prev, signal)

    out = {
        "last_updated":          TODAY,
        "last_updated_ts":       NOW_TS,
        "source":                fetched["source"],
        "fii_net_30d":           fii_30d,
        "fii_net_5d":            fetched["fii_net_5d"],
        "fii_buy_30d":           fetched.get("fii_buy_30d", 0),
        "fii_sell_30d":          fetched.get("fii_sell_30d", 0),
        "dii_net_30d":           fetched["dii_net_30d"],
        "dii_net_5d":            fetched["dii_net_5d"],
        "fii_ema5_gt_ema20":     fetched.get("ema5_gt_ema20", False),
        "signal":                signal,
        "score":                 score,
        "stale":                 False,
        "consecutive_bull_days": consec,
        "fetch_failed":          False,
    }

    with open(FII_DATA_FILE, "w") as f:
        json.dump(out, f, indent=2)

    print(f"✅ fii_data.json saved:")
    print(f"   FII 30D net: ₹{fii_30d:,.0f} Cr | 5D: ₹{fetched['fii_net_5d']:,.0f} Cr")
    print(f"   DII 30D net: ₹{fetched['dii_net_30d']:,.0f} Cr")
    print(f"   Signal: {signal} | Score: {score} | EMA5>EMA20: {fetched.get('ema5_gt_ema20')}")
    print(f"   Consecutive bull days: {consec}")


if __name__ == "__main__":
    main()
