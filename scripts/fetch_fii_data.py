#!/usr/bin/env python3
"""
scripts/fetch_fii_data.py  — v2
================================
NSE direct API GitHub Actions pe block ho jaata hai (empty response / 403).

Source priority (all free, no auth):
  1. Trendlyne FII/DII screener CSV   (most reliable, public)
  2. Moneycontrol FII data API        (fallback 1)
  3. Tickertape / Groww public data   (fallback 2)
  4. Previous stale data              (last resort)

Output → fii_data.json (same schema as before — app-compatible)
"""

import json, os, datetime, time, re
import requests

TODAY    = datetime.date.today().isoformat()
NOW_TS   = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
OUT_FILE = "fii_data.json"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.google.com/",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_prev() -> dict:
    try:
        if os.path.exists(OUT_FILE):
            with open(OUT_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def score_from_net(fii_net_30d: float) -> tuple[str, float]:
    """SOP v2026.08 scoring: >5000=BULL(1.0), 0-5000=MILD(0.5), <0=0.0"""
    if fii_net_30d > 5000:   return "BULL",      1.0
    elif fii_net_30d > 0:    return "MILD_BULL",  0.5
    elif fii_net_30d > -5000: return "NEUTRAL",   0.0
    else:                    return "BEAR",       0.0


def save(data: dict):
    with open(OUT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ {OUT_FILE} saved: {data.get('signal')} | score={data.get('score')} | "
          f"FII 30D=₹{data.get('fii_net_30d',0):,.0f}Cr")


def consecutive_bull(prev: dict, signal: str) -> int:
    p = prev.get("consecutive_bull_days", 0)
    ps = prev.get("signal", "NEUTRAL")
    if signal in ("BULL","MILD_BULL") and ps in ("BULL","MILD_BULL"):
        return p + 1
    return 1 if signal in ("BULL","MILD_BULL") else 0


# ── Source 1: Trendlyne FII/DII public data ───────────────────────────────────
def fetch_trendlyne() -> dict | None:
    """
    Trendlyne has public FII/DII data accessible without auth.
    URL: https://trendlyne.com/macro/fii-dii-data/
    We parse the JSON embedded in the page or use their API endpoint.
    """
    urls = [
        "https://trendlyne.com/api/macro/fii-dii/?format=json",
        "https://trendlyne.com/macro-data/fii-dii/latest/snapshot-pastmonth",
    ]
    session = requests.Session()
    session.headers.update(HEADERS)

    for url in urls:
        try:
            r = session.get(url, timeout=15)
            print(f"  Trendlyne {url}: HTTP {r.status_code}")
            if r.status_code != 200:
                continue

            # Try JSON parse
            try:
                data = r.json()
                if isinstance(data, list) and len(data) > 0:
                    return _parse_trendlyne_json(data)
                if isinstance(data, dict) and data.get("results"):
                    return _parse_trendlyne_json(data["results"])
            except Exception:
                pass

            # Try HTML parsing — look for embedded JSON
            txt = r.text
            m = re.search(r'fii_dii_data\s*=\s*(\[.*?\]);', txt, re.DOTALL)
            if m:
                entries = json.loads(m.group(1))
                return _parse_trendlyne_json(entries)

        except Exception as e:
            print(f"  Trendlyne error: {e}")

    return None


def _parse_trendlyne_json(entries: list) -> dict | None:
    """Parse Trendlyne FII/DII list entries."""
    if not entries:
        return None
    try:
        fii_30d = sum(float(e.get("fii_net", e.get("fiiNet", 0)) or 0) for e in entries[:30])
        fii_5d  = sum(float(e.get("fii_net", e.get("fiiNet", 0)) or 0) for e in entries[:5])
        dii_30d = sum(float(e.get("dii_net", e.get("diiNet", 0)) or 0) for e in entries[:30])
        dii_5d  = sum(float(e.get("dii_net", e.get("diiNet", 0)) or 0) for e in entries[:5])
        if fii_30d == 0 and dii_30d == 0:
            return None
        return {"fii_net_30d": round(fii_30d, 2), "fii_net_5d": round(fii_5d, 2),
                "dii_net_30d": round(dii_30d, 2), "dii_net_5d": round(dii_5d, 2),
                "source": "trendlyne"}
    except Exception as e:
        print(f"  Trendlyne parse error: {e}")
        return None


# ── Source 2: Moneycontrol FII/DII ────────────────────────────────────────────
def fetch_moneycontrol() -> dict | None:
    """Moneycontrol FII/DII page — parse embedded data."""
    session = requests.Session()
    session.headers.update({**HEADERS, "Referer": "https://www.moneycontrol.com/"})
    urls = [
        "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php",
        "https://www.moneycontrol.com/mc-api/prod/v1/mcforum/widget/fii-dii?type=monthly",
    ]
    for url in urls:
        try:
            r = session.get(url, timeout=15)
            print(f"  Moneycontrol {url[-50:]}: HTTP {r.status_code}")
            if r.status_code != 200:
                continue
            # Try JSON
            try:
                d = r.json()
                if d.get("data"):
                    return _parse_mc_json(d["data"])
            except Exception:
                pass
            # Try HTML — look for FII net numbers
            txt = r.text
            # Pattern: "Net Investment": "1234.56"
            nets = re.findall(r'"Net Investment"\s*:\s*"([+-]?[\d,]+\.?\d*)"', txt)
            if len(nets) >= 2:
                fii_vals = [float(v.replace(",","")) for v in nets[:5]]
                dii_vals = [float(v.replace(",","")) for v in nets[5:10]] if len(nets)>5 else []
                if any(v != 0 for v in fii_vals):
                    return {"fii_net_30d": round(sum(fii_vals), 2),
                            "fii_net_5d":  round(sum(fii_vals[:5]), 2),
                            "dii_net_30d": round(sum(dii_vals), 2) if dii_vals else 0,
                            "dii_net_5d":  0, "source": "moneycontrol"}
        except Exception as e:
            print(f"  Moneycontrol error: {e}")
    return None


def _parse_mc_json(data) -> dict | None:
    try:
        if isinstance(data, list):
            fii_30d = sum(float(e.get("fii_net_value", 0) or 0) for e in data[:30])
            dii_30d = sum(float(e.get("dii_net_value", 0) or 0) for e in data[:30])
            if fii_30d != 0:
                return {"fii_net_30d": round(fii_30d,2), "fii_net_5d": 0,
                        "dii_net_30d": round(dii_30d,2), "dii_net_5d": 0,
                        "source": "moneycontrol"}
    except Exception:
        pass
    return None


# ── Source 3: NSE India via different approach ─────────────────────────────────
def fetch_nse_with_selenium_approach() -> dict | None:
    """
    NSE blocks direct API calls from GitHub Actions IPs.
    Alternative: Use NSE's publicly accessible CSV download.
    URL: https://archives.nseindia.com/content/fo/fii_stats_DDMMYYYY.xls
    Or use BSE FII data as proxy.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    # Try BSE FII data (more accessible)
    try:
        today_obj = datetime.date.today()
        # Try last 5 trading days
        dates_to_try = []
        d = today_obj
        while len(dates_to_try) < 30:
            if d.weekday() < 5:  # weekday
                dates_to_try.append(d)
            d -= datetime.timedelta(days=1)

        fii_nets = []
        dii_nets = []

        for dt in dates_to_try[:30]:
            date_str = dt.strftime("%d%m%Y")
            url = f"https://archives.nseindia.com/content/fo/fii_stats_{date_str}.xls"
            try:
                r = session.get(url, timeout=8)
                if r.status_code == 200 and len(r.content) > 100:
                    # XLS file — try to parse
                    try:
                        import io
                        # Simple binary scan for numbers
                        content = r.content
                        # Mark as fetched
                        print(f"  NSE archive {date_str}: got {len(content)} bytes")
                        # For now just count as success
                        fii_nets.append(0)  # placeholder
                    except Exception:
                        pass
                time.sleep(0.3)
            except Exception:
                pass

        if len(fii_nets) > 0:
            return None  # Return None — let other sources handle it
    except Exception as e:
        print(f"  NSE archive: {e}")

    # Try BSE India FII stats
    try:
        r = session.get(
            "https://www.bseindia.com/markets/MarketInfo/FIIStat.aspx",
            timeout=15
        )
        if r.status_code == 200:
            txt = r.text
            # Look for FII net values in BSE page
            nets = re.findall(r'>([\-\d,]+\.\d+)<.*?Net Purchase', txt)
            if nets:
                vals = [float(v.replace(",","")) for v in nets[:5]]
                total = sum(vals)
                if total != 0:
                    print(f"  BSE FII: got {len(vals)} values, total={total:.0f}")
                    return {"fii_net_30d": round(total, 2), "fii_net_5d": round(sum(vals[:5]),2),
                            "dii_net_30d": 0, "dii_net_5d": 0, "source": "bse_india"}
    except Exception as e:
        print(f"  BSE error: {e}")

    return None


# ── Source 4: Investing.com public data ───────────────────────────────────────
def fetch_investing_com() -> dict | None:
    """Investing.com FII data — calendar economic events."""
    session = requests.Session()
    session.headers.update({**HEADERS,
        "Referer": "https://in.investing.com/",
        "X-Requested-With": "XMLHttpRequest",
    })
    try:
        r = session.get(
            "https://in.investing.com/economic-calendar/foreign-institutional-investors-net-purchases-943",
            timeout=15
        )
        print(f"  Investing.com: HTTP {r.status_code}")
        if r.status_code == 200:
            txt = r.text
            # Extract values from page
            vals = re.findall(r'data-value="([\-\d\.]+)"', txt)
            if vals:
                floats = [float(v) for v in vals[:30] if v]
                if floats:
                    total = sum(floats[:30])
                    # Convert from Cr if needed
                    return {"fii_net_30d": round(total, 2), "fii_net_5d": round(sum(floats[:5]),2),
                            "dii_net_30d": 0, "dii_net_5d": 0, "source": "investing_com"}
    except Exception as e:
        print(f"  Investing.com error: {e}")
    return None


# ── Source 5: Screener.in / alternative public Indian sources ─────────────────
def fetch_alternative_india() -> dict | None:
    """
    Try multiple alternative Indian financial data sources.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    # Try Tickertape
    try:
        r = session.get(
            "https://api.tickertape.in/market/fiidii?duration=1M",
            timeout=15,
            headers={**HEADERS, "Origin": "https://tickertape.in", "Referer": "https://tickertape.in/"}
        )
        print(f"  Tickertape: HTTP {r.status_code}")
        if r.status_code == 200:
            d = r.json()
            if d.get("data"):
                entries = d["data"]
                fii_30d = sum(float(e.get("fiiNet", 0) or 0) for e in entries[:30])
                dii_30d = sum(float(e.get("diiNet", 0) or 0) for e in entries[:30])
                if fii_30d != 0:
                    return {"fii_net_30d": round(fii_30d,2), "fii_net_5d": 0,
                            "dii_net_30d": round(dii_30d,2), "dii_net_5d": 0,
                            "source": "tickertape"}
    except Exception as e:
        print(f"  Tickertape error: {e}")

    # Try Groww public market data
    try:
        r = session.get(
            "https://groww.in/stocks/market-overview/fii-dii",
            timeout=15,
            headers={**HEADERS, "Referer": "https://groww.in/"}
        )
        print(f"  Groww: HTTP {r.status_code}")
        if r.status_code == 200:
            txt = r.text
            # Look for JSON in page
            m = re.search(r'"fiiNet"\s*:\s*([\-\d\.]+)', txt)
            if m:
                fii_val = float(m.group(1))
                print(f"  Groww FII value: {fii_val}")
                return {"fii_net_30d": fii_val, "fii_net_5d": 0,
                        "dii_net_30d": 0, "dii_net_5d": 0,
                        "source": "groww"}
    except Exception as e:
        print(f"  Groww error: {e}")

    # Try StockEdge public API
    try:
        r = session.get(
            "https://app.stockedge.com/api/StockEdge/GetFIIDIIDetails",
            timeout=15,
            headers={**HEADERS, "Referer": "https://app.stockedge.com/"}
        )
        print(f"  StockEdge: HTTP {r.status_code}")
        if r.status_code == 200:
            d = r.json()
            entries = d if isinstance(d, list) else d.get("data", d.get("Data", []))
            if entries:
                fii_30d = sum(float(e.get("FIINet", e.get("fiiNet", 0)) or 0) for e in entries[:30])
                if fii_30d != 0:
                    dii_30d = sum(float(e.get("DIINet", e.get("diiNet", 0)) or 0) for e in entries[:30])
                    return {"fii_net_30d": round(fii_30d,2), "fii_net_5d": 0,
                            "dii_net_30d": round(dii_30d,2), "dii_net_5d": 0,
                            "source": "stockedge"}
    except Exception as e:
        print(f"  StockEdge error: {e}")

    return None


# ── Source 6: NSE India proper session with delay ─────────────────────────────
def fetch_nse_proper() -> dict | None:
    """
    NSE with proper session — slower, more realistic browser simulation.
    Sometimes works even from GitHub Actions.
    """
    session = requests.Session()
    # Step 1: Get cookies
    try:
        r0 = session.get("https://www.nseindia.com/", timeout=15, headers=HEADERS)
        print(f"  NSE homepage: HTTP {r0.status_code} | cookies: {list(session.cookies.keys())}")
        time.sleep(2)

        # Step 2: Hit a non-API page first
        r1 = session.get("https://www.nseindia.com/market-data/fii-dii-trading-activity",
                         timeout=15, headers={**HEADERS, "Referer": "https://www.nseindia.com/"})
        print(f"  NSE FII page: HTTP {r1.status_code}")
        time.sleep(1.5)

        # Step 3: Try API
        r2 = session.get(
            "https://www.nseindia.com/api/fiidiiTradeReact",
            timeout=15,
            headers={
                **HEADERS,
                "Referer": "https://www.nseindia.com/market-data/fii-dii-trading-activity",
                "X-Requested-With": "XMLHttpRequest",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            }
        )
        print(f"  NSE API: HTTP {r2.status_code} | len={len(r2.content)}")

        if r2.status_code == 200 and len(r2.content) > 10:
            data = r2.json()
            if isinstance(data, list) and len(data) > 0:
                print(f"  NSE proper: got {len(data)} entries")
                entries = data
                fii_30d  = sum(float(e.get("fiiNet",  0) or 0) for e in entries[:30])
                fii_5d   = sum(float(e.get("fiiNet",  0) or 0) for e in entries[:5])
                fii_buy  = sum(float(e.get("fiiBuy",  0) or 0) for e in entries[:30])
                fii_sell = sum(float(e.get("fiiSell", 0) or 0) for e in entries[:30])
                dii_30d  = sum(float(e.get("diiNet",  0) or 0) for e in entries[:30])
                dii_5d   = sum(float(e.get("diiNet",  0) or 0) for e in entries[:5])
                if fii_30d != 0 or fii_buy != 0:
                    return {"fii_net_30d": round(fii_30d,2), "fii_net_5d": round(fii_5d,2),
                            "fii_buy_30d": round(fii_buy,2), "fii_sell_30d": round(fii_sell,2),
                            "dii_net_30d": round(dii_30d,2), "dii_net_5d": round(dii_5d,2),
                            "source": "nse_api"}
    except Exception as e:
        print(f"  NSE proper error: {e}")
    return None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    prev = load_prev()
    print(f"Previous: {prev.get('last_updated','none')} | score={prev.get('score','?')} | signal={prev.get('signal','?')}")

    fetched = None

    print("\n[1] Trying NSE proper session...")
    fetched = fetch_nse_proper()

    if fetched is None:
        print("\n[2] Trying Trendlyne...")
        fetched = fetch_trendlyne()

    if fetched is None:
        print("\n[3] Trying Tickertape / Groww / StockEdge...")
        fetched = fetch_alternative_india()

    if fetched is None:
        print("\n[4] Trying Moneycontrol...")
        fetched = fetch_moneycontrol()

    if fetched is None:
        print("\n⚠️  All sources failed. Writing stale data.")
        out = {**prev,
               "last_updated": TODAY, "last_updated_ts": NOW_TS,
               "stale": True, "fetch_failed": True}
        save(out)
        return

    # ── Build output ──────────────────────────────────────────────────────────
    fii_30d = fetched.get("fii_net_30d", 0)
    fii_5d  = fetched.get("fii_net_5d",  0)
    dii_30d = fetched.get("dii_net_30d", 0)
    dii_5d  = fetched.get("dii_net_5d",  0)

    # EMA approximation from 5D net (simple)
    prev_ema5  = prev.get("fii_net_5d",  0)
    ema5_gt_20 = fii_5d > prev_ema5

    signal, score = score_from_net(fii_30d)
    consec       = consecutive_bull(prev, signal)

    out = {
        "last_updated":          TODAY,
        "last_updated_ts":       NOW_TS,
        "source":                fetched.get("source", "unknown"),
        "fii_net_30d":           fii_30d,
        "fii_net_5d":            fii_5d,
        "fii_buy_30d":           fetched.get("fii_buy_30d",  0),
        "fii_sell_30d":          fetched.get("fii_sell_30d", 0),
        "dii_net_30d":           dii_30d,
        "dii_net_5d":            dii_5d,
        "fii_ema5_gt_ema20":     ema5_gt_20,
        "signal":                signal,
        "score":                 score,
        "stale":                 False,
        "consecutive_bull_days": consec,
        "fetch_failed":          False,
    }
    save(out)


if __name__ == "__main__":
    main()
