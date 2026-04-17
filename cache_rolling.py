"""
cache_rolling.py
================
5-day rolling cache manager — YFinance, Upstox, Angel One teeno ke liye.
Sirf cache builders use karte hain isko (GitHub Actions pe chalta hai).
Cache loaders GitHub raw URLs se fetch karte hain — is file ka use nahi karte.

Folder structure (teeno sources ke liye same pattern):
  cache/                       ← YFinance
  cache_upstox/                ← Upstox
  cache_angelone/              ← Angel One
    ├── cache_index.json       ← {"dates": ["2026-04-14",...], "latest": "2026-04-16"}
    ├── 2026-04-14/
    │   ├── close.parquet
    │   ├── high.parquet
    │   ├── volume.parquet
    │   ├── ath.parquet
    │   └── cache_meta.json
    └── 2026-04-16/  ...  (max 5 dirs — 6th build pe oldest auto-pruned)
"""

import json
import shutil
from datetime import datetime
from pathlib import Path

MAX_CACHED_DAYS = 5


def log_default(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def read_cache_index(cache_dir: Path) -> dict:
    """cache_index.json read karo. Missing/corrupt ho to empty dict."""
    index_path = cache_dir / "cache_index.json"
    if index_path.exists():
        try:
            return json.loads(index_path.read_text())
        except Exception:
            pass
    return {"dates": [], "latest": ""}


def write_cache_index(cache_dir: Path, dates: list) -> dict:
    """Sorted dates list se cache_index.json write karo."""
    idx = {
        "dates":  sorted(dates),
        "latest": sorted(dates)[-1] if dates else "",
    }
    (cache_dir / "cache_index.json").write_text(json.dumps(idx, indent=2))
    return idx


def save_rolling_cache(
    cache_dir : Path,
    today_str : str,
    close,
    high,
    volume,
    ath_df,
    meta      : dict,
    log_fn    = None,
) -> list:
    """
    Dated subfolder mein parquets + meta save karo.
    MAX_CACHED_DAYS se zyada ho to oldest auto-prune.

    Returns:
        list: Available cache dates (sorted oldest → latest)
    """
    if log_fn is None:
        log_fn = log_default

    cache_dir.mkdir(exist_ok=True)
    day_dir = cache_dir / today_str
    day_dir.mkdir(exist_ok=True)

    # ── Parquet + meta save ─────────────────────────────────────
    log_fn(f"Saving to {cache_dir.name}/{today_str}/...")
    close.to_parquet(day_dir  / "close.parquet")
    high.to_parquet(day_dir   / "high.parquet")
    volume.to_parquet(day_dir / "volume.parquet")
    ath_df.to_parquet(day_dir / "ath.parquet")
    with open(day_dir / "cache_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    for fname in ["close.parquet", "high.parquet", "volume.parquet", "ath.parquet"]:
        size_mb = (day_dir / fname).stat().st_size / 1_048_576
        log_fn(f"  {fname}: {size_mb:.1f} MB")

    # ── Existing dates merge (index + disk scan) ────────────────
    idx      = read_cache_index(cache_dir)
    existing = idx.get("dates", [])
    disk = [
        d.name for d in cache_dir.iterdir()
        if d.is_dir() and len(d.name) == 10 and d.name.count("-") == 2
    ]
    all_dates = sorted(set(existing + disk + [today_str]))

    # ── Prune oldest beyond MAX_CACHED_DAYS ─────────────────────
    if len(all_dates) > MAX_CACHED_DAYS:
        to_remove = all_dates[:-MAX_CACHED_DAYS]
        for old in to_remove:
            old_dir = cache_dir / old
            if old_dir.exists():
                shutil.rmtree(old_dir)
                log_fn(f"  🗑️  Pruned: {old}")
        all_dates = all_dates[-MAX_CACHED_DAYS:]

    write_cache_index(cache_dir, all_dates)
    log_fn(f"  📅 Index updated: {all_dates}")
    return all_dates
