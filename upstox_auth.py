"""
upstox_auth.py
==============
Upstox OAuth2 Access Token Manager for Streamlit apps.

Flow:
  1. First run  → shows login URL → user pastes auth_code → token saved
  2. Next runs  → token loaded from cache (JSON file)
  3. Token expired (daily reset at midnight IST) → auto re-prompts

Setup:
  Add these to .streamlit/secrets.toml  (or Streamlit Cloud Secrets):
      [upstox]
      api_key    = "YOUR_API_KEY"
      api_secret = "YOUR_API_SECRET"
      redirect_uri = "https://your-app.streamlit.app/"   # must match Upstox dev console
"""

import os
import json
import time
import requests
import streamlit as st
from datetime import datetime, timezone
from pathlib import Path

# ── Token cache file (local runs). On Streamlit Cloud use st.session_state only ──
TOKEN_CACHE_FILE = Path(".upstox_token_cache.json")


# ─────────────────────────────────────────────────────────────
# Helpers: save / load token from disk cache
# ─────────────────────────────────────────────────────────────
def _save_token_cache(token_data: dict):
    """Persist token to local JSON file (dev use only)."""
    try:
        TOKEN_CACHE_FILE.write_text(json.dumps(token_data))
    except Exception:
        pass   # silently skip on read-only filesystems (Streamlit Cloud)


def _load_token_cache() -> dict | None:
    """Load token from local JSON file if it exists."""
    try:
        if TOKEN_CACHE_FILE.exists():
            return json.loads(TOKEN_CACHE_FILE.read_text())
    except Exception:
        pass
    return None


def _is_token_valid(token_data: dict) -> bool:
    """
    Upstox access tokens expire at midnight IST every day.
    We store 'expires_at' (UTC epoch) when we fetch the token.
    """
    if not token_data or "access_token" not in token_data:
        return False
    expires_at = token_data.get("expires_at", 0)
    return time.time() < expires_at


# ─────────────────────────────────────────────────────────────
# Step 1: Build OAuth authorization URL
# ─────────────────────────────────────────────────────────────
def get_auth_url(api_key: str, redirect_uri: str) -> str:
    """Returns the Upstox login URL user must visit to get auth_code."""
    return (
        f"https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code"
        f"&client_id={api_key}"
        f"&redirect_uri={redirect_uri}"
    )


# ─────────────────────────────────────────────────────────────
# Step 2: Exchange auth_code for access_token
# ─────────────────────────────────────────────────────────────
def exchange_code_for_token(auth_code: str, api_key: str,
                             api_secret: str, redirect_uri: str) -> dict:
    """
    POST to Upstox token endpoint.
    Returns token_data dict with access_token + expires_at.
    Raises requests.HTTPError on failure.
    """
    url  = "https://api.upstox.com/v2/login/authorization/token"
    resp = requests.post(url, data={
        "code":          auth_code,
        "client_id":     api_key,
        "client_secret": api_secret,
        "redirect_uri":  redirect_uri,
        "grant_type":    "authorization_code",
    }, headers={"Accept": "application/json"})
    resp.raise_for_status()

    data = resp.json()
    # Upstox token is valid until midnight IST → approx 24h from generation
    # We conservatively set expiry to 23 hours from now
    data["expires_at"] = time.time() + (23 * 3600)
    return data


# ─────────────────────────────────────────────────────────────
# Main public function: get a valid access_token
# ─────────────────────────────────────────────────────────────
def get_upstox_access_token(sidebar: bool = True) -> str | None:
    """
    Returns a valid Upstox access_token, or None if user hasn't authenticated yet.

    Renders Streamlit UI in sidebar (or main area) to guide the user through
    the one-time-per-day OAuth flow.

    Usage in data_service.py:
        from upstox_auth import get_upstox_access_token
        token = get_upstox_access_token()
    """
    # ── Read credentials from st.secrets ────────────────────────
    try:
        api_key      = st.secrets["upstox"]["api_key"]
        api_secret   = st.secrets["upstox"]["api_secret"]
        redirect_uri = st.secrets["upstox"]["redirect_uri"]
    except (KeyError, FileNotFoundError):
        st.error("⚠️ Upstox credentials not found in `.streamlit/secrets.toml`. "
                 "Please add `[upstox]` section with `api_key`, `api_secret`, `redirect_uri`.")
        return None

    # ── 1. Check session_state first (fastest) ──────────────────
    cached = st.session_state.get("upstox_token_data")
    if _is_token_valid(cached):
        return cached["access_token"]

    # ── 2. Check disk cache ─────────────────────────────────────
    disk_cache = _load_token_cache()
    if _is_token_valid(disk_cache):
        st.session_state["upstox_token_data"] = disk_cache
        return disk_cache["access_token"]

    # ── 3. Need fresh token: show OAuth UI ──────────────────────
    container = st.sidebar if sidebar else st

    container.warning("🔐 **Upstox token expired or not found.** Please log in.")
    auth_url = get_auth_url(api_key, redirect_uri)

    container.markdown(
        f"**Step 1:** [Click here to log in to Upstox]({auth_url})\n\n"
        f"After login, Upstox will redirect to your redirect URL with `?code=XXXX` in the address bar."
    )
    auth_code = container.text_input(
        "**Step 2:** Paste the `code` value from the redirect URL here:",
        key="upstox_auth_code_input",
        placeholder="e.g. abc123xyz"
    )

    if container.button("🔓 Generate Access Token", key="upstox_auth_btn"):
        if not auth_code.strip():
            container.error("Please paste the auth code first.")
            return None
        try:
            with st.spinner("Exchanging code for token…"):
                token_data = exchange_code_for_token(
                    auth_code.strip(), api_key, api_secret, redirect_uri
                )
            st.session_state["upstox_token_data"] = token_data
            _save_token_cache(token_data)
            container.success("✅ Upstox authenticated successfully!")
            st.rerun()
        except Exception as e:
            container.error(f"Token generation failed: {e}")
            return None

    return None   # not yet authenticated


# ─────────────────────────────────────────────────────────────
# Utility: clear token (force re-login)
# ─────────────────────────────────────────────────────────────
def logout_upstox():
    """Call this to force re-authentication (e.g. from a sidebar button)."""
    st.session_state.pop("upstox_token_data", None)
    if TOKEN_CACHE_FILE.exists():
        TOKEN_CACHE_FILE.unlink()
