"""
upstox_auto_auth.py
===================
Playwright headless browser se Upstox OAuth automation.

Kyun Playwright:
  - Upstox login page JS execute karta hai (session tokens, CSRF, fingerprinting)
  - requests library ye sab miss karta hai → 500/401 errors
  - Playwright actual Chrome chalata hai → sab automatically handle hota hai

Flow:
  1. Browser launch → login.upstox.com load
  2. Mobile number fill + submit (OTP send)
  3. TOTP from pyotp fill (SMS OTP nahi — TOTP authenticator)
  4. MPIN fill + submit
  5. Redirect se auth_code extract
  6. code → access_token

SECURITY: PIN, TOTP, token kabhi logs mein nahi dikhte.
"""

import os
import re
import time
import urllib.parse

import pyotp
import requests


def _mask(v: str, n: int = 4) -> str:
    return (v[:n] + "***") if v else "***"

def _safe_log(msg: str):
    print(f"[upstox_auth] {msg}", flush=True)

def _extract_code(url: str):
    try:
        p = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        c = p.get("code", [None])[0]
        if c:
            return c
    except Exception:
        pass
    m = re.search(r"[?&]code=([^&\"'\s]+)", str(url))
    return m.group(1) if m else None


def _exchange_code(auth_code, client_id, client_secret, redirect_uri) -> str:
    _safe_log("Step final: Exchanging code for access_token...")
    try:
        resp = requests.post(
            "https://api.upstox.com/v2/login/authorization/token",
            data={
                "code":          auth_code,
                "client_id":     client_id,
                "client_secret": client_secret,
                "redirect_uri":  redirect_uri,
                "grant_type":    "authorization_code",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept":       "application/json",
            },
            timeout=20,
        )
        resp.raise_for_status()
        token = resp.json().get("access_token", "")
        if not token:
            raise RuntimeError("access_token missing in response")
        _safe_log(f"  Token: {_mask(token)} (len={len(token)}) ✅")
        return token
    except requests.HTTPError as e:
        raise RuntimeError(
            f"Token exchange HTTP {e.response.status_code}. "
            "CLIENT_ID / CLIENT_SECRET / REDIRECT_URI check karo."
        ) from None
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Token exchange error: {type(e).__name__}") from None


def _auth_with_playwright(
    client_id, client_secret, redirect_uri,
    mobile, pin, totp_secret
) -> str:
    """Playwright headless Chrome se Upstox login."""
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    login_url = (
        f"https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code"
        f"&client_id={client_id}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
    )

    _safe_log("Playwright: Launching headless Chrome...")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ]
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 720},
        )
        page = context.new_page()

        auth_code = None

        try:
            # ── Step 1: Load login page ────────────────────────
            _safe_log(f"  Step 1: Loading login page...")
            page.goto(login_url, wait_until="networkidle", timeout=30000)
            _safe_log(f"  Page URL: {page.url[:60]}")
            time.sleep(2)

            # ── Step 2: Fill mobile number ─────────────────────
            _safe_log(f"  Step 2: Filling mobile {_mask(mobile, 3)}*****...")

            # Mobile input — various selectors Upstox uses
            mobile_selectors = [
                'input[name="mobileNum"]',
                'input[name="mobile_num"]',
                'input[name="mobileNumber"]',
                'input[type="tel"]',
                'input[placeholder*="mobile" i]',
                'input[placeholder*="phone" i]',
                'input[placeholder*="Mobile" i]',
            ]
            mobile_filled = False
            for sel in mobile_selectors:
                try:
                    elem = page.wait_for_selector(sel, timeout=3000)
                    if elem:
                        elem.fill(mobile)
                        mobile_filled = True
                        _safe_log(f"  Mobile filled via: {sel}")
                        break
                except PWTimeout:
                    continue

            if not mobile_filled:
                _safe_log("  Could not find mobile input — taking screenshot for debug")
                page.screenshot(path="/tmp/upstox_step2_debug.png")
                raise RuntimeError("Mobile input field not found on page")

            # Submit mobile
            submit_selectors = [
                'button[type="submit"]',
                'button:has-text("Continue")',
                'button:has-text("Get OTP")',
                'button:has-text("Send OTP")',
                'button:has-text("Next")',
            ]
            for sel in submit_selectors:
                try:
                    btn = page.wait_for_selector(sel, timeout=2000)
                    if btn:
                        btn.click()
                        _safe_log(f"  Clicked: {sel}")
                        break
                except PWTimeout:
                    continue

            time.sleep(3)

            # ── Step 3: TOTP field ─────────────────────────────
            _safe_log("  Step 3: Checking for TOTP field...")
            totp_code = pyotp.TOTP(totp_secret).now()
            # Note: totp_code value not logged

            totp_selectors = [
                'input[name="totp"]',
                'input[placeholder*="TOTP" i]',
                'input[placeholder*="authenticator" i]',
                'input[placeholder*="Google" i]',
                'input[name="otp"]',
                'input[placeholder*="OTP" i]',
                'input[type="number"]',
                'input[maxlength="6"]',
            ]
            totp_filled = False
            for sel in totp_selectors:
                try:
                    elem = page.wait_for_selector(sel, timeout=3000)
                    if elem:
                        elem.fill(totp_code)
                        totp_filled = True
                        _safe_log(f"  TOTP filled (value masked)")
                        break
                except PWTimeout:
                    continue

            if not totp_filled:
                _safe_log("  TOTP field not found yet — may appear after mobile submit")

            time.sleep(2)

            # ── Step 4: PIN field ──────────────────────────────
            _safe_log("  Step 4: Filling PIN (value masked)...")

            pin_selectors = [
                'input[name="pin"]',
                'input[name="mpin"]',
                'input[name="client_secret"]',
                'input[type="password"]',
                'input[placeholder*="PIN" i]',
                'input[placeholder*="password" i]',
                'input[placeholder*="MPIN" i]',
            ]
            pin_filled = False
            for sel in pin_selectors:
                try:
                    elem = page.wait_for_selector(sel, timeout=3000)
                    if elem:
                        elem.fill(pin)
                        pin_filled = True
                        _safe_log(f"  PIN filled via: {sel}")
                        break
                except PWTimeout:
                    continue

            if not pin_filled:
                _safe_log("  PIN field not found — may appear on next page")

            # If TOTP not filled before, try again now
            if not totp_filled:
                totp_code = pyotp.TOTP(totp_secret).now()
                for sel in totp_selectors:
                    try:
                        elem = page.wait_for_selector(sel, timeout=2000)
                        if elem:
                            elem.fill(totp_code)
                            totp_filled = True
                            _safe_log("  TOTP filled (value masked)")
                            break
                    except PWTimeout:
                        continue

            # Submit login
            time.sleep(1)
            for sel in submit_selectors + ['button:has-text("Login")', 'button:has-text("Sign in")', 'input[type="submit"]']:
                try:
                    btn = page.wait_for_selector(sel, timeout=2000)
                    if btn and btn.is_visible():
                        btn.click()
                        _safe_log(f"  Login submitted via: {sel}")
                        break
                except PWTimeout:
                    continue

            # ── Step 5: Wait for redirect → extract code ───────
            _safe_log("  Step 5: Waiting for redirect with auth_code...")
            max_wait = 15
            for i in range(max_wait):
                current_url = page.url
                code = _extract_code(current_url)
                if code:
                    _safe_log(f"  auth_code extracted from redirect ✅")
                    auth_code = code
                    break

                # Check if we're on 127.0.0.1 error page
                if "127.0.0.1" in current_url or "localhost" in current_url:
                    code = _extract_code(current_url)
                    if code:
                        auth_code = code
                        _safe_log(f"  auth_code from 127.0.0.1 redirect ✅")
                        break

                time.sleep(1)
                _safe_log(f"  Waiting... ({i+1}/{max_wait}) URL: {current_url[:50]}")

            if not auth_code:
                # Take screenshot for debugging
                page.screenshot(path="/tmp/upstox_final_debug.png")
                _safe_log(f"  Final URL: {page.url}")
                _safe_log("  Screenshot saved to /tmp/upstox_final_debug.png")

        finally:
            browser.close()

        return auth_code


def get_upstox_token_automated(
    client_id, client_secret, redirect_uri,
    mobile, pin, totp_secret
) -> str:

    if "127.0.0.1" not in redirect_uri and "localhost" not in redirect_uri:
        _safe_log(
            "WARNING: REDIRECT_URI should be https://127.0.0.1/ for automation!\n"
            "  developer.upstox.com → App → Edit → Redirect URI = https://127.0.0.1/\n"
            "  GitHub Secret UPSTOX_REDIRECT_URI = https://127.0.0.1/"
        )

    _safe_log("Starting Playwright headless auth...")
    auth_code = _auth_with_playwright(
        client_id, client_secret, redirect_uri,
        mobile, pin, totp_secret
    )

    if not auth_code:
        raise RuntimeError(
            "Upstox auth failed — auth_code nahi mila.\n"
            "Checks:\n"
            "  1. REDIRECT_URI = https://127.0.0.1/ (developer console + GitHub Secret)\n"
            "  2. TOTP verify: python -c \"import pyotp; print(pyotp.TOTP('SECRET').now())\"\n"
            "  3. UPSTOX_PIN — 6-digit MPIN\n"
            "  4. UPSTOX_MOBILE — 10-digit bina +91\n"
            "Values logs mein nahi dikhti."
        )

    return _exchange_code(auth_code, client_id, client_secret, redirect_uri)


def get_token_from_env() -> str:
    required = [
        "UPSTOX_CLIENT_ID", "UPSTOX_CLIENT_SECRET", "UPSTOX_REDIRECT_URI",
        "UPSTOX_MOBILE", "UPSTOX_PIN", "UPSTOX_TOTP_SECRET",
    ]
    missing = [k for k in required if not os.getenv(k, "").strip()]
    if missing:
        raise RuntimeError(
            f"GitHub Secrets missing: {missing}\n"
            "Settings → Secrets → Actions → New repository secret"
        )
    return get_upstox_token_automated(
        client_id     = os.environ["UPSTOX_CLIENT_ID"],
        client_secret = os.environ["UPSTOX_CLIENT_SECRET"],
        redirect_uri  = os.environ["UPSTOX_REDIRECT_URI"],
        mobile        = os.environ["UPSTOX_MOBILE"],
        pin           = os.environ["UPSTOX_PIN"],
        totp_secret   = os.environ["UPSTOX_TOTP_SECRET"],
    )
