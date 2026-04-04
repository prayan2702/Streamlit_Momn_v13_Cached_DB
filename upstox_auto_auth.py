"""
upstox_auto_auth.py
===================
Playwright headless browser — fixed based on actual screenshots.

Page flow confirmed:
  Page 1: Mobile input (#mobileNum) + "Get OTP" button
  Page 2: "Enter OTP or TOTP" — single input id="otpNum"
           + "Continue" button
  Page 3: MPIN input id="pinCode" + submit → redirect with auth_code

ROOT CAUSE OF PREVIOUS FAILURE:
  page.route() intercepts resource requests (XHR/fetch/images) but NOT
  top-level navigation requests. Upstox redirects the page itself to
  https://127.0.0.1/?code=... which is a navigation, so route() never fires.

FIX (v3):
  Use page.expect_navigation(wait_until="commit") BEFORE clicking Continue.
  "commit" fires the instant the browser commits to the new URL — before
  any page content loads, and before Chrome tries to connect to 127.0.0.1
  and errors out. page.url at that moment contains ?code=...

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
    _safe_log("Final step: Exchanging code for access_token...")
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


def _fill_first_visible_input(page, value: str, label: str) -> bool:
    try:
        inputs = page.query_selector_all("input")
        for inp in inputs:
            try:
                if inp.is_visible() and inp.is_enabled():
                    itype = inp.get_attribute("type") or "text"
                    if itype.lower() not in ("hidden", "submit", "button", "checkbox", "radio"):
                        inp.fill(value)
                        _safe_log(f"  {label} filled in first visible input ✅")
                        return True
            except Exception:
                continue
    except Exception as e:
        _safe_log(f"  fill_first_visible_input error: {type(e).__name__}")
    return False


def _log_page_state(page, step: str):
    try:
        inputs = page.eval_on_selector_all(
            "input",
            "els => els.filter(e => e.offsetParent !== null).map(e => "
            "({type: e.type, id: e.id, name: e.name, placeholder: e.placeholder}))"
        )
        buttons = page.eval_on_selector_all(
            "button",
            "els => els.filter(e => e.offsetParent !== null)"
            ".map(e => e.textContent.trim()).filter(t => t)"
        )
        _safe_log(f"  [{step}] Inputs: {inputs}")
        _safe_log(f"  [{step}] Buttons: {buttons[:5]}")
    except Exception as e:
        _safe_log(f"  [{step}] Debug failed: {type(e).__name__}")


def _click_button(page, texts: list, timeout: int = 5000) -> bool:
    for text in texts:
        try:
            btn = page.wait_for_selector(
                f'button:has-text("{text}")',
                timeout=timeout, state="visible"
            )
            if btn:
                btn.click()
                _safe_log(f"  Clicked: '{text}' ✅")
                return True
        except Exception:
            continue
    return False


def _auth_with_playwright(
    client_id, client_secret, redirect_uri,
    mobile, pin, totp_secret
) -> str:

    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    login_url = (
        "https://api.upstox.com/v2/login/authorization/dialog"
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
            # ── Page 1: Mobile number ──────────────────────────
            _safe_log("Page 1: Loading login page...")
            page.goto(login_url, wait_until="networkidle", timeout=30000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(2)
            _safe_log(f"  URL: {page.url[:60]}")
            _log_page_state(page, "Page1")
            page.screenshot(path="/tmp/upstox_page1.png")

            _safe_log(f"  Filling mobile {_mask(mobile, 3)}*****...")
            filled = False
            for sel in ["#mobileNum", 'input[id="mobileNum"]',
                        'input[type="text"]', 'input[type="tel"]']:
                try:
                    elem = page.wait_for_selector(sel, timeout=4000, state="visible")
                    if elem:
                        elem.click()
                        elem.fill(mobile)
                        _safe_log(f"  Mobile via: {sel} ✅")
                        filled = True
                        break
                except PWTimeout:
                    continue

            if not filled:
                filled = _fill_first_visible_input(page, mobile, "Mobile")
            if not filled:
                page.screenshot(path="/tmp/upstox_mobile_fail.png")
                raise RuntimeError("Mobile input nahi mila")

            time.sleep(0.5)
            if not _click_button(page, ["Get OTP", "SEND OTP", "Send OTP", "Continue", "Next"]):
                page.keyboard.press("Enter")
            time.sleep(4)

            # ── Page 2: OTP / TOTP entry ───────────────────────
            _safe_log("Page 2: OTP/TOTP entry...")
            page.screenshot(path="/tmp/upstox_page2.png")
            _log_page_state(page, "Page2")

            totp_code = pyotp.TOTP(totp_secret).now()

            totp_filled = False
            for sel in ['#otpNum', 'input[id="otpNum"]', 'input[name="totp"]',
                        'input[name="otp"]', 'input[placeholder*="OTP" i]',
                        'input[placeholder*="TOTP" i]', 'input[maxlength="6"]',
                        'input[type="number"]', 'input[type="tel"]', 'input[type="text"]']:
                try:
                    elem = page.wait_for_selector(sel, timeout=3000, state="visible")
                    if elem:
                        elem.click()
                        elem.fill(totp_code)
                        _safe_log(f"  TOTP via: {sel} (value masked) ✅")
                        totp_filled = True
                        break
                except PWTimeout:
                    continue

            if not totp_filled:
                totp_filled = _fill_first_visible_input(page, totp_code, "TOTP")
            if not totp_filled:
                _safe_log("  WARNING: TOTP field not filled")

            time.sleep(0.5)
            if not _click_button(page, ["Continue", "Verify", "Submit", "Next", "Proceed"]):
                page.keyboard.press("Enter")
            time.sleep(3)

            # ── Page 3: MPIN entry ─────────────────────────────
            _safe_log("Page 3: MPIN/PIN entry...")
            page.screenshot(path="/tmp/upstox_page3.png")
            _log_page_state(page, "Page3")

            pin_filled = False
            for sel in ['#pinCode', 'input[id="pinCode"]', 'input[name="mpin"]',
                        'input[name="pin"]', 'input[name="client_secret"]',
                        'input[type="password"]', 'input[placeholder*="PIN" i]',
                        'input[placeholder*="MPIN" i]', 'input[maxlength="6"]',
                        'input[type="number"]', 'input[type="text"]']:
                try:
                    elem = page.wait_for_selector(sel, timeout=3000, state="visible")
                    if elem:
                        elem.click()
                        elem.fill(pin)
                        _safe_log(f"  PIN via: {sel} (value masked) ✅")
                        pin_filled = True
                        break
                except PWTimeout:
                    continue

            if not pin_filled:
                pin_filled = _fill_first_visible_input(page, pin, "PIN")
            if not pin_filled:
                _safe_log("  WARNING: PIN field not filled")

            time.sleep(0.5)

            # ── KEY FIX v3: expect_navigation(wait_until="commit") ─────────
            # PROBLEM: page.route() only intercepts sub-resource requests
            #   (XHR, fetch, images). It does NOT intercept top-level page
            #   navigations. Upstox does a full page redirect to
            #   https://127.0.0.1/?code=... so route() never fires.
            #
            # SOLUTION: expect_navigation(wait_until="commit") opens a context
            #   manager that resolves the INSTANT the browser commits to
            #   navigating to the new URL — before any page content is fetched,
            #   and before Chrome tries (and fails) to connect to 127.0.0.1.
            #   Reading page.url at that exact moment gives us ?code=...
            # ──────────────────────────────────────────────────────────────────
            _safe_log("  Submitting PIN — waiting for redirect (expect_navigation commit)...")

            redirect_url_captured = None
            try:
                with page.expect_navigation(
                    url=re.compile(r"(127\.0\.0\.1|localhost)"),
                    wait_until="commit",
                    timeout=15000,
                ):
                    clicked = _click_button(
                        page, ["Continue", "Login", "Submit", "Proceed", "Verify"]
                    )
                    if not clicked:
                        page.keyboard.press("Enter")

                # Context manager exited = navigation URL committed
                redirect_url_captured = page.url
                _safe_log(f"  expect_navigation fired ✅ — URL captured")

            except PWTimeout:
                _safe_log("  expect_navigation timed out — trying plain click + fallback poll...")
                # Button may not have been clicked yet if timeout before click
                _click_button(page, ["Continue", "Login", "Submit", "Proceed", "Verify"])

            # Extract from committed navigation URL
            if redirect_url_captured:
                auth_code = _extract_code(redirect_url_captured)
                if auth_code:
                    _safe_log("  auth_code extracted from navigation URL ✅")

            # Fallback poll if expect_navigation missed
            if not auth_code:
                _safe_log("  Fallback: polling page.url for 10s...")
                for i in range(10):
                    try:
                        url = page.url
                        if ("127.0.0.1" in url or "localhost" in url) and "code=" in url:
                            auth_code = _extract_code(url)
                            if auth_code:
                                _safe_log(f"  auth_code from fallback poll ✅")
                                break
                        _safe_log(f"  fallback ({i+1}/10): {url[:70]}")
                    except Exception:
                        _safe_log(f"  fallback ({i+1}/10): page.url error")
                    time.sleep(1)

            try:
                page.screenshot(path="/tmp/upstox_midwait.png")
                page.screenshot(path="/tmp/upstox_final_debug.png")
                _safe_log(f"  Final URL: {page.url[:80]}")
            except Exception as e:
                _safe_log(f"  Final screenshot/url failed: {type(e).__name__}")

        finally:
            try:
                browser.close()
            except Exception:
                pass

    return auth_code


def get_upstox_token_automated(
    client_id, client_secret, redirect_uri,
    mobile, pin, totp_secret
) -> str:
    if "127.0.0.1" not in redirect_uri and "localhost" not in redirect_uri:
        _safe_log(
            f"WARNING: REDIRECT_URI={redirect_uri} — should be https://127.0.0.1/\n"
            "  developer.upstox.com → App → Edit → Redirect URI = https://127.0.0.1/\n"
            "  GitHub Secret UPSTOX_REDIRECT_URI = https://127.0.0.1/"
        )

    auth_code = _auth_with_playwright(
        client_id, client_secret, redirect_uri,
        mobile, pin, totp_secret
    )

    if not auth_code:
        raise RuntimeError(
            "Upstox auth failed — auth_code nahi mila.\n"
            "Checks:\n"
            "  1. REDIRECT_URI = https://127.0.0.1/ (dev console + GitHub Secret)\n"
            "  2. TOTP: python -c \"import pyotp; print(pyotp.TOTP('SECRET').now())\"\n"
            "     Upstox app ke OTP se match karna chahiye\n"
            "  3. UPSTOX_PIN — 6-digit MPIN\n"
            "  4. UPSTOX_MOBILE — 10-digit bina +91"
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
