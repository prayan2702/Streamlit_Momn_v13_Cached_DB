"""
upstox_auto_auth.py
===================
Playwright headless browser — fixed based on actual screenshots.

Page flow confirmed:
  Page 1: Mobile input (#mobileNum) + "Get OTP" button
  Page 2: "Enter OTP or TOTP" — single plain <input> (no name/placeholder)
           + "Continue" button
  Page 3: MPIN input + submit → redirect with auth_code

FIX (v2): After PIN submit, Upstox redirects to https://127.0.0.1/?code=...
  Since no server runs on 127.0.0.1, Chrome crashes to chrome-error://
  before page.url can be read. Solution: intercept the redirect request
  via page.route() and page.on("framenavigated") BEFORE PIN submit,
  capture auth_code from the URL, and abort the failed navigation.

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
    """Page pe jo pehla visible input ho, usme value daalo."""
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
    """Debug: log all inputs and buttons."""
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
    """Try multiple button texts, click first found."""
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

        # ── KEY FIX: Intercept redirect to 127.0.0.1 / localhost ──────────
        # Upstox sends auth_code as ?code= param in the redirect URL.
        # Since no server runs on 127.0.0.1, Chrome crashes to chrome-error://
        # BEFORE page.url can be polled. We intercept & abort the request
        # here so we capture the URL before Chrome ever tries to load it.

        captured_redirect_url: list = []  # mutable container for closure

        def _intercept_redirect(route):
            url = route.request.url
            _safe_log(f"  [route] Intercepted: {url[:80]}")
            code = _extract_code(url)
            if code and not captured_redirect_url:
                captured_redirect_url.append(url)
                _safe_log("  [route] auth_code captured via route interception ✅")
            # Abort the request — no server on 127.0.0.1, so this prevents
            # chrome-error:// and the URL being lost
            try:
                route.abort()
            except Exception:
                pass

        # Also listen on framenavigated as a belt-and-suspenders backup
        def _on_frame_navigated(frame):
            if frame != page.main_frame:
                return
            url = frame.url
            if ("127.0.0.1" in url or "localhost" in url) and "code=" in url:
                code = _extract_code(url)
                if code and not captured_redirect_url:
                    captured_redirect_url.append(url)
                    _safe_log(f"  [framenavigated] auth_code captured ✅")

        # Route patterns cover http and https variants of 127.0.0.1 / localhost
        for pattern in [
            "**/127.0.0.1/**",
            "**/127.0.0.1*",
            "**/localhost/**",
            "**/localhost*",
        ]:
            try:
                page.route(pattern, _intercept_redirect)
            except Exception:
                pass

        page.on("framenavigated", _on_frame_navigated)
        # ─────────────────────────────────────────────────────────────────

        try:
            # ── Page 1: Mobile number ──────────────────────────
            _safe_log("Page 1: Loading login page...")
            page.goto(login_url, wait_until="networkidle", timeout=30000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(2)
            _safe_log(f"  URL: {page.url[:60]}")
            _log_page_state(page, "Page1")
            page.screenshot(path="/tmp/upstox_page1.png")

            # Fill mobile — confirmed: id="mobileNum"
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

            # Click "Get OTP"
            if not _click_button(page, ["Get OTP", "SEND OTP", "Send OTP",
                                        "Continue", "Next"]):
                _safe_log("  No button clicked — trying Enter key...")
                page.keyboard.press("Enter")
            time.sleep(4)  # OTP delivery wait

            # ── Page 2: OTP / TOTP entry ───────────────────────
            _safe_log("Page 2: OTP/TOTP entry...")
            page.screenshot(path="/tmp/upstox_page2.png")
            _log_page_state(page, "Page2")

            # Generate TOTP
            totp_code = pyotp.TOTP(totp_secret).now()
            # TOTP value not logged

            # Fill TOTP — confirmed: id="otpNum", type="text"
            totp_filled = False
            totp_selectors = [
                '#otpNum',
                'input[id="otpNum"]',
                'input[name="totp"]',
                'input[name="otp"]',
                'input[placeholder*="OTP" i]',
                'input[placeholder*="TOTP" i]',
                'input[maxlength="6"]',
                'input[type="number"]',
                'input[type="tel"]',
                'input[type="text"]',
            ]
            for sel in totp_selectors:
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
                _safe_log("  Trying fill_first_visible_input for TOTP...")
                totp_filled = _fill_first_visible_input(page, totp_code, "TOTP")

            if not totp_filled:
                _safe_log("  WARNING: TOTP field not filled")

            time.sleep(0.5)

            # Click Continue
            if not _click_button(page, ["Continue", "Verify", "Submit",
                                        "Next", "Proceed"]):
                page.keyboard.press("Enter")
            time.sleep(3)

            # ── Page 3: MPIN entry ─────────────────────────────
            _safe_log("Page 3: MPIN/PIN entry...")
            page.screenshot(path="/tmp/upstox_page3.png")
            _log_page_state(page, "Page3")

            # Fill PIN — confirmed: id="pinCode", type="password"
            pin_selectors = [
                '#pinCode',
                'input[id="pinCode"]',
                'input[name="mpin"]',
                'input[name="pin"]',
                'input[name="client_secret"]',
                'input[type="password"]',
                'input[placeholder*="PIN" i]',
                'input[placeholder*="MPIN" i]',
                'input[maxlength="6"]',
                'input[type="number"]',
                'input[type="text"]',
            ]
            pin_filled = False
            for sel in pin_selectors:
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
                _safe_log("  Trying fill_first_visible_input for PIN...")
                pin_filled = _fill_first_visible_input(page, pin, "PIN")

            if not pin_filled:
                _safe_log("  WARNING: PIN field not filled")

            time.sleep(0.5)

            # Submit — route interceptor fires here during/after click
            _safe_log("  Submitting PIN — watching for redirect...")
            if not _click_button(page, ["Continue", "Login", "Submit",
                                        "Proceed", "Verify"]):
                page.keyboard.press("Enter")

            # ── Wait for intercepted redirect URL ──────────────
            # Poll captured_redirect_url (filled by route interceptor)
            # Max ~15 seconds; interceptor fires almost instantly on redirect
            _safe_log("Waiting for auth_code from redirect interceptor...")
            for i in range(15):
                if captured_redirect_url:
                    auth_code = _extract_code(captured_redirect_url[0])
                    if auth_code:
                        _safe_log(f"  auth_code extracted from intercepted URL ✅")
                        break

                # Fallback: also check page.url in case browser is slow
                try:
                    url = page.url
                    if ("127.0.0.1" in url or "localhost" in url) and "code=" in url:
                        auth_code = _extract_code(url)
                        if auth_code:
                            _safe_log(f"  auth_code from page.url fallback ✅")
                            break
                except Exception:
                    pass

                if i == 8:
                    try:
                        page.screenshot(path="/tmp/upstox_midwait.png")
                        _log_page_state(page, f"MidWait-{i}")
                    except Exception:
                        pass

                time.sleep(1)
                _safe_log(f"  ({i+1}/15) waiting... captured={bool(captured_redirect_url)}")

            try:
                page.screenshot(path="/tmp/upstox_final_debug.png")
                _safe_log(f"  Final URL: {page.url[:80]}")
            except Exception:
                _safe_log("  Final screenshot failed (page may be closed)")

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
