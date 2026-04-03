"""
upstox_auto_auth.py
===================
Correct flow discovered from browser logs:
  - Login domain: login.upstox.com (NOT api-v2.upstox.com)
  - Auth code redirect: goes to REDIRECT_URI with ?code=...
  - For automation: REDIRECT_URI must be https://127.0.0.1/
    (requests catches the ConnectionError and extracts code from URL)

IMPORTANT: REDIRECT_URI must be https://127.0.0.1/ in:
  1. Upstox Developer Console (app settings)
  2. GitHub Secret UPSTOX_REDIRECT_URI

SECURITY: PIN, TOTP, token kabhi logs mein nahi dikhte.
"""

import os
import re
import time
import urllib.parse

import pyotp
import requests
from requests.exceptions import ConnectionError as ReqConnError

_LOGIN_BASE  = "https://login.upstox.com"
_TOKEN_URL   = "https://api.upstox.com/v2/login/authorization/token"
_AUTH_DIALOG = "https://api.upstox.com/v2/login/authorization/dialog"

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _mask(v: str, n: int = 4) -> str:
    return (v[:n] + "***") if v else "***"

def _safe_log(msg: str):
    print(f"[upstox_auth] {msg}", flush=True)

def _extract_code(url: str):
    try:
        p = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        return (p.get("code") or [None])[0]
    except Exception:
        return None


def _exchange_code(session, auth_code, client_id, client_secret, redirect_uri) -> str:
    _safe_log("Step 4: Exchanging code for access_token...")
    try:
        resp = session.post(
            _TOKEN_URL,
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


def get_upstox_token_automated(
    client_id, client_secret, redirect_uri,
    mobile, pin, totp_secret
) -> str:

    # REDIRECT_URI check — must be 127.0.0.1 for automation
    if "127.0.0.1" not in redirect_uri and "localhost" not in redirect_uri:
        _safe_log(
            f"  WARNING: REDIRECT_URI = {redirect_uri[:30]}..."
        )
        _safe_log(
            "  For automation, REDIRECT_URI should be https://127.0.0.1/"
        )
        _safe_log(
            "  Change it in: Upstox Dev Console + UPSTOX_REDIRECT_URI Secret"
        )

    session = requests.Session()
    session.headers.update({
        "User-Agent":      _BROWSER_UA,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })

    # ── Step 1: Load login page → session cookies ──────────────
    _safe_log("Step 1: Loading Upstox login page...")

    login_url = (
        f"{_LOGIN_BASE}/"
        f"?client_id={client_id}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
        f"&response_type=code"
        f"&state=auto"
    )

    try:
        r1 = session.get(login_url, allow_redirects=True, timeout=20)
        _safe_log(f"  Status: {r1.status_code} | URL: {r1.url[:60]}")
        _safe_log(f"  Cookies: {list(session.cookies.keys())}")
    except Exception as e:
        # Try dialog URL as fallback
        _safe_log(f"  login.upstox.com failed ({type(e).__name__}) — trying dialog URL...")
        try:
            r1 = session.get(
                _AUTH_DIALOG,
                params={"response_type": "code", "client_id": client_id, "redirect_uri": redirect_uri},
                allow_redirects=True, timeout=20,
            )
            _safe_log(f"  Dialog status: {r1.status_code}")
        except Exception as e2:
            raise RuntimeError(f"Step 1 failed: {type(e2).__name__}") from None

    time.sleep(1.5)

    # x-api-key = client_id (confirmed from udapi_api_key cookie = client_id)
    x_api_key = client_id

    def _headers_for(origin: str) -> dict:
        return {
            "Accept":       "application/json, */*",
            "Content-Type": "application/json",
            "x-api-key":    x_api_key,
            "Origin":       origin,
            "Referer":      r1.url,
        }

    # ── Step 2: Send OTP / mobile verify ─────────────────────
    _safe_log(f"Step 2: Send OTP to {_mask(mobile, 3)}*****...")
    step2_ok = False

    step2_candidates = [
        (f"{_LOGIN_BASE}/api/v1/mobile/otp",          "login.upstox.com"),
        (f"{_LOGIN_BASE}/api/v1/send-otp",             "login.upstox.com"),
        (f"{_LOGIN_BASE}/api/mobile/otp",              "login.upstox.com"),
        ("https://api-v2.upstox.com/user/v1/otp",     "api-v2.upstox.com"),
    ]
    mobile_keys = ["mobile_num", "mobileNum", "mobile", "phone"]

    for ep, origin in step2_candidates:
        if step2_ok:
            break
        for mkey in mobile_keys[:2]:
            try:
                r2 = session.post(
                    ep,
                    json={mkey: mobile},
                    headers=_headers_for(f"https://{origin}"),
                    allow_redirects=False,
                    timeout=15,
                )
                _safe_log(f"  {ep.split('/')[-1]}+{mkey}: HTTP {r2.status_code}")

                if r2.status_code in (200, 201):
                    step2_ok = True
                    _safe_log("  OTP sent ✅")
                    break
                elif r2.status_code == 404:
                    break  # endpoint doesn't exist, try next
                else:
                    try:
                        errs = r2.json().get("errors") or []
                        ec   = errs[0].get("errorCode", "") if errs else ""
                        emsg = errs[0].get("message", "")   if errs else ""
                        if ec:
                            _safe_log(f"    {ec}: {emsg}")
                    except Exception:
                        pass
            except Exception as e:
                _safe_log(f"  {type(e).__name__}")

    if not step2_ok:
        _safe_log("  Step 2 inconclusive — trying direct login (Step 3)...")

    time.sleep(2)

    # ── Step 3: Submit PIN + TOTP ─────────────────────────────
    _safe_log("Step 3: Login with MPIN+TOTP (values masked)...")

    step3_candidates = [
        (f"{_LOGIN_BASE}/api/v1/login",         "login.upstox.com"),
        (f"{_LOGIN_BASE}/api/v1/otp/verify",    "login.upstox.com"),
        (f"{_LOGIN_BASE}/api/login",            "login.upstox.com"),
        (f"{_LOGIN_BASE}/api/v1/auth",          "login.upstox.com"),
        ("https://api-v2.upstox.com/user/v1/login", "api-v2.upstox.com"),
    ]
    pin_keys = ["mpin", "client_secret", "pin", "password"]

    def _do_login(totp_val: str):
        for ep, origin in step3_candidates:
            for pkey in pin_keys:
                pld = {"mobile_num": mobile, pkey: pin, "totp": totp_val}
                try:
                    r3 = session.post(
                        ep, json=pld,
                        headers=_headers_for(f"https://{origin}"),
                        allow_redirects=False,
                        timeout=20,
                    )
                    _safe_log(f"  {ep.split('/')[-1]}+{pkey}: HTTP {r3.status_code}")

                    # 404 = endpoint not found, skip to next
                    if r3.status_code == 404:
                        break

                    try:
                        body = r3.json()
                        bkeys = [k for k in body.keys() if k != "errors"]
                        if bkeys:
                            _safe_log(f"    Body keys: {list(body.keys())}")

                        # Extract auth_code from various response formats
                        data = body.get("data", {})
                        if isinstance(data, dict):
                            for k in ("redirect_url", "redirectUrl", "redirect", "code"):
                                val = data.get(k, "")
                                if val:
                                    code = _extract_code(val) if "redirect" in k else val
                                    if code:
                                        _safe_log(f"    auth_code from data.{k} ✅")
                                        return code

                        for k in ("redirect_url", "redirectUrl", "code"):
                            val = body.get(k, "")
                            if val:
                                code = _extract_code(val) if "redirect" in k else val
                                if code:
                                    _safe_log(f"    auth_code from {k} ✅")
                                    return code

                        if r3.status_code >= 400:
                            errs = body.get("errors") or []
                            ec   = errs[0].get("errorCode", "") if errs else ""
                            emsg = errs[0].get("message", "")   if errs else ""
                            if ec:
                                _safe_log(f"    Error: {ec} — {emsg}")

                    except Exception:
                        pass

                    # Location header (redirect to redirect_uri)
                    loc = r3.headers.get("Location", "")
                    if loc:
                        code = _extract_code(loc)
                        if code:
                            _safe_log(f"    auth_code from Location ✅")
                            return code

                    # HTML scan
                    if r3.status_code == 200 and r3.text:
                        m = re.search(r"[?&]code=([^&\"'\s]+)", r3.text)
                        if m:
                            return m.group(1)

                except ReqConnError as ce:
                    # Connection to 127.0.0.1 refused = SUCCESS!
                    # Extract code from the URL it tried to connect to
                    err_str = str(ce)
                    code = _extract_code(err_str)
                    if not code:
                        m = re.search(r"code=([^&\"'\s&]+)", err_str)
                        if m:
                            code = m.group(1)
                    if code:
                        _safe_log(f"    auth_code from 127.0.0.1 redirect ✅")
                        return code
                    _safe_log(f"    Connection error (127.0.0.1 redirect): {type(ce).__name__}")

                except Exception as e:
                    _safe_log(f"  {type(e).__name__}")

                time.sleep(0.2)
        return None

    totp_code = pyotp.TOTP(totp_secret).now()
    auth_code = _do_login(totp_code)

    if not auth_code:
        _safe_log("  Waiting 31s for fresh TOTP window...")
        time.sleep(31)
        totp_code = pyotp.TOTP(totp_secret).now()
        _safe_log("  Retry with fresh TOTP (masked)...")
        auth_code = _do_login(totp_code)

    if not auth_code:
        raise RuntimeError(
            "Upstox auth failed.\n\n"
            "SOLUTION — REDIRECT_URI change karo:\n"
            "  1. developer.upstox.com → My Apps → App edit\n"
            "     Redirect URL change karo: https://127.0.0.1/\n"
            "  2. GitHub Secret UPSTOX_REDIRECT_URI = https://127.0.0.1/\n"
            "  3. Workflow dobara run karo\n\n"
            "Yeh zaroori hai kyunki automation mein browser nahi hota\n"
            "jo Streamlit URL pe redirect handle kare.\n"
            "Values logs mein nahi dikhti."
        )

    return _exchange_code(session, auth_code, client_id, client_secret, redirect_uri)


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
