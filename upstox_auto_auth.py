"""
upstox_auto_auth.py
===================
CORRECT COMPLETE FLOW (confirmed from browser DevTools):

Step 1: GET login.upstox.com → session cookies
Step 2: POST service.upstox.com/login/open/v7/auth/1fa/otp/generate
        body: {mobileNumber: mobile}
        → OTP sent to phone (automation mein skip — MPIN+TOTP use karo)
Step 3: POST service.upstox.com/login/open/v7/auth/2fa/totp
        body: {mobileNumber, mpin, totp}
        → auth_identity_token cookie set hoti hai
Step 4: GET service.upstox.com/gateway-worker/v1/verify-access-token
        ?client_id=CLIENT_ID&redirect_uri=https://127.0.0.1/&response_type=code
        → redirects to https://127.0.0.1/?code=AUTH_CODE
        → requests catches ConnectionError → extract code from URL
Step 5: POST token endpoint → access_token

CRITICAL: REDIRECT_URI must be https://127.0.0.1/
  - Upstox Developer Console mein bhi set karo
  - GitHub Secret UPSTOX_REDIRECT_URI = https://127.0.0.1/

SECURITY: PIN, TOTP, token kabhi logs mein nahi dikhte.
"""

import os
import re
import time
import uuid
import urllib.parse

import pyotp
import requests
from requests.exceptions import ConnectionError as ReqConnError

_SERVICE   = "https://service.upstox.com"
_LOGIN_URL = "https://login.upstox.com"
_TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token"
_AUTH_DIALOG = "https://api.upstox.com/v2/login/authorization/dialog"

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)


def _mask(v: str, n: int = 4) -> str:
    return (v[:n] + "***") if v else "***"

def _safe_log(msg: str):
    print(f"[upstox_auth] {msg}", flush=True)

def _extract_code(text: str):
    try:
        # Try as URL first
        p = urllib.parse.parse_qs(urllib.parse.urlparse(text).query)
        c = p.get("code", [None])[0]
        if c:
            return c
    except Exception:
        pass
    # Fallback: regex
    m = re.search(r"[?&]code=([^&\"'\s]+)", text)
    return m.group(1) if m else None

def _make_device_details() -> str:
    dev_uuid = str(uuid.uuid4())
    return (
        f"platform=WEB|osName=Windows/10|osVersion=Chrome/146.0.0.0|"
        f"appVersion=4.0.0|modelName=Chrome|manufacturer=unknown|"
        f"uuid={dev_uuid}|userAgent=Upstox 3.0 {_BROWSER_UA}"
    )

def _make_request_id() -> str:
    return "WPRO-" + uuid.uuid4().hex[:12]


def _exchange_code(session, auth_code, client_id, client_secret, redirect_uri) -> str:
    _safe_log("Step 5: Exchanging code for access_token...")
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

    # REDIRECT_URI validation
    if "127.0.0.1" not in redirect_uri and "localhost" not in redirect_uri:
        _safe_log(f"  WARNING: REDIRECT_URI = {redirect_uri}")
        _safe_log(
            "  For automation, REDIRECT_URI = https://127.0.0.1/ zaroori hai!\n"
            "  1. developer.upstox.com → App → Edit → Redirect URL = https://127.0.0.1/\n"
            "  2. GitHub Secret UPSTOX_REDIRECT_URI = https://127.0.0.1/"
        )

    device_details = _make_device_details()
    request_id     = _make_request_id()

    session = requests.Session()
    session.headers.update({
        "User-Agent":      _BROWSER_UA,
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
    })

    # Common service.upstox.com headers
    svc_hdrs = {
        "Accept":           "application/json, */*",
        "Content-Type":     "application/json",
        "Origin":           _LOGIN_URL,
        "Referer":          f"{_LOGIN_URL}/",
        "x-device-details": device_details,
        "x-request-id":     request_id,
        "sec-fetch-dest":   "empty",
        "sec-fetch-mode":   "cors",
        "sec-fetch-site":   "same-site",
    }

    # ── Step 1: Load login page → session cookies ──────────────
    _safe_log("Step 1: Loading login.upstox.com...")
    login_page = (
        f"{_LOGIN_URL}/"
        f"?client_id={client_id}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
        f"&response_type=code"
    )
    try:
        r1 = session.get(login_page, allow_redirects=True, timeout=20)
        _safe_log(f"  Status: {r1.status_code} | Cookies: {list(session.cookies.keys())}")
    except Exception as e:
        _safe_log(f"  login.upstox.com failed ({type(e).__name__}) — trying dialog...")
        try:
            r1 = session.get(
                _AUTH_DIALOG,
                params={"response_type": "code", "client_id": client_id, "redirect_uri": redirect_uri},
                allow_redirects=True, timeout=20,
            )
            _safe_log(f"  Dialog status: {r1.status_code}")
        except Exception as e2:
            raise RuntimeError(f"Step 1 failed: {type(e2).__name__}") from None

    time.sleep(2)

    # ── Step 2: Generate OTP (mobile submit) ──────────────────
    _safe_log(f"Step 2: OTP generate for {_mask(mobile, 3)}*****...")
    otp_gen_ep = f"{_SERVICE}/login/open/v7/auth/1fa/otp/generate"

    otp_bodies = [
        {"mobileNumber": mobile},
        {"mobileNumber": mobile, "requestId": request_id},
        {"mobile_num": mobile},
        {"mobile": mobile},
    ]

    step2_ok = False
    for body in otp_bodies:
        try:
            r2 = session.post(
                otp_gen_ep, json=body,
                headers=svc_hdrs,
                allow_redirects=False, timeout=20,
            )
            _safe_log(f"  {list(body.keys())}: HTTP {r2.status_code}")

            if r2.status_code in (200, 201):
                step2_ok = True
                _safe_log("  OTP generated ✅")
                try:
                    _safe_log(f"  Response keys: {list(r2.json().keys())}")
                except Exception:
                    pass
                break
            try:
                errs = r2.json().get("errors") or []
                ec   = errs[0].get("errorCode", "") if errs else ""
                emsg = errs[0].get("message", "")   if errs else ""
                if ec:
                    _safe_log(f"  Error: {ec} — {emsg}")
            except Exception:
                pass
        except Exception as e:
            _safe_log(f"  {type(e).__name__}")

    if not step2_ok:
        _safe_log("  Step 2 inconclusive — continuing...")

    time.sleep(2)

    # ── Step 3: Login with MPIN + TOTP → auth_identity_token ──
    _safe_log("Step 3: MPIN+TOTP login (values masked)...")

    # Confirmed endpoints from v7 auth pattern:
    step3_eps = [
        f"{_SERVICE}/login/open/v7/auth/2fa/totp",
        f"{_SERVICE}/login/open/v7/auth/2fa/login",
        f"{_SERVICE}/login/open/v7/auth/1fa/otp/verify",
        f"{_SERVICE}/login/open/v7/auth/login",
    ]
    pin_keys = ["mpin", "pin", "client_secret"]

    def _do_step3(totp_val: str) -> bool:
        """Returns True agar auth_identity_token cookie set ho gayi."""
        for ep in step3_eps:
            for pkey in pin_keys:
                pld = {
                    "mobileNumber": mobile,
                    pkey:           pin,
                    "totp":         totp_val,
                    "requestId":    request_id,
                }
                try:
                    r3 = session.post(
                        ep, json=pld,
                        headers=svc_hdrs,
                        allow_redirects=False, timeout=20,
                    )
                    _safe_log(f"  {ep.split('/')[-1]}+{pkey}: HTTP {r3.status_code}")

                    if r3.status_code == 404:
                        break  # endpoint nahi hai

                    # auth_identity_token cookie check
                    if "auth_identity_token" in session.cookies:
                        _safe_log("  auth_identity_token cookie set ✅")
                        return True

                    try:
                        body = r3.json()
                        bkeys = list(body.keys())
                        _safe_log(f"    Body keys: {bkeys}")

                        if r3.status_code in (200, 201):
                            # Check token in body
                            data = body.get("data", {})
                            if isinstance(data, dict) and data.get("auth_identity_token"):
                                # Manually set cookie
                                session.cookies.set(
                                    "auth_identity_token",
                                    data["auth_identity_token"]
                                )
                                _safe_log("    auth_identity_token from body ✅")
                                return True
                            # Maybe already set via Set-Cookie header
                            if "auth_identity_token" in session.cookies:
                                return True

                        if r3.status_code >= 400:
                            errs = body.get("errors") or []
                            ec   = errs[0].get("errorCode", "") if errs else ""
                            emsg = errs[0].get("message", "")   if errs else ""
                            if ec:
                                _safe_log(f"    Error: {ec} — {emsg}")
                    except Exception:
                        pass

                except Exception as e:
                    _safe_log(f"    {type(e).__name__}")
                time.sleep(0.3)
        return "auth_identity_token" in session.cookies

    totp_code = pyotp.TOTP(totp_secret).now()
    logged_in = _do_step3(totp_code)

    if not logged_in:
        _safe_log("  Waiting 31s for fresh TOTP window...")
        time.sleep(31)
        totp_code = pyotp.TOTP(totp_secret).now()
        _safe_log("  Retry Step 3 (fresh TOTP)...")
        logged_in = _do_step3(totp_code)

    if not logged_in:
        _safe_log("  WARNING: auth_identity_token not confirmed — trying Step 4 anyway...")

    # ── Step 4: verify-access-token → auth_code ───────────────
    # (Confirmed from browser: gateway-worker/v1/verify-access-token)
    _safe_log("Step 4: Getting auth_code from verify-access-token...")

    verify_url = (
        f"{_SERVICE}/gateway-worker/v1/verify-access-token"
        f"?client_id={client_id}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
        f"&response_type=code"
    )

    auth_code = None
    try:
        # allow_redirects=False to catch the redirect to 127.0.0.1
        r4 = session.get(
            verify_url,
            headers={
                "Accept":         "*/*",
                "Origin":         _LOGIN_URL,
                "Referer":        f"{_LOGIN_URL}/",
                "sec-fetch-site": "same-site",
                "sec-fetch-mode": "cors",
                "sec-fetch-dest": "empty",
            },
            allow_redirects=False,
            timeout=20,
        )
        _safe_log(f"  verify-access-token: HTTP {r4.status_code}")

        # Check redirect location
        loc = r4.headers.get("Location", "")
        if loc:
            _safe_log(f"  Redirect to: {loc[:50]}...")
            auth_code = _extract_code(loc)
            if auth_code:
                _safe_log(f"  auth_code from Location ✅")

        # Check response body
        if not auth_code:
            try:
                body = r4.json()
                _safe_log(f"  Body keys: {list(body.keys())}")
                for k in ("code", "redirect_url", "redirectUrl"):
                    val = body.get(k, "")
                    if val:
                        auth_code = _extract_code(val) if "redirect" in k else val
                        if auth_code:
                            _safe_log(f"  auth_code from body.{k} ✅")
                            break
            except Exception:
                pass

    except ReqConnError as ce:
        # Connection to 127.0.0.1 refused = SUCCESS! Code is in the URL
        err_str = str(ce)
        auth_code = _extract_code(err_str)
        if auth_code:
            _safe_log(f"  auth_code from 127.0.0.1 redirect ✅")

    except Exception as e:
        _safe_log(f"  Step 4 error: {type(e).__name__}")

    # Also try with allow_redirects=True to follow to 127.0.0.1
    if not auth_code:
        try:
            r4b = session.get(
                verify_url,
                headers={
                    "Accept":         "*/*",
                    "Origin":         _LOGIN_URL,
                    "Referer":        f"{_LOGIN_URL}/",
                },
                allow_redirects=True,
                timeout=20,
            )
            _safe_log(f"  verify (redirected): HTTP {r4b.status_code} | URL: {r4b.url[:60]}")
            auth_code = _extract_code(r4b.url)
            if auth_code:
                _safe_log(f"  auth_code from final URL ✅")
        except ReqConnError as ce:
            auth_code = _extract_code(str(ce))
            if auth_code:
                _safe_log(f"  auth_code from connection error URL ✅")
        except Exception as e:
            _safe_log(f"  {type(e).__name__}")

    if not auth_code:
        raise RuntimeError(
            "Upstox auth failed — auth_code nahi mila.\n\n"
            "MOST LIKELY CAUSE — REDIRECT_URI galat hai:\n"
            "  Current REDIRECT_URI: " + redirect_uri + "\n"
            "  Required REDIRECT_URI: https://127.0.0.1/\n\n"
            "FIX:\n"
            "  1. developer.upstox.com → My Apps → Edit App\n"
            "     Redirect URI = https://127.0.0.1/\n"
            "  2. GitHub Secret UPSTOX_REDIRECT_URI = https://127.0.0.1/\n"
            "  3. Workflow dobara run karo\n\n"
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
