"""
upstox_auto_auth.py
===================
GitHub Actions mein headless Upstox OAuth.
pyotp + requests se actual Upstox internal login API use karta hai.

SECURITY:
  - PIN, TOTP, token values kabhi log/print nahi hote
  - Sirf masked (first 4 chars + ***) form mein dikhte hain
  - Error messages mein credential values nahi hote
"""

import os
import re
import time
import urllib.parse

import pyotp
import requests

_BASE_URL    = "https://api.upstox.com"
_AUTH_DIALOG = f"{_BASE_URL}/v2/login/authorization/dialog"
_TOKEN_URL   = f"{_BASE_URL}/v2/login/authorization/token"
_LOGIN_BASE  = "https://api-v2.upstox.com"
_SEND_OTP    = f"{_LOGIN_BASE}/user/v1/send_otp_for_mobile"
_VERIFY_OTP  = f"{_LOGIN_BASE}/user/v1/login"

_BROWSER_UA  = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _mask(value: str, visible: int = 4) -> str:
    if not value:
        return "***"
    return value[:visible] + "***"

def _safe_log(msg: str):
    print(f"[upstox_auth] {msg}", flush=True)

def _extract_code(url: str):
    try:
        params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        codes  = params.get("code", [])
        return codes[0] if codes else None
    except Exception:
        return None


def _exchange_code(session, auth_code, client_id, client_secret, redirect_uri) -> str:
    _safe_log("Step 4: Exchanging auth_code for access_token...")
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
        _safe_log(f"  access_token: {_mask(token)} (len={len(token)}) OK")
        return token
    except requests.HTTPError as e:
        raise RuntimeError(
            f"Token exchange failed: HTTP {e.response.status_code}. "
            "CLIENT_ID, CLIENT_SECRET, REDIRECT_URI check karo."
        ) from None
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Token exchange error: {type(e).__name__}") from None


def get_upstox_token_automated(
    client_id, client_secret, redirect_uri,
    mobile, pin, totp_secret
) -> str:

    session = requests.Session()
    session.headers.update({
        "User-Agent":      _BROWSER_UA,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })

    auth_params = {
        "response_type": "code",
        "client_id":     client_id,
        "redirect_uri":  redirect_uri,
    }

    # Step 1: Load dialog page
    _safe_log("Step 1: Loading auth dialog page...")
    try:
        r1 = session.get(
            _AUTH_DIALOG, params=auth_params,
            allow_redirects=True, timeout=20,
        )
        _safe_log(f"  Status: {r1.status_code} | Cookies: {list(session.cookies.keys())}")
    except Exception as e:
        raise RuntimeError(f"Step 1 failed: {type(e).__name__}") from None

    time.sleep(1.5)

    # Step 2: Send OTP to mobile
    _safe_log(f"Step 2: Sending OTP to {_mask(mobile, 3)}*****...")
    step2_ok = False

    endpoints_to_try = [
        (_SEND_OTP,    {},           "application/json"),
        (_AUTH_DIALOG, auth_params,  "application/json"),
    ]
    mobile_keys = ["mobile_num", "mobileNum", "mobile", "phone"]

    for ep, params, ctype in endpoints_to_try:
        if step2_ok:
            break
        for mkey in mobile_keys:
            try:
                r2 = session.post(
                    ep, params=params,
                    json={mkey: mobile},
                    headers={
                        "Accept":       "application/json, */*",
                        "Content-Type": ctype,
                        "Origin":       "https://api.upstox.com",
                        "Referer":      r1.url,
                    },
                    allow_redirects=False, timeout=20,
                )
                _safe_log(f"  {ep.split('/')[-1]}+{mkey}: HTTP {r2.status_code}")

                if r2.status_code in (200, 201, 204):
                    step2_ok = True
                    # Early auth_code check
                    loc = r2.headers.get("Location", "")
                    if loc:
                        code = _extract_code(loc)
                        if code:
                            _safe_log("  Got auth_code early!")
                            return _exchange_code(session, code, client_id, client_secret, redirect_uri)
                    break

                if r2.status_code == 400:
                    try:
                        errs = r2.json().get("errors") or []
                        ec   = errs[0].get("errorCode", "") if errs else ""
                        _safe_log(f"  Error code: {ec}")
                    except Exception:
                        pass

            except Exception as e:
                _safe_log(f"  {ep.split('/')[-1]} error: {type(e).__name__}")

    if not step2_ok:
        _safe_log("  Step 2 inconclusive — proceeding to Step 3...")

    time.sleep(1)

    # Step 3: Submit PIN + TOTP
    _safe_log("Step 3: Submitting PIN + TOTP (values masked)...")

    def _try_step3(totp_val: str):
        payloads = [
            {"mobile_num": mobile, "client_secret": pin, "totp": totp_val},
            {"mobile_num": mobile, "pin":            pin, "totp": totp_val},
            {"mobile_num": mobile, "mpin":           pin, "totp": totp_val},
            {"mobileNum":  mobile, "mpin":           pin, "totp": totp_val},
        ]
        step3_eps = [_VERIFY_OTP, _AUTH_DIALOG]

        for ep in step3_eps:
            for idx, pld in enumerate(payloads):
                try:
                    r = session.post(
                        ep,
                        params=auth_params if ep == _AUTH_DIALOG else {},
                        json=pld,
                        headers={
                            "Accept":       "application/json, */*",
                            "Content-Type": "application/json",
                            "Origin":       "https://api.upstox.com",
                            "Referer":      r1.url,
                        },
                        allow_redirects=False, timeout=20,
                    )
                    _safe_log(f"  {ep.split('/')[-1]} fmt{idx+1}: HTTP {r.status_code}")

                    loc = r.headers.get("Location", "")
                    if loc:
                        code = _extract_code(loc)
                        if code:
                            return code

                    try:
                        body = r.json()
                        if "code" in body:
                            return body["code"]
                        for key in ("redirect_url", "redirectUrl", "url"):
                            if key in body:
                                code = _extract_code(body[key])
                                if code:
                                    return code
                        if r.status_code >= 400:
                            errs = body.get("errors") or []
                            ec   = errs[0].get("errorCode", "") if errs else body.get("error", "")
                            _safe_log(f"  Error: {ec}")
                    except Exception:
                        pass

                    if r.status_code == 200 and r.text:
                        m = re.search(r"[?&]code=([^&\"'\s]+)", r.text)
                        if m:
                            return m.group(1)

                except Exception as e:
                    _safe_log(f"  {ep.split('/')[-1]} fmt{idx+1}: {type(e).__name__}")

                time.sleep(0.2)
        return None

    totp_code = pyotp.TOTP(totp_secret).now()
    auth_code = _try_step3(totp_code)

    if not auth_code:
        _safe_log("  Waiting 31s for fresh TOTP window...")
        time.sleep(31)
        totp_code = pyotp.TOTP(totp_secret).now()
        _safe_log("  Retry with fresh TOTP (masked)...")
        auth_code = _try_step3(totp_code)

    if not auth_code:
        raise RuntimeError(
            "Upstox auth failed — auth_code nahi mila.\n"
            "Debug steps:\n"
            "  1. TOTP verify karo — Python console mein:\n"
            "       import pyotp; print(pyotp.TOTP('TOTP_SECRET').now())\n"
            "     Upstox app ke current OTP se match karna chahiye\n"
            "  2. UPSTOX_PIN — Upstox login 6-digit PIN\n"
            "  3. UPSTOX_MOBILE — 10-digit bina +91\n"
            "  4. UPSTOX_REDIRECT_URI — Developer console se exact match\n"
            "Values kabhi logs mein nahi dikhti."
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
            "Settings → Secrets and variables → Actions → New repository secret"
        )
    return get_upstox_token_automated(
        client_id     = os.environ["UPSTOX_CLIENT_ID"],
        client_secret = os.environ["UPSTOX_CLIENT_SECRET"],
        redirect_uri  = os.environ["UPSTOX_REDIRECT_URI"],
        mobile        = os.environ["UPSTOX_MOBILE"],
        pin           = os.environ["UPSTOX_PIN"],
        totp_secret   = os.environ["UPSTOX_TOTP_SECRET"],
    )
