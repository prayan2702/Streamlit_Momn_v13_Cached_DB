"""
upstox_auto_auth.py
===================
Key insight: udapi_api_key cookie = CLIENT_ID hi hai.
Toh x-api-key = os.environ["UPSTOX_CLIENT_ID"] directly.

Baaki issue: exact endpoint URL aur request format.
Multiple combinations try karta hai.

SECURITY: PIN, TOTP, token kabhi logs mein nahi dikhte.
"""

import os
import re
import time
import urllib.parse

import pyotp
import requests

_AUTH_DIALOG = "https://api.upstox.com/v2/login/authorization/dialog"
_TOKEN_URL   = "https://api.upstox.com/v2/login/authorization/token"

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

    # ── Step 1: Load dialog page ───────────────────────────────
    _safe_log("Step 1: Loading auth dialog page...")
    try:
        r1 = session.get(
            _AUTH_DIALOG, params=auth_params,
            allow_redirects=True, timeout=20,
        )
        _safe_log(f"  Status: {r1.status_code}")
        _safe_log(f"  Cookies: {list(session.cookies.keys())}")
    except Exception as e:
        raise RuntimeError(f"Step 1 failed: {type(e).__name__}") from None

    # KEY INSIGHT: x-api-key = CLIENT_ID (confirmed from cookie = client_id)
    x_api_key = client_id
    _safe_log(f"  Using x-api-key = CLIENT_ID: {_mask(x_api_key)} ✅")

    time.sleep(1.5)

    # All internal API call variations to try
    # Format: (base_url, path, mobile_key)
    STEP2_ATTEMPTS = [
        ("https://api-v2.upstox.com",   "/user/v1/otp",                 "mobile_num"),
        ("https://api-v2.upstox.com",   "/user/v1/send_otp_for_mobile", "mobile_num"),
        ("https://api.upstox.com",      "/user/v1/otp",                 "mobile_num"),
        ("https://api.upstox.com",      "/user/v1/send_otp",            "mobile_num"),
        ("https://api-v2.upstox.com",   "/user/v1/otp",                 "mobileNum"),
    ]

    STEP3_ATTEMPTS = [
        ("https://api-v2.upstox.com",  "/user/v1/login"),
        ("https://api-v2.upstox.com",  "/user/v1/verify"),
        ("https://api.upstox.com",     "/user/v1/login"),
        ("https://api-v2.upstox.com",  "/user/v1/otp/verify"),
    ]

    def _make_headers(base_url: str) -> dict:
        return {
            "Accept":       "application/json, */*",
            "Content-Type": "application/json",
            "x-api-key":    x_api_key,
            "Origin":       base_url,
            "Referer":      r1.url,
        }

    # ── Step 2: Send OTP ──────────────────────────────────────
    _safe_log(f"Step 2: Send OTP to {_mask(mobile, 3)}*****...")
    step2_ok = False

    for base, path, mkey in STEP2_ATTEMPTS:
        url = base + path
        try:
            r2 = session.post(
                url,
                json={mkey: mobile},
                headers=_make_headers(base),
                allow_redirects=False,
                timeout=15,
            )
            status = r2.status_code
            _safe_log(f"  {path}+{mkey}: HTTP {status}")

            if status in (200, 201):
                step2_ok = True
                _safe_log("  OTP sent ✅")
                # Check early auth_code
                try:
                    body = r2.json()
                    for k in ("redirect_url", "code"):
                        val = body.get(k, "")
                        if val:
                            code = _extract_code(val) if "redirect" in k else val
                            if code:
                                return _exchange_code(session, code, client_id, client_secret, redirect_uri)
                except Exception:
                    pass
                break

            # Log error details (no credential values)
            try:
                errs = r2.json().get("errors") or []
                ec   = errs[0].get("errorCode", "") if errs else ""
                emsg = errs[0].get("message", "")   if errs else ""
                if ec:
                    _safe_log(f"    Error: {ec} — {emsg}")
            except Exception:
                pass

        except Exception as e:
            _safe_log(f"  {path}: {type(e).__name__}")

    if not step2_ok:
        _safe_log("  OTP step inconclusive — continuing to login...")

    time.sleep(2)

    # ── Step 3: Login with MPIN + TOTP ─────────────────────────
    _safe_log("Step 3: Login MPIN+TOTP (values masked)...")

    def _do_login(totp_val: str):
        pin_keys = ["mpin", "client_secret", "pin"]

        for base, path in STEP3_ATTEMPTS:
            url = base + path
            for pkey in pin_keys:
                pld = {"mobile_num": mobile, pkey: pin, "totp": totp_val}
                try:
                    r3 = session.post(
                        url, json=pld,
                        headers=_make_headers(base),
                        allow_redirects=False,
                        timeout=20,
                    )
                    _safe_log(f"  {path}+{pkey}: HTTP {r3.status_code}")

                    # Parse response
                    try:
                        body = r3.json()
                        bkeys = list(body.keys())
                        _safe_log(f"    Body keys: {bkeys}")

                        # Check data.redirect_url
                        data = body.get("data", {})
                        if isinstance(data, dict):
                            for k in ("redirect_url", "redirectUrl", "redirect"):
                                val = data.get(k, "")
                                if val:
                                    code = _extract_code(val)
                                    if code:
                                        _safe_log(f"    auth_code from data.{k} ✅")
                                        return code

                        # Top-level redirect_url or code
                        for k in ("redirect_url", "redirectUrl", "code"):
                            val = body.get(k, "")
                            if val:
                                code = _extract_code(val) if "redirect" in k else val
                                if code:
                                    _safe_log(f"    auth_code from {k} ✅")
                                    return code

                        # Error logging (no values)
                        if r3.status_code >= 400:
                            errs = body.get("errors") or []
                            ec   = errs[0].get("errorCode", "") if errs else ""
                            emsg = errs[0].get("message", "")   if errs else ""
                            _safe_log(f"    Error: {ec} — {emsg}")

                    except Exception:
                        pass

                    # Location header
                    loc = r3.headers.get("Location", "")
                    if loc:
                        code = _extract_code(loc)
                        if code:
                            _safe_log(f"    auth_code from Location ✅")
                            return code

                    # HTML
                    if r3.status_code == 200 and r3.text:
                        m = re.search(r"[?&]code=([^&\"'\s]+)", r3.text)
                        if m:
                            return m.group(1)

                except Exception as e:
                    _safe_log(f"  {path}: {type(e).__name__}")

                time.sleep(0.2)
        return None

    totp_code = pyotp.TOTP(totp_secret).now()
    auth_code = _do_login(totp_code)

    if not auth_code:
        _safe_log("  Waiting 31s for fresh TOTP window...")
        time.sleep(31)
        totp_code = pyotp.TOTP(totp_secret).now()
        _safe_log("  Retry (fresh TOTP)...")
        auth_code = _do_login(totp_code)

    if not auth_code:
        raise RuntimeError(
            "Upstox auth failed.\n"
            "Browser DevTools se exact endpoint find karo:\n"
            "  1. Chrome → F12 → Network → Fetch/XHR\n"
            "  2. Login page pe mobile submit karo\n"
            "  3. Request URL + headers + body share karo\n"
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
