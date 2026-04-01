"""
upstox_auto_auth.py
===================
Correct Upstox headless auth flow:

Step 1: GET dialog → udapi_api_key cookie
Step 2: POST api-v2.upstox.com/user/v1/send_otp_for_mobile
        headers: x-api-key = udapi_api_key cookie
        body:    {mobile_num}
Step 3: POST api-v2.upstox.com/user/v1/login
        headers: x-api-key = udapi_api_key cookie
        body:    {mobile_num, mpin, totp}
        response: redirect_url containing ?code=...
Step 4: code → access_token via token endpoint

SECURITY: PIN, TOTP, token kabhi logs mein nahi dikhte.
"""

import os
import re
import time
import urllib.parse

import pyotp
import requests

_AUTH_DIALOG   = "https://api.upstox.com/v2/login/authorization/dialog"
_TOKEN_URL     = "https://api.upstox.com/v2/login/authorization/token"
_API_V2_BASE   = "https://api-v2.upstox.com"
_SEND_OTP_EP   = f"{_API_V2_BASE}/user/v1/send_otp_for_mobile"
_LOGIN_EP      = f"{_API_V2_BASE}/user/v1/login"

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

    # ── Step 1: GET dialog → udapi_api_key cookie ──────────────
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

    api_key = session.cookies.get("udapi_api_key", "")
    if api_key:
        _safe_log(f"  udapi_api_key: {_mask(api_key)} ✅")
    else:
        _safe_log("  WARNING: udapi_api_key not in cookies")

    time.sleep(1.5)

    # Internal API headers — x-api-key is the session key
    int_headers = {
        "Accept":       "application/json, */*",
        "Content-Type": "application/json",
        "x-api-key":    api_key,
        "Origin":       _API_V2_BASE,
        "Referer":      r1.url,
    }

    # ── Step 2: Send OTP to mobile ────────────────────────────
    _safe_log(f"Step 2: Send OTP to {_mask(mobile, 3)}*****...")
    step2_ok = False

    for mkey in ["mobile_num", "mobileNum"]:
        try:
            r2 = session.post(
                _SEND_OTP_EP,
                json={mkey: mobile},
                headers=int_headers,
                allow_redirects=False,
                timeout=20,
            )
            _safe_log(f"  {mkey}: HTTP {r2.status_code}")

            if r2.status_code in (200, 201):
                step2_ok = True
                _safe_log("  OTP sent ✅")
                try:
                    body = r2.json()
                    _safe_log(f"  Response status: {body.get('status', '')}")
                except Exception:
                    pass
                break

            # Log error (not values)
            try:
                errs = r2.json().get("errors") or []
                ec = errs[0].get("errorCode", "") if errs else ""
                msg = errs[0].get("message", "") if errs else ""
                _safe_log(f"  Error: {ec} — {msg}")
            except Exception:
                pass

        except Exception as e:
            _safe_log(f"  {mkey}: {type(e).__name__}")

    if not step2_ok:
        _safe_log("  Step 2 failed — login might not need OTP (MPIN+TOTP flow)")
        _safe_log("  Continuing to Step 3 (direct MPIN+TOTP login)...")

    time.sleep(2)  # OTP delivery wait

    # ── Step 3: Login with MPIN + TOTP ───────────────────────
    _safe_log("Step 3: Login with MPIN + TOTP (values masked)...")

    def _do_login(totp_val: str):
        """
        Try login with internal endpoint.
        Upstox uses 'mpin' key for PIN in internal API.
        Response contains redirect_url with auth_code.
        """
        payloads = [
            # Format A — mpin (Upstox internal API standard)
            {"mobile_num": mobile, "mpin": pin, "totp": totp_val},
            # Format B — client_secret (older format)
            {"mobile_num": mobile, "client_secret": pin, "totp": totp_val},
            # Format C — pin
            {"mobile_num": mobile, "pin": pin, "totp": totp_val},
        ]

        for idx, pld in enumerate(payloads):
            try:
                r3 = session.post(
                    _LOGIN_EP,
                    json=pld,
                    headers=int_headers,
                    allow_redirects=False,
                    timeout=20,
                )
                _safe_log(f"  fmt{idx+1}: HTTP {r3.status_code}")

                # 200 with redirect_url in body (most common)
                try:
                    body = r3.json()
                    _safe_log(f"  Body keys: {list(body.keys())}")

                    # Check redirect_url in body
                    for key in ("redirect_url", "redirectUrl", "data"):
                        val = body.get(key, "")
                        if isinstance(val, str) and val:
                            code = _extract_code(val)
                            if code:
                                _safe_log(f"  auth_code from body[{key}] ✅")
                                return code
                        # data might be a dict
                        if isinstance(val, dict):
                            for subkey in ("redirect_url", "redirectUrl", "code"):
                                subval = val.get(subkey, "")
                                if subval:
                                    code = _extract_code(subval) if "redirect" in subkey else subval
                                    if code:
                                        return code

                    if "code" in body:
                        return body["code"]

                    # Error info (no values)
                    if r3.status_code >= 400:
                        errs = body.get("errors") or []
                        ec = errs[0].get("errorCode", "") if errs else body.get("error", "")
                        emsg = errs[0].get("message", "") if errs else ""
                        _safe_log(f"  Error: {ec} — {emsg}")

                except Exception as je:
                    _safe_log(f"  JSON parse: {type(je).__name__}")

                # Location header redirect
                loc = r3.headers.get("Location", "")
                if loc:
                    code = _extract_code(loc)
                    if code:
                        _safe_log(f"  auth_code from Location header ✅")
                        return code

                # HTML scan
                if r3.status_code == 200 and r3.text:
                    m = re.search(r"[?&]code=([^&\"'\s]+)", r3.text)
                    if m:
                        return m.group(1)

            except Exception as e:
                _safe_log(f"  fmt{idx+1}: {type(e).__name__}")

            time.sleep(0.3)
        return None

    totp_code = pyotp.TOTP(totp_secret).now()
    auth_code = _do_login(totp_code)

    if not auth_code:
        _safe_log("  Waiting 31s for fresh TOTP window...")
        time.sleep(31)
        totp_code = pyotp.TOTP(totp_secret).now()
        _safe_log("  Retry (fresh TOTP, masked)...")
        auth_code = _do_login(totp_code)

    if not auth_code:
        raise RuntimeError(
            "Upstox auth failed.\n"
            "Checks:\n"
            "  1. UPSTOX_MOBILE: 10-digit, bina +91\n"
            "  2. UPSTOX_PIN: Upstox app 6-digit MPIN\n"
            "  3. TOTP verify locally:\n"
            "     python -c \"import pyotp; print(pyotp.TOTP('SECRET').now())\"\n"
            "     Upstox app OTP se match karna chahiye\n"
            "  4. UPSTOX_REDIRECT_URI: developer.upstox.com pe exactly jo hai\n"
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
