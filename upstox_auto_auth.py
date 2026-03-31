"""
upstox_auto_auth.py
===================
GitHub Actions mein headless Upstox OAuth.

Fix: udapi_api_key cookie jo Step 1 mein milti hai, use
     x-api-key header ke roop mein Steps 2 & 3 mein pass karna zaroori hai.
     Ye missing tha — isliye 401/400 aa raha tha.

SECURITY: PIN, TOTP, token kabhi log nahi hote.
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
        _safe_log(f"  Token OK: {_mask(token)} (len={len(token)})")
        return token
    except requests.HTTPError as e:
        raise RuntimeError(
            f"Token exchange HTTP {e.response.status_code} — "
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

    # ── Step 1: Load dialog page → udapi_api_key cookie ───────
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

    # udapi_api_key cookie — CRITICAL: Steps 2 & 3 mein x-api-key header chahiye
    api_key_cookie = session.cookies.get("udapi_api_key", "")
    if api_key_cookie:
        _safe_log(f"  udapi_api_key found: {_mask(api_key_cookie)}")
    else:
        _safe_log("  WARNING: udapi_api_key cookie not found")

    time.sleep(1.5)

    # Base headers for all API calls — x-api-key is the key fix
    def _api_headers(extra: dict = {}) -> dict:
        h = {
            "User-Agent":   _BROWSER_UA,
            "Accept":       "application/json, */*",
            "Content-Type": "application/json",
            "Origin":       "https://api.upstox.com",
            "Referer":      r1.url,
        }
        if api_key_cookie:
            h["x-api-key"] = api_key_cookie
        h.update(extra)
        return h

    # ── Step 2: Submit mobile number ──────────────────────────
    _safe_log(f"Step 2: Submitting mobile {_mask(mobile, 3)}*****...")
    step2_ok = False

    for mkey in ["mobile_num", "mobileNum"]:
        try:
            r2 = session.post(
                _AUTH_DIALOG,
                params=auth_params,
                json={mkey: mobile},
                headers=_api_headers(),
                allow_redirects=False,
                timeout=20,
            )
            _safe_log(f"  {mkey}: HTTP {r2.status_code}")

            if r2.status_code in (200, 201):
                step2_ok = True
                loc = r2.headers.get("Location", "")
                if loc:
                    code = _extract_code(loc)
                    if code:
                        _safe_log("  Got auth_code early at Step 2!")
                        return _exchange_code(session, code, client_id, client_secret, redirect_uri)
                break

            # Log error code only (not values)
            if r2.status_code in (400, 401):
                try:
                    errs = r2.json().get("errors") or []
                    ec   = errs[0].get("errorCode", "") if errs else ""
                    _safe_log(f"  Error code: {ec}")
                except Exception:
                    pass

        except Exception as e:
            _safe_log(f"  {mkey} error: {type(e).__name__}")

    if not step2_ok:
        _safe_log("  Step 2 inconclusive — proceeding anyway...")

    time.sleep(1)

    # ── Step 3: Submit PIN + TOTP ──────────────────────────────
    _safe_log("Step 3: Submitting PIN + TOTP (values masked)...")

    def _try_step3(totp_val: str):
        # Payload formats — Upstox uses "client_secret" key for PIN
        payloads = [
            {"mobile_num": mobile, "client_secret": pin, "totp": totp_val},
            {"mobile_num": mobile, "pin":            pin, "totp": totp_val},
            {"mobile_num": mobile, "mpin":           pin, "totp": totp_val},
        ]

        for idx, pld in enumerate(payloads):
            try:
                r3 = session.post(
                    _AUTH_DIALOG,
                    params=auth_params,
                    json=pld,
                    headers=_api_headers(),
                    allow_redirects=False,
                    timeout=20,
                )
                _safe_log(f"  fmt{idx+1}: HTTP {r3.status_code}")

                # Check redirect
                loc = r3.headers.get("Location", "")
                if loc:
                    code = _extract_code(loc)
                    if code:
                        return code

                # Check JSON body
                try:
                    body = r3.json()
                    if "code" in body:
                        return body["code"]
                    for key in ("redirect_url", "redirectUrl", "url"):
                        if key in body:
                            code = _extract_code(body[key])
                            if code:
                                return code
                    if r3.status_code >= 400:
                        errs = body.get("errors") or []
                        ec   = errs[0].get("errorCode", "") if errs else body.get("error", "")
                        _safe_log(f"  Error: {ec}")
                except Exception:
                    pass

                # HTML body
                if r3.status_code == 200 and r3.text:
                    m = re.search(r"[?&]code=([^&\"'\s]+)", r3.text)
                    if m:
                        return m.group(1)

            except Exception as e:
                _safe_log(f"  fmt{idx+1} error: {type(e).__name__}")

            time.sleep(0.3)
        return None

    # First TOTP attempt
    totp_code = pyotp.TOTP(totp_secret).now()
    auth_code = _try_step3(totp_code)

    # Retry with fresh TOTP window
    if not auth_code:
        _safe_log("  Waiting 31s for fresh TOTP window...")
        time.sleep(31)
        totp_code = pyotp.TOTP(totp_secret).now()
        _safe_log("  Retrying (fresh TOTP, values masked)...")
        auth_code = _try_step3(totp_code)

    if not auth_code:
        raise RuntimeError(
            "Upstox auth failed — auth_code nahi mila.\n"
            "Debug steps:\n"
            "  1. TOTP verify: python -c \"import pyotp; print(pyotp.TOTP('YOUR_TOTP_SECRET').now())\"\n"
            "     Upstox app ke current OTP se match karna chahiye\n"
            "  2. UPSTOX_PIN — Upstox login 6-digit PIN (wo jo app mein set kiya hai)\n"
            "  3. UPSTOX_MOBILE — 10-digit bina +91 (e.g. 9876543210)\n"
            "  4. UPSTOX_REDIRECT_URI — Developer console mein exactly jo set kiya\n"
            "Values logs mein kabhi nahi dikhti."
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
