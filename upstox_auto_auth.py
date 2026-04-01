"""
upstox_auto_auth.py
===================
Fix: UDAPI10000 = request body mein client_id + redirect_uri bhi chahiye,
sirf query params mein nahi — ye hi root cause tha.

Correct Upstox headless login flow:
  Step 1: GET dialog → udapi_api_key cookie
  Step 2: POST dialog (x-api-key header + body mein client_id bhi)
          → {mobile_num, client_id, redirect_uri, response_type}
  Step 3: POST dialog (x-api-key header + body mein client_id bhi)
          → {mobile_num, client_secret, totp, client_id, redirect_uri, response_type}
  Step 4: auth_code → access_token

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
        _safe_log(f"  Token OK: {_mask(token)} (len={len(token)}) ✅")
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

    # Query params (for GET only)
    auth_params = {
        "response_type": "code",
        "client_id":     client_id,
        "redirect_uri":  redirect_uri,
    }

    # ── Step 1: Load dialog → udapi_api_key cookie ─────────────
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
        _safe_log("  WARNING: udapi_api_key not found")

    time.sleep(1.5)

    # Common headers for Steps 2 & 3
    # KEY FIX: x-api-key header with udapi_api_key cookie value
    api_headers = {
        "Accept":       "application/json, */*",
        "Content-Type": "application/json",
        "Origin":       "https://api.upstox.com",
        "Referer":      r1.url,
        "x-api-key":    api_key,
    }

    # ── Step 2: Submit mobile ──────────────────────────────────
    # KEY FIX: client_id + redirect_uri + response_type in JSON BODY
    _safe_log(f"Step 2: Submitting mobile {_mask(mobile, 3)}*****...")
    step2_ok = False

    for mkey in ["mobile_num", "mobileNum"]:
        try:
            body2 = {
                mkey:             mobile,
                "client_id":      client_id,        # ← KEY FIX
                "redirect_uri":   redirect_uri,      # ← KEY FIX
                "response_type":  "code",            # ← KEY FIX
            }
            r2 = session.post(
                _AUTH_DIALOG,
                json=body2,
                headers=api_headers,
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
                        _safe_log("  Got auth_code at Step 2!")
                        return _exchange_code(session, code, client_id, client_secret, redirect_uri)
                break

            if r2.status_code in (400, 401):
                try:
                    errs = r2.json().get("errors") or []
                    ec   = errs[0].get("errorCode", "") if errs else ""
                    _safe_log(f"  Error: {ec}")
                except Exception:
                    pass

        except Exception as e:
            _safe_log(f"  {mkey}: {type(e).__name__}")

    if not step2_ok:
        _safe_log("  Step 2 inconclusive — continuing to Step 3...")

    time.sleep(1)

    # ── Step 3: Submit PIN + TOTP ──────────────────────────────
    # KEY FIX: client_id + redirect_uri + response_type in BODY here too
    _safe_log("Step 3: Submitting PIN + TOTP (values masked)...")

    def _try_step3(totp_val: str):
        # "client_secret" key = PIN in Upstox API (confusing naming!)
        payloads = [
            # Format A — "client_secret" key for PIN (Upstox standard)
            {
                "mobile_num":    mobile,
                "client_secret": pin,
                "totp":          totp_val,
                "client_id":     client_id,
                "redirect_uri":  redirect_uri,
                "response_type": "code",
            },
            # Format B — "pin" key
            {
                "mobile_num":    mobile,
                "pin":           pin,
                "totp":          totp_val,
                "client_id":     client_id,
                "redirect_uri":  redirect_uri,
                "response_type": "code",
            },
            # Format C — "mpin" key
            {
                "mobile_num":    mobile,
                "mpin":          pin,
                "totp":          totp_val,
                "client_id":     client_id,
                "redirect_uri":  redirect_uri,
                "response_type": "code",
            },
        ]

        for idx, pld in enumerate(payloads):
            try:
                r3 = session.post(
                    _AUTH_DIALOG,
                    json=pld,
                    headers=api_headers,
                    allow_redirects=False,
                    timeout=20,
                )
                _safe_log(f"  fmt{idx+1}: HTTP {r3.status_code}")

                # Check Location redirect
                loc = r3.headers.get("Location", "")
                if loc:
                    code = _extract_code(loc)
                    if code:
                        _safe_log(f"  auth_code from redirect ✅")
                        return code

                # Check JSON body
                try:
                    body = r3.json()
                    if "code" in body:
                        _safe_log(f"  auth_code in body ✅")
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

                # HTML body scan
                if r3.status_code == 200 and r3.text:
                    m = re.search(r"[?&]code=([^&\"'\s]+)", r3.text)
                    if m:
                        return m.group(1)

            except Exception as e:
                _safe_log(f"  fmt{idx+1}: {type(e).__name__}")

            time.sleep(0.3)
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
            "Debug karo:\n"
            "  1. TOTP verify karo:\n"
            "     python -c \"import pyotp; print(pyotp.TOTP('YOUR_SECRET').now())\"\n"
            "     Upstox app ke OTP se match karna chahiye\n"
            "  2. UPSTOX_PIN — 6-digit (Upstox app login PIN)\n"
            "  3. UPSTOX_MOBILE — 10-digit bina +91\n"
            "  4. UPSTOX_REDIRECT_URI — developer.upstox.com pe jo set hai exactly wahi\n"
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
