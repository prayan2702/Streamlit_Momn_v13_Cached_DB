"""
upstox_auto_auth.py
===================
GitHub Actions mein headless Upstox OAuth.
pyotp + requests se browser simulate karta hai.

SECURITY:
  - Credentials kabhi log/print nahi hote
  - Token sirf masked form mein dikhta hai
  - Error messages mein credential values nahi hote
  - Sab sensitive values env vars se aate hain (GitHub Secrets)

Required env vars (GitHub Secrets se inject hote hain):
    UPSTOX_CLIENT_ID
    UPSTOX_CLIENT_SECRET
    UPSTOX_REDIRECT_URI
    UPSTOX_MOBILE         (10-digit, bina +91)
    UPSTOX_PIN            (6-digit login PIN)
    UPSTOX_TOTP_SECRET    (base32 key — Upstox 2FA setup se)
"""

import os
import re
import time
import urllib.parse

import pyotp
import requests

# ── Constants ─────────────────────────────────────────────────
_AUTH_DIALOG = "https://api.upstox.com/v2/login/authorization/dialog"
_TOKEN_URL   = "https://api.upstox.com/v2/login/authorization/token"

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ── Security helpers ──────────────────────────────────────────
def _mask(value: str, visible: int = 4) -> str:
    """Token/secret ka sirf pehle N chars dikhao, baaki mask karo."""
    if not value:
        return "***"
    return value[:visible] + "***"


def _safe_log(msg: str):
    """Print karo — credentials kabhi is function se pass nahi hone chahiye."""
    print(f"[upstox_auth] {msg}", flush=True)


def _extract_code(url: str) -> str | None:
    """Redirect URL se auth_code nikalo."""
    try:
        params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        codes  = params.get("code", [])
        return codes[0] if codes else None
    except Exception:
        return None


# ── Token exchange ────────────────────────────────────────────
def _exchange_code(
    session       : requests.Session,
    auth_code     : str,
    client_id     : str,
    client_secret : str,
    redirect_uri  : str,
) -> str:
    """auth_code → access_token. Credentials log nahi hote."""
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
            headers={"Content-Type": "application/x-www-form-urlencoded",
                     "Accept": "application/json"},
            timeout=20,
        )
        resp.raise_for_status()
        token = resp.json().get("access_token", "")
        if not token:
            # Response body print mat karo — client_secret ho sakta hai
            raise RuntimeError("access_token missing in token endpoint response")
        _safe_log(f"  access_token received: {_mask(token)} (len={len(token)})")
        return token
    except requests.HTTPError as e:
        # Status code OK hai log karna, response body nahi (credentials ho sakte hain)
        raise RuntimeError(
            f"Token exchange failed: HTTP {e.response.status_code}. "
            "Check CLIENT_ID, CLIENT_SECRET, REDIRECT_URI in GitHub Secrets."
        ) from None
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Token exchange failed: {type(e).__name__}") from None


# ── Main auth flow ────────────────────────────────────────────
def get_upstox_token_automated(
    client_id     : str,
    client_secret : str,
    redirect_uri  : str,
    mobile        : str,
    pin           : str,
    totp_secret   : str,
) -> str:
    """
    Fully automated Upstox OAuth.
    Returns access_token string.
    Raises RuntimeError with safe (no-credential) message on failure.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": _BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })

    auth_params = {
        "response_type": "code",
        "client_id":     client_id,
        "redirect_uri":  redirect_uri,
    }

    # ── Step 1: Auth page load → cookies ──────────────────────
    _safe_log("Step 1: Loading Upstox auth page...")
    try:
        r1 = session.get(_AUTH_DIALOG, params=auth_params, timeout=20)
        _safe_log(f"  Status: {r1.status_code}")
    except Exception as e:
        raise RuntimeError(f"Step 1 failed (network error): {type(e).__name__}") from None

    time.sleep(1)

    json_headers = {
        "User-Agent":   _BROWSER_UA,
        "Accept":       "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin":       "https://api.upstox.com",
        "Referer":      _AUTH_DIALOG,
    }

    # ── Step 2: Submit mobile ──────────────────────────────────
    # Mobile ko partially mask karke log karo (privacy)
    _safe_log(f"Step 2: Submitting mobile {_mask(mobile, 3)}XXXXXXX...")
    try:
        r2 = session.post(
            _AUTH_DIALOG,
            params=auth_params,
            json={"mobile_num": mobile},
            headers=json_headers,
            allow_redirects=False,
            timeout=20,
        )
        _safe_log(f"  Status: {r2.status_code}")

        # Early redirect check
        loc = r2.headers.get("Location", "")
        if loc:
            code = _extract_code(loc)
            if code:
                _safe_log("  Got auth_code at step 2")
                return _exchange_code(session, code, client_id, client_secret, redirect_uri)

    except Exception as e:
        raise RuntimeError(f"Step 2 failed (mobile submit): {type(e).__name__}") from None

    time.sleep(0.5)

    # ── Step 3: Submit PIN + TOTP ──────────────────────────────
    # TOTP aur PIN KABHI log nahi karte
    _safe_log("Step 3: Submitting PIN + TOTP (values masked)...")

    totp_code = pyotp.TOTP(totp_secret).now()
    # Note: totp_code log nahi ho raha

    # Do formats try karo (Upstox ne kabhi kabhi change kiya hai)
    payloads = [
        {"mobile_num": mobile, "client_secret": pin, "totp": totp_code},
        {"mobile_num": mobile, "pin":            pin, "totp": totp_code},
    ]

    auth_code = None
    for i, payload in enumerate(payloads):
        try:
            r3 = session.post(
                _AUTH_DIALOG,
                params=auth_params,
                json=payload,
                headers=json_headers,
                allow_redirects=False,
                timeout=20,
            )
            # Status code log karo, body/payload nahi
            _safe_log(f"  Format {chr(65+i)}: HTTP {r3.status_code}")

            loc = r3.headers.get("Location", "")
            if loc:
                auth_code = _extract_code(loc)
                if auth_code:
                    _safe_log(f"  auth_code extracted from redirect")
                    break

            # JSON body check (payload values nahi print karte)
            try:
                body = r3.json()
                if "code" in body:
                    auth_code = body["code"]
                    _safe_log("  auth_code found in response body")
                    break
                if body.get("status") == "error":
                    err_type = body.get("errors", [{}])
                    # Error message print karo par bina sensitive values ke
                    _safe_log(f"  API error type: {err_type[0].get('errorCode', 'unknown') if err_type else 'unknown'}")
            except Exception:
                pass

            # HTML body mein code search
            if r3.status_code == 200 and r3.text:
                match = re.search(r"[?&]code=([^&\"'\s]+)", r3.text)
                if match:
                    auth_code = match.group(1)
                    _safe_log("  auth_code found in HTML response")
                    break

        except Exception as e:
            _safe_log(f"  Format {chr(65+i)} error: {type(e).__name__}")

        time.sleep(0.3)

    # ── TOTP window expiry retry ───────────────────────────────
    if not auth_code:
        _safe_log("  TOTP window may have expired — waiting 31s for next window...")
        time.sleep(31)
        totp_code = pyotp.TOTP(totp_secret).now()
        _safe_log("  Retrying with fresh TOTP (value masked)...")

        for payload_template in payloads:
            payload_template["totp"] = totp_code
            try:
                r3 = session.post(
                    _AUTH_DIALOG,
                    params=auth_params,
                    json=payload_template,
                    headers=json_headers,
                    allow_redirects=False,
                    timeout=20,
                )
                loc = r3.headers.get("Location", "")
                if loc:
                    auth_code = _extract_code(loc)
                    if auth_code:
                        _safe_log("  auth_code extracted (retry)")
                        break
            except Exception as e:
                _safe_log(f"  Retry error: {type(e).__name__}")

    if not auth_code:
        # Error message mein credential values NAHI hain
        raise RuntimeError(
            "Upstox auth failed — auth_code nahi mila.\n"
            "Possible causes (check GitHub Secrets):\n"
            "  1. UPSTOX_PIN galat\n"
            "  2. UPSTOX_TOTP_SECRET galat (base32 string hona chahiye)\n"
            "  3. UPSTOX_MOBILE galat (10-digit, bina +91)\n"
            "  4. UPSTOX_REDIRECT_URI app settings se match nahi karta\n"
            "  5. Upstox ne login page update kar diya\n"
            "Credentials ki actual values kabhi logs mein nahi dikhti."
        )

    return _exchange_code(session, auth_code, client_id, client_secret, redirect_uri)


# ── Env-var wrapper ───────────────────────────────────────────
def get_token_from_env() -> str:
    """
    GitHub Secrets → env vars → access_token.
    Missing secrets check karta hai — values kabhi print nahi karta.
    """
    required = [
        "UPSTOX_CLIENT_ID",
        "UPSTOX_CLIENT_SECRET",
        "UPSTOX_REDIRECT_URI",
        "UPSTOX_MOBILE",
        "UPSTOX_PIN",
        "UPSTOX_TOTP_SECRET",
    ]
    missing = [k for k in required if not os.getenv(k, "").strip()]
    if missing:
        raise RuntimeError(
            f"GitHub Secrets missing: {missing}\n"
            "GitHub repo → Settings → Secrets and variables → Actions → New repository secret"
        )

    return get_upstox_token_automated(
        client_id     = os.environ["UPSTOX_CLIENT_ID"],
        client_secret = os.environ["UPSTOX_CLIENT_SECRET"],
        redirect_uri  = os.environ["UPSTOX_REDIRECT_URI"],
        mobile        = os.environ["UPSTOX_MOBILE"],
        pin           = os.environ["UPSTOX_PIN"],
        totp_secret   = os.environ["UPSTOX_TOTP_SECRET"],
    )
