"""
fyers_auth.py
=============
Fyers API v3 authentication for Streamlit app (live data).
Credentials Streamlit Secrets se aate hain — kabhi hardcode mat karo.

Fyers Auth Flow (2-step):
  1. Browser-based OAuth:  user link pe click kare → auth code milta hai → access_token
  2. Automated TOTP:       headless login (GitHub Actions ke liye)

Streamlit ke liye BROWSER FLOW use hoga (user manually login kare).
GitHub Actions cache builder ke liye AUTOMATED TOTP flow use hoga.

Streamlit Secrets mein add karo:
  [fyers]
  app_id       = "XY12345-100"      # app_id = client_id (format: APPID-100)
  secret_id    = "xxxxxxxxxxxx"     # secret key from myapi.fyers.in
  redirect_uri = "https://127.0.0.1"  # same as registered in Fyers app

Usage:
  from fyers_auth import get_fyers_client, render_fyers_auth_sidebar
  fyers = get_fyers_client()  # returns FyersModel object or None

Access token format: "XY12345-100:eyJhbGc..."  (app_id + ":" + token)
Token valid: current trading day only (auto-expires at midnight)

NSE Symbol format: NSE:RELIANCE-EQ (NOT RELIANCE.NS)
Data available from: 2017-07-03 (Fyers limitation — NOT from 2000)
"""

import hashlib
import time
import streamlit as st

# ── Try fyers_apiv3 import ─────────────────────────────────────
try:
    from fyers_apiv3 import fyersModel
    from fyers_apiv3.fyersModel import SessionModel
    _FYERS_OK = True
except ImportError:
    _FYERS_OK = False


def _get_secrets() -> dict:
    fy = st.secrets.get("fyers", {})
    return {
        "app_id":       fy.get("app_id",       "").strip(),
        "secret_id":    fy.get("secret_id",     "").strip(),
        "redirect_uri": fy.get("redirect_uri",  "https://127.0.0.1").strip(),
    }


def _validate_secrets(creds: dict) -> list[str]:
    return [k for k in ["app_id", "secret_id"] if not creds.get(k)]


def _make_auth_url(app_id: str, secret_id: str, redirect_uri: str) -> str:
    """Fyers OAuth login URL generate karo."""
    try:
        session = SessionModel(
            client_id    = app_id,
            secret_key   = secret_id,
            redirect_uri = redirect_uri,
            response_type= "code",
            grant_type   = "authorization_code",
        )
        return session.generate_authcode()
    except Exception as e:
        return f"ERROR:{e}"


def _exchange_code_for_token(
    app_id: str, secret_id: str, redirect_uri: str, auth_code: str
) -> str | None:
    """Auth code → access token exchange."""
    try:
        session = SessionModel(
            client_id    = app_id,
            secret_key   = secret_id,
            redirect_uri = redirect_uri,
            response_type= "code",
            grant_type   = "authorization_code",
        )
        session.set_token(auth_code)
        resp = session.generate_token()
        if resp.get("s") == "ok":
            raw_token = resp["access_token"]
            # Fyers format: "app_id:token"
            return f"{app_id}:{raw_token}"
        else:
            return None
    except Exception:
        return None


def get_fyers_client(sidebar: bool = False) -> "fyersModel.FyersModel | None":
    """
    Authenticated FyersModel object return karo.

    Flow:
      1. Session state mein saved access_token check karo
      2. Agar nahi → sidebar mein auth link show karo
      3. User auth_code paste kare → token exchange → client ready

    sidebar=True → Streamlit sidebar mein auth UI show karo
    Returns FyersModel on success, None otherwise.
    """
    if not _FYERS_OK:
        _msg = (
            "⚠️ `fyers-apiv3` install nahi hai.\n"
            "`requirements.txt` mein add karo:\n"
            "```\nfyers-apiv3\n```"
        )
        if sidebar:
            st.sidebar.warning(_msg)
        else:
            st.warning(_msg)
        return None

    creds   = _get_secrets()
    missing = _validate_secrets(creds)

    if missing:
        _miss_str = ", ".join(f"`fyers.{k}`" for k in missing)
        if sidebar:
            st.sidebar.markdown(f"""
            <div style="background:#fee2e2;border:1px solid #fca5a5;border-left:4px solid #dc2626;
                        border-radius:10px;padding:12px 16px;font-size:12px;color:#7f1d1d;margin:6px 0;">
              🔴 <b>Fyers Secrets Missing</b><br>
              Streamlit Secrets mein add karo: {_miss_str}
            </div>""", unsafe_allow_html=True)
        return None

    # ── Check for saved token ──────────────────────────────────
    saved_token = st.session_state.get("_fyers_access_token", "")
    if saved_token:
        try:
            fyers = fyersModel.FyersModel(
                client_id = creds["app_id"],
                token     = saved_token,
                is_async  = False,
                log_path  = "",
            )
            if sidebar:
                masked = creds["app_id"][:6] + "***"
                st.sidebar.markdown(f"""
                <div style="background:#dcfce7;border:1px solid #86efac;border-left:4px solid #16a34a;
                            border-radius:10px;padding:10px 14px;font-size:12px;color:#15803d;margin:6px 0;">
                  🟢 <b>Fyers Connected</b><br>
                  <span style="font-size:11px;">App: {masked} &nbsp;·&nbsp; Token active</span>
                </div>""", unsafe_allow_html=True)
                if sidebar and st.sidebar.button("🔄 Refresh Fyers Token", key="_fyers_refresh"):
                    st.session_state.pop("_fyers_access_token", None)
                    st.rerun()
            return fyers
        except Exception:
            st.session_state.pop("_fyers_access_token", None)

    # ── No token — show OAuth login flow ──────────────────────
    auth_url = _make_auth_url(
        creds["app_id"], creds["secret_id"], creds["redirect_uri"]
    )

    _target = st.sidebar if sidebar else st

    if auth_url.startswith("ERROR"):
        _target.error(f"❌ Fyers auth URL generation failed: {auth_url}")
        return None

    _target.markdown(f"""
    <div style="background:#dbeafe;border:1px solid #93c5fd;border-left:4px solid #2563eb;
                border-radius:10px;padding:12px 16px;font-size:12px;color:#1e3a5f;margin:6px 0;">
      🔵 <b>Fyers Login Required</b><br>
      <ol style="margin:6px 0 0 0;padding-left:16px;line-height:2;">
        <li><a href="{auth_url}" target="_blank" style="color:#1d4ed8;font-weight:700;">
            ▶ Fyers Login Link</a> pe click karo</li>
        <li>Fyers account se login karo (TOTP/PIN)</li>
        <li>Redirect hone ke baad URL se <code>auth_code=XXXXX</code> copy karo</li>
        <li>Neeche paste karo</li>
      </ol>
    </div>""", unsafe_allow_html=True)

    _widget_key = "fyers_auth_code_input"
    auth_code_raw = _target.text_input(
        "🔑 Auth Code paste karo",
        key=_widget_key,
        placeholder="auth_code=ey...",
        help="Fyers redirect URL se auth_code parameter copy karo"
    )

    if auth_code_raw:
        # Strip "auth_code=" prefix if user pastes full URL param
        auth_code = auth_code_raw.strip()
        if "auth_code=" in auth_code:
            auth_code = auth_code.split("auth_code=")[-1].split("&")[0].strip()

        if _target.button("✅ Generate Fyers Token", key="_fyers_gen_token"):
            with st.spinner("Fyers token generate ho raha hai..."):
                token = _exchange_code_for_token(
                    creds["app_id"], creds["secret_id"],
                    creds["redirect_uri"], auth_code
                )
            if token:
                st.session_state["_fyers_access_token"] = token
                st.success("✅ Fyers token generated!")
                st.rerun()
            else:
                _target.error(
                    "❌ Token exchange failed. Auth code expired ya galat hai.\n"
                    "Auth code sirf ek baar use ho sakta hai — login link dobara kholo."
                )
    return None
