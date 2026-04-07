"""
angelone_auth.py
================
Angel One SmartAPI authentication for Streamlit app (live data).
Credentials Streamlit Secrets se aate hain — kabhi hardcode mat karo.

Streamlit Secrets mein add karo:
  [angelone]
  client_id   = "R1234567"
  api_key     = "xxxxxxxxxxxxxxxxxxxxxxxx"
  password    = "your_password"
  totp_secret = "JBSWY3DPEHPK3PXP"

Usage:
  from angelone_auth import get_angelone_client
  smart_api = get_angelone_client()  # returns authenticated SmartConnect object

  # Sidebar status show karne ke liye:
  get_angelone_client(sidebar=True)

Angel One API Key Invalid fix:
  1. smartapi.angelone.in/new/apps → "Add App"
  2. App Name: "MomnScreener" (kuch bhi)
  3. Redirect URL: https://localhost (kuch bhi)
  4. Primary Static IP: 127.0.0.1
     (Historical data ke liye IP strict nahi hai — sirf placeholder chahiye)
  5. "Add" → API Key copy karo
  6. Streamlit Cloud → App Settings → Secrets → angelone.api_key update karo
  7. App restart karo
"""

import time
import streamlit as st

try:
    from SmartApi import SmartConnect
    import pyotp
    _SMARTAPI_OK = True
except ImportError:
    _SMARTAPI_OK = False


def _get_secrets() -> dict:
    """Streamlit Secrets se Angel One credentials lo."""
    ao = st.secrets.get("angelone", {})
    return {
        "client_id":   ao.get("client_id", "").strip(),
        "api_key":     ao.get("api_key",   "").strip(),
        "password":    ao.get("password",  "").strip(),
        "totp_secret": ao.get("totp_secret", "").strip(),
    }


def _validate_secrets(creds: dict) -> list[str]:
    """Missing fields return karo."""
    required = ["client_id", "api_key", "password", "totp_secret"]
    return [k for k in required if not creds.get(k)]


@st.cache_resource(show_spinner=False)
def _create_session(api_key: str, client_id: str, password: str, totp_secret: str):
    """
    Angel One SmartAPI session banao.
    @st.cache_resource = session objects cache karta hai (not serializable)
    TTL nahi hai — session ~24hr valid hota hai.
    Naya session = app restart ya st.cache_resource.clear()
    """
    obj  = SmartConnect(api_key=api_key)
    totp = pyotp.TOTP(totp_secret).now()
    data = obj.generateSession(client_id, password, totp)
    if data.get("status") is False:
        raise ValueError(data.get("message", "Session generation failed"))
    return obj


def get_angelone_client(sidebar: bool = False) -> "SmartConnect | None":
    """
    Authenticated SmartConnect object return karo.
    sidebar=True → Streamlit sidebar mein status/error show karo.

    Returns:
      SmartConnect object on success
      None on failure (sidebar mein error show hoga)
    """
    if not _SMARTAPI_OK:
        if sidebar:
            st.sidebar.warning(
                "⚠️ `smartapi-python` + `pyotp` install nahi hain.\n"
                "`requirements.txt` mein add karo:\n"
                "```\nsmartapi-python\npyotp\n```"
            )
        return None

    creds   = _get_secrets()
    missing = _validate_secrets(creds)

    if missing:
        _missing_keys = ", ".join(f"`angelone.{k}`" for k in missing)
        if sidebar:
            st.sidebar.markdown(f"""
            <div style="background:#fee2e2;border:1px solid #fca5a5;border-left:4px solid #dc2626;
                        border-radius:10px;padding:12px 16px;font-size:12px;color:#7f1d1d;margin:6px 0;">
              🔴 <b>Angel One Secrets Missing</b><br>
              <span>Streamlit Secrets mein add karo: {_missing_keys}</span>
            </div>""", unsafe_allow_html=True)
        return None

    # ── Try cached session or create new ──────────────────────
    try:
        smart_api = _create_session(
            creds["api_key"], creds["client_id"],
            creds["password"], creds["totp_secret"]
        )
        if sidebar:
            masked_id = creds["client_id"][:3] + "***"
            st.sidebar.markdown(f"""
            <div style="background:#dcfce7;border:1px solid #86efac;border-left:4px solid #16a34a;
                        border-radius:10px;padding:10px 14px;font-size:12px;color:#15803d;margin:6px 0;">
              🟢 <b>Angel One Connected</b><br>
              <span style="font-size:11px;">Client: {masked_id} &nbsp;·&nbsp; Session active</span>
            </div>""", unsafe_allow_html=True)
        return smart_api

    except ValueError as e:
        # API key invalid / session failed
        err_msg = str(e)
        if sidebar:
            _show_api_key_error(sidebar_mode=True, detail=err_msg)
        return None

    except Exception as e:
        if sidebar:
            st.sidebar.error(f"❌ Angel One connection error: {e}")
        return None


def _show_api_key_error(sidebar_mode: bool = True, detail: str = ""):
    """API Key Invalid ka detailed fix guide."""
    html = f"""
    <div style="background:#fee2e2;border:1px solid #fca5a5;border-left:4px solid #dc2626;
                border-radius:10px;padding:12px 16px;font-size:12px;color:#7f1d1d;margin:6px 0;">
      🔴 <b>Angel One API Key Invalid</b><br>
      <span style="font-size:11px;color:#991b1b;">
        Streamlit Secrets mein jo <code>api_key</code> hai woh expired ya galat hai.
        {f'<br>Error: {detail[:80]}' if detail else ''}
      </span>
      <br><br>
      <b>Fix karein:</b>
      <ol style="margin:4px 0 0 0;padding-left:16px;line-height:1.9;">
        <li><a href="https://smartapi.angelone.in/new/apps" target="_blank"
               style="color:#1d4ed8;">Angel One SmartAPI Console</a> open karein</li>
        <li>Apna existing app check karein (ya naya banayein)</li>
        <li>Naya <b>API Key</b> copy karein</li>
        <li>Streamlit Cloud → Secrets mein
            <code>angelone.api_key</code> update karein</li>
        <li>App restart karein</li>
      </ol>
      <div style="font-size:10.5px;margin-top:6px;color:#b91c1c;">
        💡 Tip: "Primary Static IP" mein <code>127.0.0.1</code> enter karo
        (historical data ke liye IP check enforce nahi hota)
      </div>
    </div>"""

    if sidebar_mode:
        st.sidebar.markdown(html, unsafe_allow_html=True)
    else:
        st.markdown(html, unsafe_allow_html=True)


def refresh_session():
    """Force new Angel One session (24hr expiry ke baad call karo)."""
    st.cache_resource.clear()
    st.rerun()
