import os
import requests

def _get_secret(name: str, default: str = "") -> str:
    # Работает и локально, и в Streamlit Cloud
    try:
        import streamlit as st
        return st.secrets.get(name, os.getenv(name, default))
    except Exception:
        return os.getenv(name, default)

def tg_send(text: str, parse_mode: str = "HTML") -> bool:
    token = _get_secret("TG_BOT_TOKEN")
    chat_id = _get_secret("TG_CHAT_ID")
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, data={
        "chat_id": chat_id,
        "text": text[:4000],   # лимит Telegram
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }, timeout=15)
    return 200 <= resp.status_code < 300
