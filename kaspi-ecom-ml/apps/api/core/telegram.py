from __future__ import annotations

import os
import io
import requests


def _get(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def is_configured() -> bool:
    return bool(_get("TG_BOT_TOKEN") and _get("TG_CHAT_ID"))


def send_text(text: str, parse_mode: str = "HTML") -> bool:
    token = _get("TG_BOT_TOKEN")
    chat_id = _get("TG_CHAT_ID")
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(
            url,
            data={
                "chat_id": chat_id,
                "text": text[:4000],
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        return 200 <= r.status_code < 300
    except Exception:
        return False


def send_csv(filename: str, csv_bytes: bytes, caption: str = "") -> bool:
    token = _get("TG_BOT_TOKEN")
    chat_id = _get("TG_CHAT_ID")
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    files = {"document": (filename, io.BytesIO(csv_bytes), "text/csv")}
    data = {"chat_id": chat_id, "caption": caption}
    try:
        r = requests.post(url, files=files, data=data, timeout=30)
        return 200 <= r.status_code < 300
    except Exception:
        return False

