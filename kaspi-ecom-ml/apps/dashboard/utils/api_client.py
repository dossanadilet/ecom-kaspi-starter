from __future__ import annotations

import os
import requests
from requests.exceptions import RequestException


def _bases() -> list[str]:
    base = os.getenv("API_BASE", "").strip()
    candidates: list[str] = []
    if base:
        candidates.append(base)
        # If base points to docker service name, add localhost fallback
        if base.startswith("http://api:") or base.startswith("https://api:"):
            candidates.append(base.replace("api:", "localhost:"))
    else:
        candidates = ["http://api:8000", "http://localhost:8000"]
    # Deduplicate preserving order
    seen = set()
    out: list[str] = []
    for b in candidates:
        if b not in seen:
            out.append(b)
            seen.add(b)
    return out


def _request(method: str, path: str, token: str, **kwargs) -> requests.Response:
    last_err: Exception | None = None
    headers = kwargs.pop("headers", {}) or {}
    headers.setdefault("Authorization", f"Bearer {token}")
    for base in _bases():
        url = base.rstrip("/") + path
        try:
            r = requests.request(method, url, headers=headers, timeout=kwargs.pop("timeout", 15), **kwargs)
            r.raise_for_status()
            return r
        except RequestException as e:
            last_err = e
            continue
    # If nothing succeeded, raise last error
    if last_err:
        raise last_err
    raise RuntimeError("API request failed with no further details")


def api_get(path: str, token: str) -> dict:
    r = _request("GET", path, token)
    return r.json()


def api_post(path: str, token: str, json: dict | None = None) -> dict:
    r = _request("POST", path, token, json=json or {})
    return r.json()


def api_post_file(path: str, token: str, file) -> dict:
    files = {"file": (file.name, file.read(), "text/csv")}
    r = _request("POST", path, token, files=files, timeout=60)
    return r.json()
