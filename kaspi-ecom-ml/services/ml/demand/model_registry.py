from __future__ import annotations

from pathlib import Path


REG_DIR = Path("models/demand")


def latest_model_path() -> Path | None:
    if not REG_DIR.exists():
        return None
    files = sorted(REG_DIR.glob("*.pkl"))
    return files[-1] if files else None

