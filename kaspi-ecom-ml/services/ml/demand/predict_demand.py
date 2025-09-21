from __future__ import annotations

import pickle
from datetime import date, timedelta
from typing import List, Tuple
from .model_registry import latest_model_path


def predict(sku: str, days: int = 7) -> List[Tuple[date, float]]:
    mp = latest_model_path()
    mean = 8.0
    if mp and mp.exists():
        try:
            with open(mp, "rb") as f:
                model = pickle.load(f)
                mean = float(model.get("mean", mean))
        except Exception:
            pass
    out: List[Tuple[date, float]] = []
    for i in range(1, days + 1):
        d = date.today() + timedelta(days=i)
        out.append((d, mean))
    return out

