from __future__ import annotations

import pickle
from datetime import date, timedelta
from pathlib import Path
import numpy as np
from .model_registry import REG_DIR
from ..common.utils import ensure_dir


def gen_demo_series(n: int = 60) -> np.ndarray:
    x = np.arange(n)
    season = 10 + 3 * np.sin(2 * np.pi * x / 7)
    noise = np.random.normal(0, 0.5, size=n)
    return season + noise


def train() -> str:
    ensure_dir(REG_DIR)
    series = gen_demo_series()
    model = {"mean": float(series.mean())}
    ver = date.today().isoformat()
    path = REG_DIR / f"demand_{ver}.pkl"
    with open(path, "wb") as f:
        pickle.dump(model, f)
    return f"demo-{ver}"


if __name__ == "__main__":
    print(train())

