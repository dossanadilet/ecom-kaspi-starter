from __future__ import annotations

from __future__ import annotations

from typing import List, Sequence
import numpy as np
from sklearn.ensemble import IsolationForest


def simple_rule_flags(series: Sequence[float], thresh: float = 0.3) -> List[int]:
    out: List[int] = []
    for i in range(1, len(series)):
        a, b = series[i - 1], series[i]
        if a and abs(b - a) / abs(a) > thresh:
            out.append(i)
    return out


def isolation_forest_flags(values: Sequence[float], contamination: float = 0.1) -> List[int]:
    arr = np.array(values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) < 5:
        return []
    X = arr.reshape(-1, 1)
    model = IsolationForest(contamination=min(contamination, 0.45), random_state=42)
    preds = model.fit_predict(X)
    return [i for i, p in enumerate(preds) if p == -1]


def relative_change(current: float, baseline: float) -> float:
    if baseline == 0:
        return float('inf') if current else 0.0
    return (current - baseline) / abs(baseline)
