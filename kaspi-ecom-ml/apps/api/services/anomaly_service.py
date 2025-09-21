from __future__ import annotations

from typing import List, Dict, Any


def detect_simple_rules(series: List[float], thresh: float = 0.3) -> List[int]:
    """Flag indices where relative change > thresh."""
    flags: List[int] = []
    for i in range(1, len(series)):
        prev, cur = series[i - 1], series[i]
        if prev and abs(cur - prev) / abs(prev) > thresh:
            flags.append(i)
    return flags


def explain_alert(idx: int, prev: float, cur: float) -> Dict[str, Any]:
    ch = 0.0 if prev == 0 else (cur - prev) / prev
    return {"index": idx, "change": ch, "prev": prev, "cur": cur}

