from __future__ import annotations

from typing import Callable, List, Tuple


def optimize(
    p0: float,
    c: float,
    fee: float,
    q_func: Callable[[float], float],
    pmin: float,
    pmax: float,
    step: float = 1000.0,
) -> tuple[tuple[float, float, float], List[tuple[float, float, float]]]:
    grid: List[float] = []
    v = p0
    while v >= pmin:
        grid.append(round(v, 2))
        v -= step
    v = p0 + step
    while v <= pmax:
        grid.append(round(v, 2))
        v += step
    grid = sorted(set(grid))
    candidates: List[tuple[float, float, float]] = []
    for p in grid:
        q = max(0.0, q_func(p))
        profit = (p * (1 - fee) - c) * q
        candidates.append((p, q, profit))
    candidates.sort(key=lambda x: x[2], reverse=True)
    return candidates[0], candidates

