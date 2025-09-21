from __future__ import annotations

from typing import Callable, List, Tuple


def optimize_price(
    cost: float,
    fee_pct: float,
    predict_qty: Callable[[float], float],
    p0: float,
    pmin: float,
    pmax: float,
    step: float = 1000.0,
) -> tuple[tuple[float, float, float], List[tuple[float, float, float]], str]:
    """Simple grid search for price optimizing profit.

    Returns (best_triplet(price, qty, profit), grid_list, explain)
    """
    p = max(pmin, min(p0, pmax))
    grid: List[float] = []
    # expand grid around p0
    v = p
    while v >= pmin:
        grid.append(round(v, 2))
        v -= step
    v = p + step
    while v <= pmax:
        grid.append(round(v, 2))
        v += step
    if p not in grid:
        grid.append(round(p, 2))
    grid = sorted(set(grid))

    candidates: List[Tuple[float, float, float]] = []
    for price in grid:
        qty = max(0.0, predict_qty(price))
        profit = (price * (1 - fee_pct) - cost) * qty
        candidates.append((price, qty, profit))

    candidates.sort(key=lambda x: x[2], reverse=True)
    best = candidates[0]
    explain = (
        f"grid={len(grid)}; cost={cost:.0f}; fee={fee_pct*100:.1f}%; "
        f"p0={p0:.0f}; pmin={pmin:.0f}; pmax={pmax:.0f}"
    )
    return best, candidates, explain

