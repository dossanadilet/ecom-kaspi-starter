from __future__ import annotations

from typing import Callable


def linear_elasticity(base_q: float, base_price: float, elasticity: float = -1.0) -> Callable[[float], float]:
    def q(p: float) -> float:
        return max(0.0, base_q * (1.0 + elasticity * ((p - base_price) / max(1.0, base_price))))
    return q

