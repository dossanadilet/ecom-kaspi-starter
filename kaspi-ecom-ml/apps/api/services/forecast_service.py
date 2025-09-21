from __future__ import annotations

from datetime import date, timedelta
from typing import List, Tuple


def simple_forecast(sku: str, days: int = 7) -> List[Tuple[date, float]]:
    """Very simple seasonal-ish forecast stub.
    TODO: replace with loading latest model from registry.
    """
    base = 10.0 if "IPH" in sku else 7.0
    out: List[Tuple[date, float]] = []
    for i in range(1, days + 1):
        d = date.today() + timedelta(days=i)
        q = base * (1.0 + 0.1 * ((i % 7) in (5, 6)))  # weekend bump
        out.append((d, q))
    return out

