from typing import List, Tuple
from app.economics import profit_per_unit

def choose_price_grid(
    p0: float,
    c_land: float,
    mp_fee: float,
    q_func,
    grid = (-0.03, -0.01, 0.0, 0.01, 0.03)
) -> tuple[tuple[float,float,float], List[tuple[float,float,float]]]:
    """
    Перебор сетки цен вокруг текущей цены p0.
    q_func(p) должен вернуть прогноз спроса (шт/нед) при цене p.
    Возвращает: (лучший_кандидат, все_кандидаты)
    кандидат = (price, profit_week, q_week)
    """
    candidates: List[Tuple[float,float,float]] = []
    for g in grid:
        p = round(p0 * (1 + g), 2)
        q = max(0.0, q_func(p))
        profit = profit_per_unit(p, c_land, mp_fee) * q
        candidates.append((p, profit, q))
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0], candidates

