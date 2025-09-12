from typing import Callable

def price_to_demand_linear(base_q: float, base_price: float, elasticity: float) -> Callable[[float], float]:
    """
    Простая линейная аппроксимация зависимости спроса от цены.
    elasticity — отрицательная (например, -1.0).
    q(p) = base_q * (1 + elasticity * ((base_price - p) / base_price))
    """
    def q(p: float) -> float:
        return max(0.0, base_q * (1.0 + elasticity * ((base_price - p) / max(1.0, base_price))))
    return q
