from apps.api.services.pricing_service import optimize_price


def test_optimize_price_grid():
    def q(p: float) -> float:
        return max(0.0, 10 * (1.0 - 0.01 * ((p - 100.0) / 100.0)))

    best, candidates, explain = optimize_price(cost=60.0, fee_pct=0.1, predict_qty=q, p0=100.0, pmin=80.0, pmax=120.0, step=10.0)
    assert isinstance(best, tuple) and len(best) == 3
    assert any(c[2] for c in candidates)
    assert "grid=" in explain

