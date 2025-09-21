from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

from ..deps import get_current_user
from ..core.db import session_scope
from ..services.pricing_service import optimize_price
from ..services.forecast_service import simple_forecast
from ..schemas import PriceReco
from ..core.db import session_scope

router = APIRouter(prefix="/products", tags=["products"])


@router.get("/{sku}/summary")
def product_summary(sku: str, user: str = Depends(get_current_user)):
    with session_scope() as s:
        row = s.execute(text("SELECT sku FROM products WHERE sku=:sku"), {"sku": sku}).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="SKU not found")
        # Demo aggregates from features_daily
        fd = s.execute(
            text(
                """
                SELECT date, competitor_min_price, competitor_avg_price, own_price, sales_units
                FROM features_daily WHERE sku=:sku ORDER BY date DESC LIMIT 30
                """
            ),
            {"sku": sku},
        ).fetchall()
        return {
            "sku": sku,
            "features": [dict(r._mapping) for r in fd],
        }


@router.get("/{sku}/profit-trend")
def profit_trend(sku: str, days: int = 90, user: str = Depends(get_current_user)):
    sql = text(
        """
        SELECT date, own_price, competitor_avg_price, sales_units
        FROM features_daily
        WHERE sku = :sku AND date >= (CURRENT_DATE - :days)
        ORDER BY date
        """
    )
    with session_scope() as s:
        rows = s.execute(sql, {"sku": sku, "days": days}).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="no data for SKU")
    data = [dict(r._mapping) for r in rows]
    for d in data:
        price = d.get("own_price") or 0.0
        sales = d.get("sales_units") or 0.0
        d["revenue"] = float(price or 0.0) * float(sales or 0.0)
        d["profit"] = float(price or 0.0) * 0.9 * float(sales or 0.0)  # rough margin assumption
    return {"sku": sku, "data": data}


@router.get("/sku-list")
def sku_list(limit: int = 20, user: str = Depends(get_current_user)):
    """Return recent SKUs for quick selection on UI."""
    sql = text(
        """
        SELECT sku, title
        FROM products
        ORDER BY created_at DESC NULLS LAST, id DESC
        LIMIT :lim
        """
    )
    with session_scope() as s:
        rows = s.execute(sql, {"lim": limit}).fetchall()
        return [dict(r._mapping) for r in rows]


@router.get("/{sku}/forecast")
def product_forecast(sku: str, horizon: int = 7, user: str = Depends(get_current_user)):
    data = simple_forecast(sku, horizon)
    return {"sku": sku, "forecast": [{"date": d.isoformat(), "q": q} for d, q in data]}


@router.get("/{sku}/price-reco", response_model=PriceReco)
def price_reco(
    sku: str,
    trial_price: Optional[float] = None,
    user: str = Depends(get_current_user),
):
    # Pull simple context from DB, fallback defaults
    with session_scope() as s:
        prod = s.execute(text("SELECT sku FROM products WHERE sku=:sku"), {"sku": sku}).fetchone()
        if not prod:
            raise HTTPException(status_code=404, detail="SKU not found")
        feat = s.execute(
            text(
                "SELECT competitor_avg_price, own_price FROM features_daily WHERE sku=:sku ORDER BY date DESC LIMIT 1"
            ),
            {"sku": sku},
        ).fetchone()
    avg = float(feat[0]) if feat and feat[0] is not None else 300000.0
    own = float(feat[1]) if feat and feat[1] is not None else avg

    def qty_curve(p: float) -> float:
        base = 10.0
        # simple price elasticity
        return max(0.0, base * (1.0 + (-1.0) * ((p - own) / max(own, 1.0))))

    cost = own * 0.75
    fee = 0.10
    p0 = trial_price or own
    pmin = own * 0.9
    pmax = own * 1.1
    best, _grid, explain = optimize_price(cost, fee, qty_curve, p0, pmin, pmax, step=1000.0)
    price, qty, profit = best
    return PriceReco(
        sku=sku,
        reco_price=float(price),
        expected_qty=float(qty),
        expected_profit=float(profit),
        explain=explain,
        model_ver="demo-v1",
    )


@router.get("/recommendations")
def list_recommendations(limit: int = 100, user: str = Depends(get_current_user)):
    """Return latest price recommendations per SKU with product title."""
    sql = text(
        """
        SELECT pr.sku, p.title, pr.price AS reco_price, pr.expected_qty, pr.expected_profit, pr.model_ver, pr.ts
        FROM price_reco pr
        JOIN (
          SELECT sku, MAX(ts) AS ts
          FROM price_reco
          GROUP BY sku
        ) last ON pr.sku = last.sku AND pr.ts = last.ts
        LEFT JOIN products p ON p.sku = pr.sku
        ORDER BY pr.ts DESC
        LIMIT :lim
        """
    )
    with session_scope() as s:
        rows = s.execute(sql, {"lim": limit}).fetchall()
        return [dict(r._mapping) for r in rows]
