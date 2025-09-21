from __future__ import annotations

from datetime import date, timedelta
from fastapi import APIRouter, Depends
from sqlalchemy import text

from ..deps import get_current_user
from ..core.db import session_scope

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/overview")
def overview(from_date: str | None = None, to_date: str | None = None, user: str = Depends(get_current_user)):
    """Return simple aggregates for the dashboard. Falls back to demo seed if empty."""
    with session_scope() as s:
        total_sku = s.execute(text("SELECT COUNT(*) FROM products")).scalar() or 0
        alerts = s.execute(text("SELECT COUNT(*) FROM alerts WHERE ack=false")).scalar() or 0
        # get last 7 days sales from features_daily
        d_to = date.fromisoformat(to_date) if to_date else date.today()
        d_from = date.fromisoformat(from_date) if from_date else d_to - timedelta(days=7)
        sales = s.execute(
            text(
                "SELECT SUM(sales_units)::float FROM features_daily WHERE date BETWEEN :a AND :b"
            ),
            {"a": d_from, "b": d_to},
        ).scalar() or 0.0
    return {"total_sku": total_sku, "open_alerts": alerts, "sales_units_7d": sales}
