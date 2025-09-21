from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text

from ..deps import get_current_user
from ..core.db import session_scope


class StrategyIn(BaseModel):
    sku: str
    min_price: float | None = None
    max_price: float | None = None
    target_margin: float | None = None
    sensitivity: float | None = None


router = APIRouter(prefix="/strategy", tags=["strategy"])


@router.post("")
def save_strategy(payload: StrategyIn, user: str = Depends(get_current_user)):
    with session_scope() as s:
        s.execute(text("DELETE FROM my_inventory WHERE sku=:sku"), {"sku": payload.sku})
        s.execute(
            text(
                """
                INSERT INTO my_inventory (sku, min_price, max_price, target_margin, sensitivity)
                VALUES (:sku, :min_price, :max_price, :target_margin, :sensitivity)
                """
            ),
            payload.dict(),
        )
    return {"ok": True}
