from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text

from ..deps import get_current_user
from ..core.db import session_scope

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("")
def list_alerts(limit: int = 50, user: str = Depends(get_current_user)):
    with session_scope() as s:
        rows = s.execute(
            text("SELECT id, type, sku, payload, ack, ts FROM alerts ORDER BY ts DESC LIMIT :l"),
            {"l": limit},
        ).fetchall()
        return [dict(r._mapping) for r in rows]


@router.post("/{alert_id}/ack")
def ack_alert(alert_id: int, user: str = Depends(get_current_user)):
    with session_scope() as s:
        s.execute(text("UPDATE alerts SET ack=true WHERE id=:id"), {"id": alert_id})
    return {"ok": True}

