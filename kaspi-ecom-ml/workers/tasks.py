from __future__ import annotations

from .celery_app import celery_app


@celery_app.task
def recompute_sku(sku: str) -> dict:
    # TODO: implement
    return {"ok": True, "sku": sku}


@celery_app.task
def send_alert(alert_id: int) -> dict:
    # TODO: implement
    return {"ok": True, "id": alert_id}

