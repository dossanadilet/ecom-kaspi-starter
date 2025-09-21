from __future__ import annotations

from fastapi import APIRouter
from prometheus_client import CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import PlainTextResponse

router = APIRouter()


@router.get("/health")
def health():
    return {"status": "ok", "version": "v1.0.0", "env": "dev"}


@router.get("/metrics")
def metrics():
    registry = CollectorRegistry()  # default registry used implicitly
    data = generate_latest()
    return PlainTextResponse(content=data, media_type=CONTENT_TYPE_LATEST)

