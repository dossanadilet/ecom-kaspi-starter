from __future__ import annotations

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import health, dashboard, products, strategy, alerts, admin, digest
from .auth import router as auth_router

app = FastAPI(title="Kaspi E-com ML API", version="v1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(health.router)
app.include_router(dashboard.router)
app.include_router(products.router)
app.include_router(strategy.router)
app.include_router(alerts.router)
app.include_router(admin.router)
app.include_router(digest.router)

@app.get("/")
def root():
    return {"service": "kaspi-ecom-ml", "env": os.getenv("ENV", "dev")}
