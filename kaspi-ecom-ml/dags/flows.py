from __future__ import annotations

from datetime import date
from prefect import flow, task
from sqlalchemy import create_engine, text
import os


@task
def build_features():
    from services.etl.build_features_daily import run as build

    build()


@task
def train_demand_model() -> str:
    from services.ml.demand.train_demand import train

    return train()


@task
def refresh_price_reco(model_ver: str):
    url = os.getenv("DATABASE_URL", "postgresql+psycopg://user:pass@postgres:5432/kaspi")
    eng = create_engine(url, future=True)
    with eng.begin() as conn:
        rows = conn.execute(text("SELECT sku, own_price FROM features_daily ORDER BY date DESC LIMIT 10")).fetchall()
        for r in rows:
            sku = r[0]
            own = float(r[1] or 300000.0)
            conn.execute(
                text(
                    "INSERT INTO price_reco (sku, price, expected_qty, expected_profit, explain, model_ver)"
                    " VALUES (:s, :p, :q, :pr, :e, :m)"
                ),
                {"s": sku, "p": own, "q": 8.0, "pr": 30000.0, "e": "demo", "m": model_ver},
            )


@task
def anomaly_check():
    from services.ml.anomaly.run_anomaly import run as run_anomaly

    run_anomaly()


@task
def export_snapshots():
    # TODO: export CSVs to S3
    pass


@flow(name="nightly_training")
def nightly_training():
    build_features()
    ver = train_demand_model()
    refresh_price_reco(ver)
    anomaly_check()
    export_snapshots()


@flow(name="inference_schedule")
def inference_schedule():
    ver = "latest"
    refresh_price_reco(ver)
