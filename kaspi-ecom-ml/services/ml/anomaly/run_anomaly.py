from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json

import pandas as pd
from sqlalchemy import create_engine, text

from .detect_anomalies import simple_rule_flags, relative_change
from apps.api.core import telegram as tg  # type: ignore


DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://user:pass@postgres:5432/kaspi")


@dataclass
class Anomaly:
    sku: str
    title: str
    metric: str
    current: float
    baseline: float
    delta: float
    ts: datetime


def _load_features(days: int = 30) -> pd.DataFrame:
    engine = create_engine(DB_URL, future=True)
    cutoff = datetime.utcnow() - timedelta(days=days)
    query = text(
        """
        SELECT fd.sku, p.title, fd.date, fd.own_price, fd.competitor_avg_price, fd.sales_units
        FROM features_daily fd
        LEFT JOIN products p ON p.sku = fd.sku
        WHERE fd.date >= :cutoff
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"cutoff": cutoff.date()})
    return df


def _detect(df: pd.DataFrame) -> List[Anomaly]:
    if df.empty:
        return []
    df = df.sort_values(["sku", "date"])
    anomalies: List[Anomaly] = []
    for sku, grp in df.groupby("sku"):
        grp = grp.dropna(subset=["own_price"])
        if grp.empty:
            continue
        latest = grp.iloc[-1]
        median_price = grp["own_price"].median()
        delta_price = relative_change(float(latest["own_price"]), float(median_price))
        if abs(delta_price) > 0.15:
            anomalies.append(
                Anomaly(
                    sku=sku,
                    title=str(latest.get("title") or ""),
                    metric="own_price",
                    current=float(latest["own_price"]),
                    baseline=float(median_price),
                    delta=float(delta_price),
                    ts=pd.to_datetime(latest["date"]).to_pydatetime(),
                )
            )

        if "sales_units" in grp.columns and grp["sales_units"].notna().sum() >= 5:
            sales = grp.dropna(subset=["sales_units"])
            median_sales = sales["sales_units"].median()
            latest_sales = float(sales.iloc[-1]["sales_units"])
            delta_sales = relative_change(latest_sales, float(median_sales))
            if abs(delta_sales) > 0.5:
                anomalies.append(
                    Anomaly(
                        sku=sku,
                        title=str(latest.get("title") or ""),
                        metric="sales_units",
                        current=latest_sales,
                        baseline=float(median_sales),
                        delta=float(delta_sales),
                        ts=pd.to_datetime(sales.iloc[-1]["date"]).to_pydatetime(),
                    )
                )
    return anomalies


def _write_alerts(items: List[Anomaly]) -> None:
    if not items:
        return
    engine = create_engine(DB_URL, future=True)
    rows = [
        {
            "type": f"anomaly_{item.metric}",
            "sku": item.sku,
            "payload": {
                "title": item.title,
                "current": item.current,
                "baseline": item.baseline,
                "delta": item.delta,
                "ts": item.ts.isoformat(),
            },
        }
        for item in items
    ]
    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(
                    "INSERT INTO alerts (type, sku, payload, ack, ts) VALUES (:type, :sku, :payload::jsonb, false, NOW())"
                ),
                {"type": row["type"], "sku": row["sku"], "payload": json.dumps(row["payload"])},
            )


def _notify(items: List[Anomaly]) -> None:
    if not tg.is_configured() or not items:
        return
    lines = ["<b>Аномалии</b>"]
    for item in items[:5]:
        title = item.title[:40] + "…" if item.title and len(item.title) > 40 else item.title
        lines.append(
            f"• {item.sku} — {title} — {item.metric}: {item.current:.0f} (база {item.baseline:.0f}, Δ={item.delta*100:.0f}%)"
        )
    tg.send_text("\n".join(lines))


def run(days: int = 30) -> Dict[str, Any]:
    df = _load_features(days)
    anomalies = _detect(df)
    _write_alerts(anomalies)
    _notify(anomalies)
    return {"total": len(anomalies)}


if __name__ == "__main__":
    out = run()
    print(out)
