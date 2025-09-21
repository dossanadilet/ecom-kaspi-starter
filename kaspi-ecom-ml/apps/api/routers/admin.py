from __future__ import annotations

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
import io
import pandas as pd
from sqlalchemy import text
from datetime import datetime, timezone
from ..core.db import session_scope
from ..core import telegram as tg

from ..deps import get_current_user
from fastapi import BackgroundTasks

router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/trigger/train")
def trigger_train(user: str = Depends(get_current_user)):
    # TODO: integrate Prefect flow trigger or Celery task
    return {"ok": True, "msg": "training scheduled (demo)", "flow_id": "nightly_training"}


@router.post("/import-snapshot")
def import_snapshot(file: UploadFile = File(...), user: str = Depends(get_current_user)):
    """Import market snapshot CSV (MVP):
    - Ensures category id=1 exists (Smartphones)
    - Creates product rows if missing (sku = 'SKU-{product_id}')
    - Inserts offers, price_history, reviews records
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV supported")
    content = file.file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"cannot parse CSV: {e}")
    required = {"product_id", "title"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=400, detail=f"CSV must contain columns: {required}")

    # Coerce common numeric fields safely
    for col in ("list_price", "price_min", "price_default", "rating"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "reviews" in df.columns:
        df["reviews"] = pd.to_numeric(df["reviews"], errors="coerce")

    def _get_float(r, key):
        v = r.get(key)
        try:
            return float(v) if pd.notna(v) else None
        except Exception:
            try:
                s = str(v).replace(" ", "").replace(",", ".")
                return float(s)
            except Exception:
                return None

    def _get_int(r, key):
        v = r.get(key)
        try:
            if pd.notna(v):
                return int(float(v))
            return None
        except Exception:
            try:
                s = str(v).strip()
                return int(s) if s.isdigit() else None
            except Exception:
                return None

    now = datetime.now(timezone.utc)
    rows = df.to_dict(orient="records")
    created_products = 0
    inserted_offers = 0
    inserted_hist = 0
    inserted_reviews = 0
    skipped_bad = 0
    last_error: str | None = None
    with session_scope() as s:
        # ensure category 1
        s.execute(text("INSERT INTO categories (id, name) VALUES (1, 'Smartphones') ON CONFLICT DO NOTHING"))
        # ensure merchant 1
        s.execute(text("INSERT INTO merchants (id, name) VALUES (1, 'Unknown') ON CONFLICT DO NOTHING"))
        # align products.id sequence to MAX(id) to avoid PK conflicts after seeded IDs
        try:
            s.execute(
                text(
                    "SELECT setval(pg_get_serial_sequence('products','id'), COALESCE((SELECT MAX(id) FROM products),0), true)"
                )
            )
        except Exception:
            pass
        for r in rows:
            try:
                pid = str(r.get("product_id") or "").strip()
                title = str(r.get("title") or "").strip()
                if not pid or not title:
                    skipped_bad += 1
                    continue
                sku = f"SKU-{pid}"
                # ensure product
                exists = s.execute(text("SELECT 1 FROM products WHERE product_id=:pid"), {"pid": pid}).fetchone()
                if not exists:
                    s.execute(
                        text(
                            "INSERT INTO products (sku, product_id, title, category_id) VALUES (:sku,:pid,:title,1)"
                        ),
                        {"sku": sku, "pid": pid, "title": title},
                    )
                    created_products += 1
                # offers
                s.execute(
                    text(
                        "INSERT INTO offers (product_id, sku, merchant_id, price_list, price_min, price_default, available, ts)"
                        " VALUES (:pid, :sku, 1, :pl, :pmin, :pdef, true, :ts)"
                    ),
                    {
                        "pid": pid,
                        "sku": sku,
                        "pl": _get_float(r, "list_price"),
                        "pmin": _get_float(r, "price_min"),
                        "pdef": _get_float(r, "price_default"),
                        "ts": now,
                    },
                )
                inserted_offers += 1
                # price history (use list_price if available)
                lp = _get_float(r, "list_price")
                if lp is not None:
                    s.execute(
                        text("INSERT INTO price_history (product_id, sku, price, ts) VALUES (:pid,:sku,:price,:ts)"),
                        {"pid": pid, "sku": sku, "price": lp, "ts": now},
                    )
                    inserted_hist += 1
                # reviews aggregate
                rating = _get_float(r, "rating")
                rcnt = _get_int(r, "reviews")
                if rating is not None or rcnt is not None:
                    s.execute(
                        text("INSERT INTO reviews (product_id, rating, review_count, ts) VALUES (:pid,:rating,:rc,:ts)"),
                        {"pid": pid, "rating": rating, "rc": rcnt, "ts": now},
                    )
                    inserted_reviews += 1
            except Exception as e:
                skipped_bad += 1
                last_error = str(e)
    resp = {
        "ok": True,
        "products_created": created_products,
        "offers": inserted_offers,
        "price_history": inserted_hist,
        "reviews": inserted_reviews,
        "skipped": skipped_bad,
        "last_error": last_error,
    }
    # Telegram notify (best-effort)
    if tg.is_configured():
        msg = (
            f"<b>Импорт снапшота</b>\n"
            f"Создано товаров: <b>{created_products}</b>\n"
            f"Офферов: <b>{inserted_offers}</b>, История цен: <b>{inserted_hist}</b>, Отзывы: <b>{inserted_reviews}</b>\n"
            f"Пропущено: {skipped_bad}"
        )
        tg.send_text(msg)
    return resp


@router.post("/flow/nightly")
def run_nightly_flow(bg: BackgroundTasks, user: str = Depends(get_current_user)):
    """Kick off demo nightly flow (synchronous tasks run in background)."""
    try:
        from dags.flows import nightly_training  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"cannot import flow: {e}")

    def _task():
        nightly_training()
        if tg.is_configured():
            tg.send_text("✅ Ночной сценарий завершён")
    bg.add_task(_task)
    if tg.is_configured():
        tg.send_text("▶️ Запущен ночной сценарий")
    return {"ok": True, "scheduled": "nightly_training"}


@router.post("/flow/inference")
def run_inference_flow(bg: BackgroundTasks, user: str = Depends(get_current_user)):
    """Kick off demo inference flow (refresh price_reco)."""
    try:
        from dags.flows import inference_schedule  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"cannot import flow: {e}")

    def _task():
        inference_schedule()
        if tg.is_configured():
            tg.send_text("✅ Инференс (price_reco) завершён")
    bg.add_task(_task)
    if tg.is_configured():
        tg.send_text("▶️ Запущен инференс (price_reco)")
    return {"ok": True, "scheduled": "inference_schedule"}


@router.post("/tg/test")
def tg_test(user: str = Depends(get_current_user)):
    if not tg.is_configured():
        raise HTTPException(status_code=400, detail="Telegram не сконфигурирован")
    ok = tg.send_text("🔔 Тестовое сообщение из Kaspi E‑commerce")
    return {"ok": ok}


@router.post("/tg/send-recos")
def tg_send_recos(limit: int = 100, user: str = Depends(get_current_user)):
    if not tg.is_configured():
        raise HTTPException(status_code=400, detail="Telegram не сконфигурирован")
    # build CSV from latest recommendations
    from sqlalchemy import text as sqltext
    import pandas as pd

    with session_scope() as s:
        rows = s.execute(
            sqltext(
                """
                SELECT pr.sku, COALESCE(p.title,'') AS title, pr.price AS reco_price, pr.expected_qty, pr.expected_profit, pr.model_ver, pr.ts
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
            ),
            {"lim": limit},
        ).fetchall()
        df = pd.DataFrame([dict(r._mapping) for r in rows])
    if df.empty:
        raise HTTPException(status_code=400, detail="Нет рекомендаций для отправки")
    csv = df.to_csv(index=False).encode("utf-8")
    ok = tg.send_csv("price_recommendations.csv", csv, caption="Рекомендации по цене")
    return {"ok": ok, "rows": len(df)}


@router.post("/run-anomaly")
def run_anomaly(user: str = Depends(get_current_user)):
    try:
        from services.ml.anomaly.run_anomaly import run as run_anomaly_job
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"cannot import anomaly runner: {e}")

    result = run_anomaly_job()
    return {"ok": True, **result}
