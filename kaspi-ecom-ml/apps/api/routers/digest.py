from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text as sqltext
import pandas as pd

from ..deps import get_current_user
from ..core.db import session_scope
from ..core import telegram as tg


router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/tg/digest")
def tg_digest(limit: int = 100, user: str = Depends(get_current_user)):
    if not tg.is_configured():
        raise HTTPException(status_code=400, detail="Telegram не сконфигурирован")

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
                ORDER BY pr.expected_profit DESC NULLS LAST
                LIMIT :lim
                """
            ),
            {"lim": limit},
        ).fetchall()
        df = pd.DataFrame([dict(r._mapping) for r in rows])
    if df.empty:
        raise HTTPException(status_code=400, detail="Нет рекомендаций для дайджеста")

    # Text top-5
    topn = df.head(5)
    lines = ["<b>Дайджест рекомендаций (топ‑5 по прибыли)</b>"]
    for _, r in topn.iterrows():
        title = (r['title'] or '')
        if len(title) > 50:
            title = title[:50] + '…'
        lines.append(f"• {r['sku']} — {title} | {float(r['reco_price']):.0f} ₸ | Δ≈{float(r['expected_profit']):.0f}")
    tg.send_text("\n".join(lines))

    # CSV full
    csv = df.to_csv(index=False).encode("utf-8")
    ok = tg.send_csv("digest_recommendations.csv", csv, caption="Полный список рекомендаций")
    return {"ok": ok, "rows": len(df)}

