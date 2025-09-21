from __future__ import annotations

import os
import time
import subprocess
import threading
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text


DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://user:pass@postgres:5432/kaspi")
AUTO_SEED = os.getenv("AUTO_SEED", "false").lower() in ("1", "true", "yes")


def wait_for_db(timeout: int = 60) -> None:
    start = time.time()
    last_err: Exception | None = None
    while time.time() - start < timeout:
        try:
            eng = create_engine(DB_URL, future=True)
            with eng.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as e:
            last_err = e
            time.sleep(2)
    raise RuntimeError(f"DB not ready: {last_err}")


def run_alembic() -> None:
    # run alembic upgrade head using the config path in image
    subprocess.check_call(["alembic", "-c", "db/alembic.ini", "upgrade", "head"])  # noqa: S603


def maybe_seed() -> None:
    if not AUTO_SEED:
        return
    # if products are empty, load demo seed
    eng = create_engine(DB_URL, future=True)
    with eng.begin() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM products")).scalar() or 0
        if cnt and int(cnt) > 0:
            return
        seed_path = Path("/app/db/sql/seed_demo.sql")
        if not seed_path.exists():
            return
        sql = seed_path.read_text(encoding="utf-8")
        # naive split by ';' for simple seed file
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for st in statements:
            conn.execute(text(st))
        # align sequences for tables with manual IDs in seed
        try:
            conn.execute(
                text(
                    "SELECT setval(pg_get_serial_sequence('products','id'), COALESCE((SELECT MAX(id) FROM products),0), true)"
                )
            )
        except Exception:
            pass


def _send_digest(limit: int = 100) -> None:
    try:
        from .telegram import is_configured, send_text, send_csv
    except Exception:
        return
    if not is_configured():
        return
    eng = create_engine(DB_URL, future=True)
    with eng.begin() as conn:
        rows = conn.execute(text(
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
        ), {"lim": limit}).fetchall()
        df = pd.DataFrame([dict(r._mapping) for r in rows])
    if df.empty:
        return
    topn = df.head(5)
    lines = ["<b>Дайджест рекомендаций (топ‑5 по прибыли)</b>"]
    for _, r in topn.iterrows():
        title = (r['title'] or '')
        if len(title) > 50:
            title = title[:50] + '…'
        lines.append(f"• {r['sku']} — {title} | {float(r['reco_price']):.0f} ₸ | Δ≈{float(r['expected_profit']):.0f}")
    send_text("\n".join(lines))
    csv = df.to_csv(index=False).encode("utf-8")
    send_csv("digest_recommendations.csv", csv, caption="Полный список рекомендаций")


def _schedule_digest() -> None:
    enabled = os.getenv("TG_DIGEST_ENABLED", "false").lower() in ("1", "true", "yes")
    if not enabled:
        return
    hour = int(os.getenv("TG_DIGEST_HOUR", "9"))
    tzname = os.getenv("TZ", "Asia/Almaty")
    tz = ZoneInfo(tzname) if ZoneInfo else None

    def _loop():
        while True:
            now = datetime.now(tz) if tz else datetime.now()
            run = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            if now >= run:
                run = run + timedelta(days=1)
            sleep_s = max(5.0, (run - now).total_seconds())
            time.sleep(sleep_s)
            try:
                _send_digest()
            except Exception:
                pass

    th = threading.Thread(target=_loop, daemon=True)
    th.start()


def main() -> None:
    wait_for_db()
    run_alembic()
    maybe_seed()
    _schedule_digest()


if __name__ == "__main__":
    main()
