from __future__ import annotations

from sqlalchemy import text, create_engine
import os
from .utils import yesterday


def run() -> None:
    url = os.getenv("DATABASE_URL", "postgresql+psycopg://user:pass@postgres:5432/kaspi")
    eng = create_engine(url, future=True)
    d = yesterday()
    with eng.begin() as conn:
        # demo: copy last known features forward one day
        conn.execute(
            text(
                """
                INSERT INTO features_daily (sku, date, competitor_min_price, competitor_avg_price, own_price, sales_units, stock_on_hand)
                SELECT sku, :d, competitor_min_price, competitor_avg_price, own_price, 8.0, 40.0
                FROM features_daily
                WHERE date = (SELECT MAX(date) FROM features_daily)
                ON CONFLICT (sku, date) DO NOTHING
                """
            ),
            {"d": d},
        )


if __name__ == "__main__":
    run()

