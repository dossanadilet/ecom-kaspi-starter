import requests
import pandas as pd
from datetime import datetime

def scrape_kaspi(category_id: str, pages: int = 5):
    """
    Загружает данные с Kaspi (примерно, имитация запроса).
    category_id = "smartphones"
    """
    base_url = "https://kaspi.kz/yml/product-view/catalog"
    items = []

    for page in range(1, pages+1):
        url = f"{base_url}?text={category_id}&page={page}"
        print(f"Fetching: {url}")
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print("Ошибка запроса:", e)
            continue

        # ⚠️ Упрощённый пример: парсинг html/json
        # Реально придётся адаптировать под ответ Kaspi
        try:
            data = resp.json()
        except:
            print("Не JSON-ответ")
            continue

        for d in data.get("data", []):
            items.append({
                "product_id": d.get("id"),
                "name": d.get("name"),
                "price": d.get("price"),
                "rating": d.get("rating"),
                "reviews": d.get("reviewsCount"),
                "seller": d.get("sellerName"),
            })

    return pd.DataFrame(items)

if __name__ == "__main__":
    df = scrape_kaspi("смартфон", pages=2)
    if not df.empty:
        out_path = f"data/market_snapshot_{datetime.now():%Y%m%d}.csv"
        df.to_csv(out_path, index=False)
        print("Saved:", out_path)
