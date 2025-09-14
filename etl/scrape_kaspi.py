# etl/scrape_kaspi.py
import asyncio
import json
import re
from pathlib import Path
from typing import Optional, Tuple
import random

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Алматы
CITY_ID = "750000000"

CATEGORY_URL = "https://kaspi.kz/shop/c/smartphones/"

# ---- Селекторы листинга ----
CARD_SELECTORS = [
    "div.item-card",
    "[data-test='item-card']",
    "div[id^='item-card-']",
    "article.product-card",
    "[itemtype='http://schema.org/Product']",
]
TITLE_SELECTORS = [
    "a.item-card__name",
    "a[data-test='item-name']",
    "a.product-card__name",
    "a[itemprop='name']",
    "a[itemprop='url'] span",
]
LINK_SELECTORS = [
    "a[href*='/shop/p/']",
    "a.item-card__name",
    "a[data-test='item-name']",
    "a.product-card__name",
    "a[itemprop='url']",
]

PID_RE = re.compile(r"/p/[^/]*-(\d+)(?:/|$)")

async def first_visible_selector(page, selectors, timeout=3000) -> Optional[str]:
    for sel in selectors:
        try:
            await page.locator(sel).first.wait_for(state="visible", timeout=timeout)
            return sel
        except PWTimeout:
            continue
    return None

async def extract_name_from_card(card) -> Optional[str]:
    for sel in TITLE_SELECTORS:
        try:
            el = card.locator(sel).first
            txt = await el.inner_text(timeout=600)
            if txt:
                txt = txt.strip()
                if txt and "₸" not in txt and "•" not in txt:
                    return txt
            attr = await el.get_attribute("title")
            if attr and attr.strip():
                return attr.strip()
        except Exception:
            continue
    try:
        alt = await card.locator("img").first.get_attribute("alt", timeout=400)
        if alt and alt.strip():
            return alt.strip()
    except Exception:
        pass
    try:
        full = await card.inner_text(timeout=600) or ""
        for ln in [x.strip() for x in full.splitlines()]:
            if not ln or "₸" in ln or len(ln) < 6:
                continue
            if all(ch.isdigit() or ch in "•. " for ch in ln):
                continue
            if any(x in ln.lower() for x in ("в корзину", "сравнить", "в рассрочку", "кредит")):
                continue
            return ln
    except Exception:
        pass
    return None

async def extract_link_from_card(card) -> Optional[str]:
    for sel in LINK_SELECTORS:
        try:
            el = card.locator(sel).first
            href = await el.get_attribute("href", timeout=400)
            if href:
                return href if href.startswith("http") else ("https://kaspi.kz" + href)
        except Exception:
            continue
    return None

def product_id_from_url(url: str) -> Optional[str]:
    m = PID_RE.search(url or "")
    return m.group(1) if m else None

# ---------- Детальная страница ----------
async def parse_ld_json(page):
    """Вернёт (rating, reviews) из JSON-LD Product, если есть."""
    try:
        # подождём немного скрипты
        await page.wait_for_timeout(300)
        scripts = page.locator("script[type='application/ld+json']")
        cnt = await scripts.count()
        for i in range(cnt):
            try:
                raw = await scripts.nth(i).inner_text(timeout=500)
                if not raw:
                    continue
                data = json.loads(raw)
                arr = data if isinstance(data, list) else [data]
                for obj in arr:
                    if isinstance(obj, dict) and obj.get("@type") in ("Product", "MobilePhone", "ProductModel"):
                        agg = obj.get("aggregateRating")
                        if isinstance(agg, dict):
                            r = agg.get("ratingValue")
                            c = agg.get("reviewCount")
                            rating = float(str(r).replace(",", ".")) if r is not None else None
                            reviews = int(str(c).replace(" ", "")) if c is not None else None
                            if (rating is not None and 0 < rating <= 5) or (reviews is not None and reviews >= 0):
                                return rating, reviews
            except Exception:
                continue
    except Exception:
        pass
    return None, None

async def parse_rating_reviews_dom(page):
    """Фолбэк: достаём рейтинг/отзывы из DOM (aria-label, '/5', класс 'rating _45', текст ‘отзывов’)."""
    import re
    rating, reviews = None, None

    # лёгкий скролл — пусть звёздочки дорисуются
    try:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight/4);")
        await page.wait_for_timeout(400)
    except Exception:
        pass

    # 1) aria-label: "4,8 из 5"
    try:
        label = await page.locator("[aria-label*='из 5'], [aria-label*='/ 5']").first.get_attribute("aria-label", timeout=800)
        if label:
            m = re.search(r"(\d+(?:[.,]\d)?)\s*(?:из|/)\s*5", label)
            if m:
                v = float(m.group(1).replace(",", "."))
                if 0 < v <= 5:
                    rating = v
    except Exception:
        pass

    # 2) явный '/5'
    if rating is None:
        try:
            t = await page.locator("span:has-text('/5'), div:has-text('/5')").first.inner_text(timeout=600)
            m = re.search(r"(\d+(?:[.,]\d)?)\s*/\s*5", t or "")
            if m:
                v = float(m.group(1).replace(",", "."))
                if 0 < v <= 5:
                    rating = v
        except Exception:
            pass

    # 3) класс rating _45 → 4.5
    if rating is None:
        try:
            cls = await page.locator("span.rating").first.get_attribute("class", timeout=600)
            if cls:
                m = re.search(r"_(\d{2})(?!\d)", cls)
                if m:
                    v = int(m.group(1)) / 10.0
                    if 0 < v <= 5:
                        rating = v
        except Exception:
            pass

    # 4) отзывы по тексту
    try:
        body = await page.inner_text("body", timeout=800)
        m = re.search(r"(\d[\d\s]{0,6})\s*(?:отзыв|отзыва|отзывов)\b", body or "", flags=re.IGNORECASE)
        if m:
            digits = "".join(ch for ch in m.group(1) if ch.isdigit())
            if digits:
                reviews = int(digits)
    except Exception:
        pass

    if rating is not None and (rating <= 0 or rating > 5):
        rating = None
    return rating, reviews

async def fetch_price_via_offers_api(context_request, product_id: str, referer_url: str, city_id: int = 750000000):
    """
    Возвращаем dict: price_min, price_default, offers_count, best_merchant.
    """
    url = "https://kaspi.kz/yml/offer-view/offers"
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/122.0.0.0 Safari/537.36")

    def parse_offers_json(js):
        if not js:
            return []
        offers = js.get("offers") or js.get("data") or (js if isinstance(js, list) else [])
        if not isinstance(offers, list):
            return []
        parsed = []
        for off in offers:
            p = off.get("price")
            if p is None and isinstance(off.get("merchantProduct"), dict):
                p = off["merchantProduct"].get("price")
            mname = (off.get("merchantName") or
                     (off.get("merchantProduct") or {}).get("shopName") or
                     off.get("shopName") or
                     off.get("merchant") or None)
            if p is None:
                continue
            try:
                price_int = int(float(str(p).replace(" ", "").replace("\u00a0", "")))
                parsed.append({"price": price_int, "merchant": mname})
            except Exception:
                continue
        return parsed

    # JSON body
    try:
        resp = await context_request.post(
            url,
            data=json.dumps({"productId": product_id, "cityId": city_id}),
            headers={
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json;charset=UTF-8",
                "Origin": "https://kaspi.kz",
                "Referer": referer_url,
                "User-Agent": ua,
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=15000,
        )
        if resp.ok:
            js = await resp.json()
            offers = parse_offers_json(js)
            if offers:
                best = min(offers, key=lambda x: x["price"])
                return {
                    "price_min": best["price"],
                    "price_default": offers[0]["price"],
                    "offers_count": len(offers),
                    "best_merchant": best.get("merchant"),
                }
        else:
            body = (await resp.text())[:200]
            print(f"[offers:json] HTTP {resp.status} for {product_id} :: {body}")
    except Exception as e:
        print(f"[offers:json] ex {product_id}: {e}")

    # x-www-form-urlencoded fallback
    try:
        resp = await context_request.post(
            url,
            data={"productId": product_id, "cityId": city_id},
            headers={
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://kaspi.kz",
                "Referer": referer_url,
                "User-Agent": ua,
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=15000,
        )
        if resp.ok:
            js = await resp.json()
            offers = parse_offers_json(js)
            if offers:
                best = min(offers, key=lambda x: x["price"])
                return {
                    "price_min": best["price"],
                    "price_default": offers[0]["price"],
                    "offers_count": len(offers),
                    "best_merchant": best.get("merchant"),
                }
        else:
            body = (await resp.text())[:200]
            print(f"[offers:form] HTTP {resp.status} for {product_id} :: {body}")
    except Exception as e:
        print(f"[offers:form] ex {product_id}: {e}")

    return {"price_min": None, "price_default": None, "offers_count": 0, "best_merchant": None}

async def fetch_detail_fields(detail_page, context_request, url: str):
    """
    Собираем:
      - price_min (из offers API → DOM фолбэк)
      - price_default (из offers API → meta[itemprop=price] → DOM first)
      - rating, reviews (JSON-LD → DOM фолбэк)
      - offers_count, best_merchant
    """
    try:
        await detail_page.goto(url, wait_until="domcontentloaded", timeout=60000)
        try:
            await detail_page.wait_for_load_state("networkidle", timeout=8000)
        except PWTimeout:
            pass

        # 1) сначала пробуем JSON-LD (самый чистый источник рейтинга/отзывов)
        rating, reviews = await parse_ld_json(detail_page)

        # 2) цены через /yml/offer-view/offers
        pid = product_id_from_url(url)
        price_min = price_default = None
        offers_count = 0
        best_merchant = None
        if pid:
            rec = await fetch_price_via_offers_api(context_request, pid, url)
            price_min = rec.get("price_min")
            price_default = rec.get("price_default")
            offers_count = rec.get("offers_count", 0)
            best_merchant = rec.get("best_merchant")

        # 3) если API не дал цен — дом-фолбэк по «Предложениям»
        if price_min is None and price_default is None:
            try:
                pmin, pfirst, offers_cnt = await scrape_offers_dom(detail_page)
                if pmin is not None:
                    price_min = pmin
                if price_default is None and pfirst is not None:
                    price_default = pfirst
                if offers_count == 0 and offers_cnt:
                    offers_count = offers_cnt
            except Exception:
                pass

        # 4) если всё ещё нет дефолтной — meta[itemprop=price]
        if price_default is None:
            try:
                meta_price = await detail_page.locator("meta[itemprop='price']").first.get_attribute("content", timeout=600)
                if meta_price and str(meta_price).replace(".", "", 1).isdigit():
                    price_default = int(float(meta_price))
            except Exception:
                pass

        # 5) рейтинг/отзывы DOM-фолбэк, НО не затираем уже найденные
        if rating is None or reviews is None:
            r2, c2 = await parse_rating_reviews_dom(detail_page)
            if rating is None and r2 is not None:
                rating = r2
            if reviews is None and c2 is not None:
                reviews = c2

        return {
            "price_min": price_min,
            "price_default": price_default,
            "rating": rating,
            "reviews": reviews,
            "offers_count": offers_count,
            "best_merchant": best_merchant,
        }
    except Exception as e:
        print(f"[detail] exception for {url}: {e}")
        return {
            "price_min": None,
            "price_default": None,
            "rating": None,
            "reviews": None,
            "offers_count": 0,
            "best_merchant": None,
        }

async def scrape_offers_dom(page):
    """
    Возвращает (price_min, price_first, offers_count) по ценам продажи (исключая рассрочку '/мес').
    """
    import re
    try:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.6);")
        await page.wait_for_timeout(700)
    except Exception:
        pass

    # Блоки с ценами в зоне «Предложения»
    rows = page.locator("div:has(span:has-text('₸')):below(:text('Предложения'))")
    if await rows.count() == 0:
        rows = page.locator("div:has(span:has-text('₸')), li:has(span:has-text('₸'))")

    prices, first_price = [], None
    offers_count = 0
    n = await rows.count()

    for i in range(n):
        row = rows.nth(i)
        try:
            txt = await row.inner_text(timeout=1200)
            if not txt:
                continue
            low = " ".join(txt.split()).lower()

            # собираем все значения с ₸
            raw = re.findall(r"(\d[\d\s\u00A0]{2,})\s*₸", txt)
            vals = []
            for r in raw:
                try:
                    v = int("".join(ch for ch in r if ch.isdigit()))
                    vals.append(v)
                except Exception:
                    pass
            if not vals:
                continue

            # если в тексте есть индикаторы рассрочки — отсекаем маленькие как «/мес»
            if any(k in low for k in ("/мес", "мес", "месяц", "в рассрочку")):
                vals = [v for v in vals if v >= 200_000]

            # выбираем «текущую»/«продаваемую» как максимальную из блока (обычно рядом ещё зачёркнутая)
            if not vals:
                continue
            cand = max(vals)

            # грубая валидация для смартфонов
            if 20_000 <= cand <= 2_000_000:
                offers_count += 1
                prices.append(cand)
                if first_price is None:
                    first_price = cand
        except Exception:
            continue

    return (min(prices) if prices else None), first_price, offers_count

def _to_int(val: str) -> Optional[int]:
    if not val:
        return None
    digits = "".join(ch for ch in str(val) if ch.isdigit())
    return int(digits) if digits else None

async def extract_list_price_from_card(card) -> Optional[int]:
    """
    Возвращает витринную цену с карточки на листинге (то, что видит пользователь),
    избегая сумм типа '₸/мес' и прочих ежемесячных платежей.
    """
    # Частые варианты
    selectors = [
        "[data-test='item-price']",
        ".item-card__prices-price",
        "[data-test='item-card'] .price",
        "span:has-text('₸')",
    ]
    txt = ""
    for sel in selectors:
        try:
            el = card.locator(sel).first
            if await el.count() == 0:
                continue
            chunk = await el.inner_text(timeout=600)
            if chunk:
                txt += " " + chunk
        except Exception:
            continue
    
    if not txt:
        return None

    # Убираем «в рассрочку», «/ мес», «×» и т.п.
    bad_markers = ["в рассрочку", "в рассрочку", "/ мес", "/мес", "×", "в кредит"]
    low = txt.lower()
    for bad in bad_markers:
        if bad in low:
            # попробуем вырезать рядом стоящие куски с "/мес" и т.п.
            low = low.replace(bad, " ")

    # Берём все числа перед символом ₸ и выбираем самое крупное
    import re
    candidates = re.findall(r"(\d[\d\s]{2,})\s*₸", low)
    values = []
    for c in candidates:
        v = _to_int(c)
        if v:
            values.append(v)

    # Если ничего не нашли — None
    if not values:
        return None

    # На листинге обычно показывают полную цену (она заметно больше ежемесячной),
    # поэтому берём максимальную из найденных.
    return max(values)

async def scrape_category(query="смартфон", pages=2, headless=True) -> pd.DataFrame:
    """С листинга берём name+url, с карточки — price_min/price_default/rating/reviews."""
    items = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context(
            locale="ru-RU",
            timezone_id="Asia/Almaty",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 900},
        )
        list_page = await context.new_page()
        detail_page = await context.new_page()

        for i in range(1, pages + 1):
            url = f"{CATEGORY_URL}?text={query}&page={i}"
            print("Открываю листинг:", url)
            await list_page.goto(url, wait_until="domcontentloaded", timeout=60000)
            try:
                await list_page.wait_for_load_state("networkidle", timeout=8000)
            except PWTimeout:
                pass

            card_selector = await first_visible_selector(list_page, CARD_SELECTORS, timeout=7000)
            if not card_selector:
                print(f"[WARN] карточки не найдены на странице {i}")
                continue

            cards = list_page.locator(card_selector)
            count = await cards.count()
            print(f"Найдено карточек: {count}")

            for idx in range(count):
                c = cards.nth(idx)
                name = await extract_name_from_card(c)
                link = await extract_link_from_card(c)
                if not name:
                    continue
                
                # --- вот здесь пауза ---
                await asyncio.sleep(0.4)
                
                detail = {"price_min": None, "price_default": None, "rating": None, "reviews": None,
                          "offers_count": 0, "best_merchant": None}
                if link:
                    detail = await fetch_detail_fields(detail_page, context.request, link)

                print(
                    f"[OK] {name} | min={detail['price_min']} | default={detail['price_default']} | "
                    f"rating={detail['rating']} | reviews={detail['reviews']} | offers={detail['offers_count']}"
                )

                items.append({
                    "page": i,
                    "name": name,
                    "price_min": detail["price_min"],
                    "price_default": detail["price_default"],
                    "rating": detail["rating"],
                    "reviews": detail["reviews"],
                    "offers_count": detail["offers_count"],
                    "best_merchant": detail["best_merchant"],
                    "url": link,
                })

        await browser.close()

    return pd.DataFrame(items)

if __name__ == "__main__":
    df = asyncio.run(scrape_category("смартфон", pages=2, headless=True))
    if df is not None and not df.empty:
        out = DATA_DIR / "market_snapshot_test.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"Сохранил {len(df)} товаров → {out}")
    else:
        print("Нет данных")
