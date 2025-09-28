# etl/scrape_kaspi.py
import argparse
import csv
import json
import logging
import random
import re
import sys
import time
import os
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Set, Tuple, Iterable, Callable, Any
from urllib.parse import quote, urlparse, parse_qs, urlencode, urlunparse

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ---------------------- Logging ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kaspi-scraper")

# ---------------------- Models ----------------------
@dataclass
class Product:
    product_id: Optional[str]
    title: str
    url: Optional[str]
    list_price: Optional[float]
    price_min: Optional[float] = None
    price_default: Optional[float] = None
    rating: Optional[float] = None
    reviews: Optional[int] = None
    offers_count: Optional[int] = None
    best_merchant: Optional[str] = None
    errors: Optional[str] = None

@dataclass
class CapturedEndpoint:
    method: str
    url: str
    req_headers: Dict[str, str]
    req_body: Optional[str]        # raw body (str)
    resp_json: Optional[dict]      # parsed JSON

# ---------------------- Selectors ----------------------
CARD_SELECTORS = [
    'article[data-product-id]',
    '[data-product-id]',
    '.item-card',
    'div[itemtype*="Product"]',
    'a[href*="/shop/p/"]'
]

LOAD_MORE_SELECTORS = [
    'button:has-text("Показать ещё")',
    '[data-test="load-more"]',
    'button:has-text("Показать еще")',
]
# Augment with stable attribute-based selectors to avoid language-dependent text matches
ALL_LOAD_MORE_SELECTORS = (
    LOAD_MORE_SELECTORS
    + [
        '[data-test="load-more"]',
        'button[data-test="load-more"]',
        'div[data-test="load-more"] button',
    ]
)

# ---------------------- Utils ----------------------
def _ctx_opts() -> dict:
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0")
    return {
        "locale": "ru-RU",
        "timezone_id": "Asia/Almaty",
        "user_agent": ua,
        "viewport": {"width": 1366, "height": 800},
        "extra_http_headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
        }
    }

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _dismiss(page):
    # cookie/consent + модалки
    sels = [
        'button:has-text("Понятно")', 'button:has-text("Согласен")',
        'button:has-text("Согласиться")', 'button:has-text("Accept")',
        '[class*="cookie"] button', '[id*="cookie"] button', '[aria-label="Закрыть"]',
        'button:has-text("Алматы")', 'button:has-text("Да")', 'button:has-text("Ок")',
        '[data-test="region-confirm"], [data-testid="region-confirm"]',
    ]
    for s in sels:
        try:
            loc = page.locator(s).first
            if loc.count():
                loc.click(timeout=800)
                time.sleep(0.2)
        except Exception:
            pass

def _regex_price_to_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    m = re.search(r'(?:(?:\d{1,3}(?:[ \u00A0]\d{3})+)|\d+)(?:[.,]\d+)?', str(text))
    if not m:
        return None
    num = m.group(0).replace('\u00A0', ' ').replace(' ', '').replace(',', '.')
    try:
        return float(num)
    except Exception:
        return None

def _wait_any_cards(page, timeout_ms=30000, tag="") -> Tuple[str, int]:
    end = time.time() + timeout_ms/1000
    last = None
    while time.time() < end:
        _dismiss(page)
        for sel in CARD_SELECTORS:
            try:
                page.wait_for_selector(sel, state="attached", timeout=1200)
                cnt = page.locator(sel).count()
                if cnt > 0:
                    return sel, cnt
            except Exception as e:
                last = e
        try:
            page.evaluate("window.scrollBy(0, Math.max(300, document.body.scrollHeight * 0.5))")
        except Exception:
            pass
        time.sleep(0.7)
    raise last or PWTimeout(f"Нет карточек ({tag})")

def _extract_products_on_page(page) -> List[Product]:
    base = None
    for sel in CARD_SELECTORS:
        if page.locator(sel).count() > 0:
            base = sel
            break
    if not base:
        return []
    cards = page.locator(base)
    n = cards.count()
    out: List[Product] = []
    for i in range(n):
        el = cards.nth(i)
        pid, title, href, lp = None, None, None, None
        try:
            pid = el.get_attribute("data-product-id")
        except Exception:
            pass
        try:
            cand = el.locator('a[title], a[data-product-name], .item-card__name, [itemprop="name"]')
            if cand.count():
                title = cand.first.inner_text(timeout=2500).strip()
        except Exception:
            pass
        try:
            link = el.locator('a[href*="/shop/p/"], a[itemprop="url"]')
            if link.count():
                href = link.first.get_attribute("href")
                if href and href.startswith("/"):
                    href = "https://kaspi.kz" + href
        except Exception:
            pass
        try:
            meta_price = el.locator('[itemprop="price"]')
            if meta_price.count():
                content_val = meta_price.first.get_attribute("content")
                if content_val:
                    lp = _regex_price_to_float(content_val)
            if lp is None:
                price_loc = el.locator('[data-test="price"], .item-card__prices, .item-card__price')
                if price_loc.count():
                    lp = _regex_price_to_float(price_loc.first.inner_text(timeout=2500))
        except Exception:
            pass
        if title:
            out.append(Product(product_id=pid, title=title, url=href, list_price=lp))
    return out

def _collect_visible_ids(page) -> Set[str]:
    ids = set()
    try:
        loc = page.locator('article[data-product-id], [data-product-id]')
        for i in range(loc.count()):
            val = loc.nth(i).get_attribute("data-product-id")
            if val:
                ids.add(val)
    except Exception:
        pass
    return ids

# ---------------------- XHR capture & pagination inference ----------------------
def _is_json_like(headers: Dict[str, str]) -> bool:
    ct = (headers or {}).get("content-type", "") or ""
    return "application/json" in ct or ct.endswith("+json")

SAVE_DUMPS = bool(int(os.getenv("KASPI_SAVE_DUMPS", "0")))

def _save_dump(prefix: str, payload: dict, ext: str = "json") -> str:
    if not SAVE_DUMPS:
        return ""
    _ensure_dir("logs")
    ts = int(time.time() * 1000)
    path = os.path.join("logs", f"{prefix}_{ts}.{ext}")
    try:
        with open(path, "w", encoding="utf-8") as f:
            if ext == "json":
                json.dump(payload, f, ensure_ascii=False, indent=2)
            else:
                f.write(str(payload))
        logger.info("Лог сохранён: %s", path)
    except Exception as e:
        logger.debug("save dump failed: %s", e)
    return path

def _infer_array_and_next(data: dict) -> Tuple[Optional[List[dict]], Dict[str, Optional[str]]]:
    """Возвращает (items, hints), где hints может содержать cursor/has_next/meta."""
    if not isinstance(data, (dict, list)):
        return None, {}
    def _scan(obj):
        if isinstance(obj, list):
            for x in obj:
                arr, hints = _scan(x)
                if arr:
                    return arr, hints
            return None, {}
        if isinstance(obj, dict):
            for key in ("cards", "items", "products", "results", "list", "edges", "nodes"):
                if isinstance(obj.get(key), list) and obj.get(key):
                    hints: Dict[str, Optional[str]] = {}
                    for k in ("next", "nextPage", "next_page", "nextToken", "next_token", "cursor", "after"):
                        if obj.get(k):
                            hints["cursor"] = str(obj.get(k))
                    for mk in ("pageInfo", "pagination", "meta", "paging"):
                        if isinstance(obj.get(mk), dict):
                            pi = obj[mk]
                            # Встречается множество ложных hasNextPage=false в нерелевантных блоках (например, фильтрах),
                            # поэтому здесь НЕ используем это как стоп-сигнал. Считаем наличие следующей страницы
                            # отдельно по limit/total в XHR-потоке.
                            for p in ("next", "cursor", "endCursor", "after"):
                                if pi.get(p):
                                    hints["cursor"] = str(pi[p])
                            for p in ("page","pageNumber","p","offset","start","from","limit","size","rows","perPage"):
                                if p in pi:
                                    hints.setdefault("meta", {})
                                    hints["meta"][p] = pi[p]
                    return obj[key], hints
            for v in obj.values():
                arr, hints = _scan(v)
                if arr:
                    return arr, hints
        return None, {}
    return _scan(data)

def _bump_query_params(url: str, step: int = 12) -> Optional[str]:
    pr = urlparse(url)
    qs = parse_qs(pr.query)
    bumped = False
    for key in ("page","p","pageNumber","page_num"):
        if key in qs:
            try:
                cur = int(qs[key][0]); qs[key] = [str(cur+1)]; bumped=True
            except: pass
    for key in ("offset","start","from"):
        if key in qs:
            try:
                cur = int(qs[key][0]); qs[key] = [str(cur+step)]; bumped=True
            except: pass
    if not bumped:
        return None
    new_q = urlencode({k: v[0] for k,v in qs.items()}, doseq=False)
    return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))

def _strip_zone_in_q(url: str) -> str:
    """Удаляет :availableInZones:... из q=, чтобы не резало пагинацию."""
    pr = urlparse(url)
    qs = parse_qs(pr.query)
    if "q" in qs and qs["q"]:
        q = qs["q"][0]
        q2 = re.sub(r":availableInZones:[A-Za-z0-9_\-]+", "", q)
        if q2 != q:
            qs["q"] = [q2]
            new_q = urlencode({k: v[0] for k, v in qs.items()})
            url = urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))
    return url

def _extract_request_id_from_data(data: Any) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    meta = data.get("data")
    if isinstance(meta, dict):
        ext = meta.get("externalSearchQueryInfo")
        if isinstance(ext, dict) and ext.get("queryID"):
            return str(ext.get("queryID"))
    return None

def _bump_graphql_body(body_str: str, hints: Dict[str, Optional[str]], step: int = 12) -> Optional[str]:
    """Пробуем инкрементировать variables.page/offset/start или подставить cursor/after."""
    try:
        body = json.loads(body_str) if body_str else {}
    except Exception:
        return None
    def _mut(op):
        got = False
        vars = op.get("variables") if isinstance(op, dict) else None
        if isinstance(vars, dict):
            # Инкремент страниц
            for k in ("page", "pageNumber", "p"):
                if isinstance(vars.get(k), int):
                    vars[k] = int(vars[k]) + 1
                    got = True
            for k in ("offset","start","from"):
                if isinstance(vars.get(k), int):
                    vars[k] = int(vars[k]) + step
                    got = True
            if hints.get("cursor"):
                for k in ("cursor","after","endCursor"):
                    if k in vars:
                        vars[k] = hints["cursor"]
                        got = True
        return got
    changed = False
    if isinstance(body, list):
        for op in body:
            if _mut(op): changed = True
    elif isinstance(body, dict):
        if _mut(body): changed = True
    if not changed and isinstance(body, dict) and "variables" in body and isinstance(body["variables"], dict) and hints.get("cursor"):
        body["variables"]["after"] = hints["cursor"]; changed = True
    return json.dumps(body, ensure_ascii=False) if changed else None

# ---------------------- Detail meta ----------------------
def _parse_product_meta(page) -> Dict[str, Optional[float]]:
    """Возвращает метаданные товара с карточки:
    { rating, reviews, price_min, price_default, offers_count }
    Использует JSON-LD (AggregateOffer/Offer) + запасные HTML-селекторы.
    """
    meta: Dict[str, Optional[float]] = {
        "rating": None,
        "reviews": None,
        "price_min": None,
        "price_default": None,
        "offers_count": None,
    }

    # 1) JSON-LD (наиболее надёжный источник для lowPrice/offerCount)
    try:
        scripts = page.locator('script[type="application/ld+json"]').all()
        for sc in scripts:
            try:
                data = json.loads(sc.inner_text(timeout=2000))
            except Exception:
                continue
            if isinstance(data, list):
                for obj in data:
                    if isinstance(obj, dict) and obj.get("@type") == "Product":
                        prod = obj
                        break
                else:
                    prod = None
            elif isinstance(data, dict) and data.get("@type") == "Product":
                prod = data
            else:
                prod = None
            if not prod:
                continue

            # rating / reviews
            agg = prod.get("aggregateRating") or {}
            r = agg.get("ratingValue")
            c = agg.get("reviewCount") or agg.get("ratingCount")
            if r is not None:
                try:
                    meta["rating"] = float(str(r).replace(",", "."))
                except Exception:
                    pass
            if c is not None:
                try:
                    meta["reviews"] = int(str(c).replace(" ", ""))
                except Exception:
                    pass

            # offers
            offers = prod.get("offers")
            if isinstance(offers, dict):
                # AggregateOffer
                low = offers.get("lowPrice") or offers.get("price")
                if low is not None:
                    try:
                        meta["price_min"] = float(str(low).replace(",", ".").replace(" ", ""))
                    except Exception:
                        pass
                if offers.get("offerCount") is not None:
                    try:
                        meta["offers_count"] = int(offers["offerCount"])
                    except Exception:
                        pass
            elif isinstance(offers, list) and offers:
                try:
                    meta["price_default"] = _regex_price_to_float(offers[0].get("price")) or meta["price_default"]
                except Exception:
                    pass

            # Иногда JSON-LD хранит и единственную "price"
            if meta["price_default"] is None:
                p = prod.get("price")
                if p is not None:
                    meta["price_default"] = _regex_price_to_float(str(p))

            # Прерываем после первого подходящего Product
            break
    except Exception:
        pass

    # 2) Запасные селекторы для отзывов/рейтинга
    try:
        # Прямые itemprop-меты
        try:
            rv_meta = page.locator('meta[itemprop="ratingValue"]').first
            if rv_meta.count():
                v = rv_meta.get_attribute('content')
                if v:
                    meta["rating"] = float(str(v).replace(',', '.'))
            rc_meta = page.locator('meta[itemprop="reviewCount"], meta[itemprop="ratingCount"]').first
            if rc_meta.count():
                v = rc_meta.get_attribute('content')
                if v:
                    meta["reviews"] = int(re.sub(r"\D+", "", v))
        except Exception:
            pass

        # Отзывы
        review_selectors = [
            '[data-testid*="review"], [class*="review"]',
            'span:has-text("отзыв")',
            'span:has-text("отзыва")',
            'span:has-text("отзывов")',
            'span[itemprop="reviewCount"], [itemprop="reviewCount"]',
        ]
        if not meta["reviews"]:
            for selector in review_selectors:
                try:
                    elements = page.locator(selector).all()
                    for element in elements:
                        text = (element.inner_text(timeout=800) or "").strip()
                        m = re.search(r"(\d+)[^\d]*отзыв", text, flags=re.I)
                        if not m:
                            m = re.search(r"\((\d+)\)", text)
                        if m:
                            meta["reviews"] = int(m.group(1))
                            break
                except Exception:
                    continue
                if meta["reviews"]:
                    break

        # Рейтинг
        rating_selectors = [
            '[data-rating]',
            '[aria-label*="из 5"]',
            'span[title*="из 5"]',
            '[class*="rating"]',
            'span[itemprop="ratingValue"], [itemprop="ratingValue"]',
        ]
        if not meta["rating"]:
            for selector in rating_selectors:
                try:
                    elements = page.locator(selector).all()
                    for element in elements:
                        val = element.get_attribute('data-rating')
                        if val:
                            meta["rating"] = float(val)
                            break
                        title_attr = element.get_attribute('title') or element.get_attribute('aria-label')
                        if title_attr and 'из 5' in title_attr:
                            m = re.search(r"(\d+(?:[\.,]\d+)?)\s*из\s*5", title_attr)
                            if m:
                                meta["rating"] = float(m.group(1).replace(',', '.'))
                                break
                        text = (element.inner_text(timeout=800) or "").strip()
                        if 'из 5' in text:
                            m = re.search(r"(\d+(?:[\.,]\d+)?)\s*из\s*5", text)
                            if m:
                                meta["rating"] = float(m.group(1).replace(',', '.'))
                                break
                except Exception:
                    continue
                if meta["rating"]:
                    break
    except Exception:
        pass

    # 3) Поиск минимальной цены по тексту "от <цена>"
    try:
        if meta.get("price_min") is None:
            # raw-string для корректной экранизации в Python-строке
            texts = page.locator(r'text=/^\s*от\s+.+/i').all() if hasattr(page.locator(r'text=/^\s*от\s+.+/i'), 'all') else []
            for el in texts:
                try:
                    t = (el.inner_text(timeout=800) or '').strip()
                except Exception:
                    continue
                if t.lower().startswith('от'):
                    val = _regex_price_to_float(t)
                    if val:
                        meta["price_min"] = val
                        break
    except Exception:
        pass

    return meta

def _enrich_detail_min_price_and_meta(context, items: List[Product], limit:int=24, delay:float=0.6) -> List[Product]:
    if not items:
        return items
    page = context.new_page()
    for it in items[:limit]:
        if not it.url:
            continue
        try:
            page.goto(it.url, wait_until="domcontentloaded")
            _dismiss(page)
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass
            meta = _parse_product_meta(page)
            # Рейтинг/отзывы
            it.rating = it.rating or meta.get("rating")
            it.reviews = it.reviews or (meta.get("reviews") if isinstance(meta.get("reviews"), (int, float)) else None)

            # Цена по источникам приоритета: JSON-LD lowPrice -> [itemprop=price] -> видимая цена
            price_min = meta.get("price_min")
            price_default = meta.get("price_default")

            if it.list_price is None:
                # список/отображаемая
                try:
                    meta_price = page.locator('[itemprop="price"]').first
                    content_val = meta_price.get_attribute("content") if meta_price.count() else None
                    val = _regex_price_to_float(content_val) if content_val else None
                    if val is None:
                        ptext = page.locator('[data-test="price"], .price__value, [class*="price"]').first.inner_text(timeout=2500)
                        val = _regex_price_to_float(ptext)
                    it.list_price = val
                except Exception:
                    pass

            # min/default корректируем
            if price_min is not None:
                it.price_min = price_min
            if price_default is not None:
                it.price_default = price_default
            # Фолбэк: если ничего не нашли — используем list_price
            if it.list_price is not None:
                it.price_default = it.price_default or it.list_price
                it.price_min = it.price_min or it.list_price

        except Exception:
            it.errors = (it.errors + "; " if it.errors else "") + "detail_visit_fail"
    page.close()
    return items

# ---------------------- UI pagination fallback by ?page=N ----------------------
def _paginate_by_page_param(page, base_url: str, start_page: int, pages: int, delay: float) -> List[Product]:
    """Простая HTML пагинация по параметру ?page=N для категорий.
    Возвращает собранные с дополнительных страниц карточки.
    """
    added: List[Product] = []
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    pr = urlparse(base_url)
    base_qs = parse_qs(pr.query)

    for pg in range(start_page, max(start_page, pages) + 1):
        qs = dict((k, v[0] if isinstance(v, list) else v) for k, v in base_qs.items())
        qs["page"] = str(pg)
        new_q = urlencode(qs)
        url = urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))
        try:
            if not _open_listing_try(page, url):
                break
            b = _extract_products_on_page(page)
            if not b:
                break
            added.extend(b)
            time.sleep(delay)
        except Exception:
            break
    return added

# ---------------------- helpers: city/zone param ----------------------
def _add_query_param(url: str, key: str, value: str) -> str:
    try:
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        pr = urlparse(url)
        qs = parse_qs(pr.query)
        qs[key] = [value]
        new_q = urlencode({k: v[0] if isinstance(v, list) and v else v for k, v in qs.items()})
        return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))
    except Exception:
        sep = '&' if ('?' in url) else '?'
        return f"{url}{sep}{key}={value}"

def _set_query_params(url: str, kv: Dict[str, str]) -> str:
    """Устанавливает/заменяет несколько query-параметров в URL."""
    try:
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        pr = urlparse(url)
        qs = parse_qs(pr.query)
        for k, v in kv.items():
            qs[k] = [v]
        new_q = urlencode({k: v[0] if isinstance(v, list) and v else v for k, v in qs.items()})
        return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))
    except Exception:
        # Фолбэк: по одному ключу, иначе вернём исходный url
        try:
            k, v = next(iter(kv.items()))
            return _add_query_param(url, k, v)
        except Exception:
            return url

def _detect_c_param(page) -> Optional[str]:
    try:
        from urllib.parse import urlparse, parse_qs
        # 1) Пытаемся взять из текущего URL
        try:
            cur = page.url
            pr = urlparse(cur)
            qs = parse_qs(pr.query)
            cvals = qs.get('c')
            if cvals:
                return cvals[0]
        except Exception:
            pass
        # 2) Иначе ищем любые ссылки с ?c=
        loc = page.locator("a[href*='?c=']").first
        if loc.count():
            href = loc.get_attribute('href') or ''
            if href:
                pr = urlparse(href)
                qs = parse_qs(pr.query)
                cvals = qs.get('c')
                if cvals:
                    return cvals[0]
    except Exception:
        return None
    return None

# ---------------------- nav[aria-label="Следующая"] pagination ----------------------
def _paginate_by_nav_next(page, pages: int, delay: float) -> List[Product]:
    """Переход по кнопке/ссылке "Следующая" внизу листинга.
    Собирает карточки с каждой следующей страницы до pages.
    """
    added: List[Product] = []
    current = 1
    while current < max(1, pages):
        btn = _find_pagination_element(page, "Следующая", allow_disabled=False, allow_active=True)
        if not btn:
            break
        try:
            classes = (btn.get_attribute("class") or "")
        except Exception:
            classes = ""
        if "_disabled" in classes:
            break
        prev_ids = _collect_visible_ids(page)
        if not _safe_click(btn):
            break
        try:
            page.wait_for_load_state("domcontentloaded")
        except Exception:
            pass
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        # ждём смену набора карточек
        changed = _wait_items_changed(page, prev_ids, timeout_ms=12000)
        if not changed:
            break
        b = _extract_products_on_page(page)
        if not b:
            break
        for it in b:
            added.append(it)
        current += 1
        time.sleep(delay)
    return added

# ---------------------- nav numeric pages pagination ----------------------
def _paginate_by_nav_numbers(page, pages: int, delay: float) -> List[Product]:
    """Переход по номерам страниц в навигации (2..N)."""
    added: List[Product] = []
    numbers: List[int] = []
    selectors = ['.pagination__el', 'nav a', 'nav[aria-label="Pagination"] a']
    for sel in selectors:
        try:
            loc = page.locator(sel)
            count = loc.count()
        except Exception:
            continue
        for i in range(count):
            try:
                text = (loc.nth(i).inner_text(timeout=500) or '').strip()
            except Exception:
                continue
            if re.fullmatch(r"\d+", text):
                try:
                    val = int(text)
                except Exception:
                    continue
                if val >= 2 and val not in numbers:
                    numbers.append(val)
    if not numbers:
        return added
    max_num = min(max(numbers), max(2, pages))
    current_ids = _collect_visible_ids(page)
    for n in range(2, max_num + 1):
        loc = _find_pagination_element(page, str(n), allow_disabled=False, allow_active=False)
        if not loc:
            continue
        try:
            classes = (loc.get_attribute("class") or "")
        except Exception:
            classes = ""
        if "_active" in classes or "_disabled" in classes:
            current_ids = _collect_visible_ids(page)
            continue
        if not _safe_click(loc):
            continue
        try:
            page.wait_for_load_state("domcontentloaded")
        except Exception:
            pass
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        if not _wait_items_changed(page, current_ids, timeout_ms=12000):
            current_ids = _collect_visible_ids(page)
            continue
        b = _extract_products_on_page(page)
        if not b:
            current_ids = _collect_visible_ids(page)
            continue
        for it in b:
            added.append(it)
        current_ids = _collect_visible_ids(page)
        time.sleep(delay)
    return added

# ---------------------- Human-like interaction helpers ----------------------
def _click_text_node(page, keywords: List[str], selectors: Optional[List[str]] = None) -> bool:
    if not keywords:
        return False
    lowered = [kw.lower().strip() for kw in keywords if kw and kw.strip()]
    if not lowered:
        return False
    payload = {"keywords": lowered, "selectors": selectors or []}
    try:
        return bool(
            page.evaluate(
                r"""
                ({ keywords, selectors }) => {
                    const lowered = (keywords || []).map(k => (k || '').trim().toLowerCase()).filter(Boolean);
                    if (!lowered.length) { return false; }
                    let nodes = [];
                    if (selectors && selectors.length) {
                        for (const sel of selectors) {
                            try {
                                nodes = nodes.concat(Array.from(document.querySelectorAll(sel)));
                            } catch (e) {}
                        }
                    } else {
                        nodes = Array.from(document.querySelectorAll('button, a, label, span, li, div'));
                    }
                    for (const node of nodes) {
                        if (!(node instanceof HTMLElement)) { continue; }
                        const text = (node.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
                        if (!text) { continue; }
                        for (const kw of lowered) {
                            if (text.includes(kw)) {
                                try { node.scrollIntoView({ block: 'center', behavior: 'smooth' }); } catch (e) {}
                                node.dispatchEvent(new Event('mouseenter', { bubbles: true }));
                                node.dispatchEvent(new Event('mouseover', { bubbles: true }));
                                node.click();
                                return true;
                            }
                        }
                    }
                    return false;
                }
                """,
                payload,
            )
        )
    except Exception:
        return False


def _apply_brand_filter_ui(page, brand: str, delay: float) -> bool:
    brand = (brand or "").strip()
    if not brand:
        return False
    try:
        _click_text_node(page, ["производитель", "производители", "бренд", "бренды"], ["button", "[role=\"button\"]"])
    except Exception:
        pass
    time.sleep(0.4)
    if _click_text_node(page, [brand], ["label", "span", "li", "button"]):
        time.sleep(delay)
        return True
    try:
        typed = page.evaluate(
            """
            (brand) => {
                const inputs = Array.from(document.querySelectorAll('input'));
                for (const input of inputs) {
                    const placeholder = (input.getAttribute('placeholder') || '').toLowerCase();
                    if (!placeholder.includes('поиск')) { continue; }
                    if (!placeholder.includes('бренд') && !placeholder.includes('производ')) { continue; }
                    input.focus();
                    input.value = '';
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.value = brand;
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    return true;
                }
                return false;
            }
            """,
            brand,
        )
    except Exception:
        typed = False
    if typed:
        time.sleep(0.4)
        if _click_text_node(page, [brand], ["label", "span", "li", "button"]):
            time.sleep(delay)
            return True
    try:
        success = page.evaluate(
            r"""
            (brand) => {
                const lower = (brand || '').toLowerCase();
                if (!lower) { return false; }
                const nodes = Array.from(document.querySelectorAll('label, span, li, div'));
                for (const node of nodes) {
                    if (!(node instanceof HTMLElement)) { continue; }
                    const text = (node.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
                    if (!text) { continue; }
                    if (text.includes(lower)) {
                        node.click();
                        return true;
                    }
                }
                return false;
            }
            """,
            brand,
        )
    except Exception:
        success = False
    if success:
        time.sleep(delay)
    return bool(success)


def _detect_active_page_number(page) -> Optional[int]:
    selectors = [
        '.pagination__el._active',
        'nav a[aria-current="page"]',
        'nav li._active',
        'nav a[aria-label="Текущая страница"]',
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count():
                text = (loc.inner_text(timeout=1000) or "").strip()
                if re.fullmatch(r"\d+", text):
                    return int(text)
        except Exception:
            continue
    try:
        loc = page.locator('[data-page].is-active').first
        if loc.count():
            text = (loc.get_attribute("data-page") or "").strip()
            if text.isdigit():
                return int(text)
    except Exception:
        pass
    return None


def _simulate_human_pagination(
    page,
    pages: int,
    delay: float,
    upsert_cb: Callable[[Product], bool],
    captured_ids: Set[str],
    get_items_len: Callable[[], int],
    max_items: int,
) -> int:
    if pages <= 1:
        return 0
    current_page = _detect_active_page_number(page) or 1
    added_total = 0
    consecutive_empty = 0
    attempts = 0
    while current_page < max(1, pages) and get_items_len() < max_items:
        attempts += 1
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass
        next_label = str(current_page + 1)
        nav = _find_pagination_element(page, next_label, allow_disabled=False, allow_active=False)
        if not nav:
            nav = _find_pagination_element(page, "Следующая", allow_disabled=False, allow_active=True)
        if not nav:
            break
        prev_ids = _collect_visible_ids(page)
        prev_url = page.url
        prev_active = _detect_active_page_number(page)
        target_href = None
        try:
            target_href = nav.get_attribute("href") or nav.get_attribute("data-href")
        except Exception:
            target_href = None
        try:
            nav.scroll_into_view_if_needed(timeout=2000)
        except Exception:
            pass
        try:
            nav.hover(timeout=1000)
        except Exception:
            pass
        if not _safe_click(nav):
            break
        try:
            page.wait_for_load_state("domcontentloaded")
        except Exception:
            pass
        try:
            page.wait_for_response(
                lambda r: "product-view/pl/filters" in r.url and r.status == 200,
                timeout=15000,
            )
        except Exception:
            pass
        try:
            page.wait_for_load_state("networkidle", timeout=12000)
        except Exception:
            pass
        changed = _wait_items_changed(page, prev_ids, timeout_ms=15000)
        if not changed:
            try:
                page.wait_for_function(
                    "(prev) => { const active = document.querySelector('[aria-current=\"page\"], .pagination__el._active, nav li._active, [data-page].is-active'); if (!active) { return false; } const text = (active.textContent || '').trim(); return text && text !== String(prev); }",
                    prev_active or "",
                    timeout=8000,
                )
            except Exception:
                pass
            new_active = _detect_active_page_number(page)
            if new_active and new_active != (prev_active or current_page):
                changed = True
        if not changed and target_href:
            try:
                if target_href.startswith("/"):
                    target_href_full = "https://kaspi.kz" + target_href
                else:
                    target_href_full = target_href
                logger.info("Human режим: прямой переход по %s", target_href_full)
                page.goto(target_href_full, wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=12000)
                except Exception:
                    pass
                changed = True
            except Exception:
                pass
        if not changed:
            try:
                page.wait_for_timeout(600)
            except Exception:
                pass
        if not changed and page.url == prev_url:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
            continue
        consecutive_empty = 0
        cards = _extract_products_on_page(page)
        snapshot_ids = []
        for it in cards:
            if it.product_id:
                snapshot_ids.append(it.product_id)
        new_added = 0
        for it in cards:
            if upsert_cb(it):
                new_added += 1
            if it.product_id:
                captured_ids.add(it.product_id)
        added_total += new_added
        detected = _detect_active_page_number(page)
        current_page = detected or (current_page + 1)
        logger.info(
            "Human режим: активная страница %s (цель %s), добавлено %s карточек (итого %s). Видимые ID: %s",
            detected or current_page,
            next_label,
            new_added,
            get_items_len(),
            snapshot_ids[:5],
        )
        if get_items_len() >= max_items:
            break
        time.sleep(delay + random.uniform(0.05, 0.2))
        if attempts > pages * 2:
            break
    return added_total

# ---------------------- Load/scroll helpers ----------------------
def _open_listing_try(page, url: str) -> bool:
    logger.info("Открываю листинг: %s", url)
    page.goto(url, wait_until="domcontentloaded")
    _dismiss(page)
    try:
        page.wait_for_load_state("networkidle", timeout=8000)
    except Exception:
        pass
    try:
        _wait_any_cards(page, timeout_ms=20000, tag="entry")
        return True
    except Exception:
        return False

def _pagination_exists(page) -> bool:
    selectors = [
        'nav a:has-text("2")',
        'nav a[aria-label="Следующая"]',
        'nav a:has-text("Следующая")',
        'a[rel="next"]',
        '.pagination a[rel="next"]',
        '.pagination__el:has-text("2")',
        '.pagination__el:has-text("Следующая")',
    ]
    for sel in selectors:
        try:
            if page.locator(sel).first.count():
                return True
        except Exception:
            continue
    return False


def _find_pagination_element(page, label: str, *, allow_disabled: bool = False, allow_active: bool = False):
    selectors = [
        f'nav a:has-text("{label}")',
        f'nav[aria-label="Pagination"] a:has-text("{label}")',
        f'.pagination__el:has-text("{label}")',
        f'.pagination li:has-text("{label}")',
        f'button:has-text("{label}")',
        f'[role="button"]:has-text("{label}")',
        f'button[data-page="{label}"]',
        f'[data-page="{label}"]',
    ]
    if label.lower() in {"следующая", "дальше"}:
        selectors.extend([
            'a[rel="next"]',
            'button[aria-label*="Следующая"]',
            '[data-test="pagination-next"]',
            'button.pagination__el:has-text("Следующая")',
        ])
    for sel in selectors:
        loc = page.locator(sel).first
        if not loc.count():
            continue
        try:
            classes = (loc.get_attribute("class") or "")
        except Exception:
            classes = ""
        if not allow_disabled and "_disabled" in classes:
            continue
        if not allow_active and "_active" in classes:
            continue
        try:
            inner = loc.locator("a, button").first
            if inner.count():
                loc = inner
        except Exception:
            pass
        return loc
    return None


def _safe_click(locator) -> bool:
    if not locator or not locator.count():
        return False
    try:
        locator.click()
        return True
    except Exception:
        pass
    try:
        locator.evaluate("el => el.click()")
        return True
    except Exception:
        pass
    try:
        handle = locator.element_handle(timeout=500)
        if handle:
            handle.click()
            return True
    except Exception:
        pass
    try:
        inner = locator.locator("a, button").first
        if inner.count():
            inner.click()
            return True
    except Exception:
        pass
    return False

def _click_load_more_until_stop(page, max_clicks:int=30, delay:float=0.7) -> None:
    clicks = 0
    while clicks < max_clicks:
        before = _collect_visible_ids(page)
        btn = None
        for sel in ALL_LOAD_MORE_SELECTORS:
            loc = page.locator(sel).first
            if loc.count():
                btn = loc
                break
        if not btn:
            break
        try:
            btn.click()
        except Exception:
            try:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(0.5)
                btn.click()
            except Exception:
                break
        page.wait_for_load_state("domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        changed = _wait_items_changed(page, before, timeout_ms=10000)
        time.sleep(delay)
        clicks += 1
        if not changed:
            break

def _wait_items_changed(page, prev_ids: Set[str], timeout_ms=12000) -> bool:
    end = time.time() + timeout_ms/1000
    while time.time() < end:
        ids = _collect_visible_ids(page)
        if ids and ids != prev_ids:
            return True
        try:
            page.evaluate("window.scrollTo(0,0)")
        except Exception:
            pass
        time.sleep(0.4)
    return False

# ---------------------- Core: one-run collection ----------------------
def _collect_one_query(query_text: str, pages:int, delay:float, headful:bool, mode:str,
                       max_items:int, detail_limit:int, no_zone:bool, category:str="smartphones", proxy: Optional[str] = "",
                       sort: str = "", human_simulation: bool = False, human_brand: Optional[str] = None) -> List[Product]:
    items: List[Product] = []
    # Индекс для апсерта: ключ -> индекс в items
    index_by_key: Dict[str, int] = {}
    human_brand = (human_brand or "").strip() or None

    rating_cache: Dict[str, Tuple[Optional[float], Optional[int]]] = {}

    def _key_for(it: Product) -> str:
        return it.product_id or f"{it.title}|{it.list_price or ''}"

    def _upsert(new_it: Product):
        key = _key_for(new_it)
        if key in index_by_key:
            i = index_by_key[key]
            cur = items[i]
            # Апгрейдим отсутствующие поля
            if not cur.url and new_it.url:
                cur.url = new_it.url
            if cur.list_price is None and new_it.list_price is not None:
                cur.list_price = new_it.list_price
            if cur.price_min is None and new_it.price_min is not None:
                cur.price_min = new_it.price_min
            if cur.price_default is None and new_it.price_default is not None:
                cur.price_default = new_it.price_default
            # Рейтинг/отзывы: если в текущем пусто — берём новое
            if (cur.rating is None or cur.rating == 0) and (new_it.rating is not None and new_it.rating != 0):
                cur.rating = new_it.rating
            if (cur.reviews is None or cur.reviews == 0) and (new_it.reviews is not None and new_it.reviews != 0):
                cur.reviews = new_it.reviews
            if not cur.best_merchant and new_it.best_merchant:
                cur.best_merchant = new_it.best_merchant
            if not cur.offers_count and new_it.offers_count:
                cur.offers_count = new_it.offers_count
            if new_it.errors:
                cur.errors = (cur.errors + "; " if cur.errors else "") + new_it.errors
            items[i] = cur
            return False  # не новый
        else:
            index_by_key[key] = len(items)
            items.append(new_it)
            return True  # новый

    def _cache_rating(prod: Product):
        if not prod:
            return
        keys: List[str] = []
        if prod.product_id:
            keys.append(prod.product_id)
        fallback = _key_for(prod)
        if fallback and fallback not in keys:
            keys.append(fallback)
        for key in keys:
            prev = rating_cache.get(key, (None, None))
            cached_rating = prev[0]
            cached_reviews = prev[1]
            new_rating = prod.rating if prod and prod.rating not in (None, 0) else cached_rating
            new_reviews = prod.reviews if prod and prod.reviews not in (None, 0) else cached_reviews
            rating_cache[key] = (new_rating, new_reviews)
        if items:
            _apply_rating_cache()

    def _apply_rating_cache() -> int:
        updated = 0
        for it in items:
            keys: List[str] = []
            if it.product_id:
                keys.append(it.product_id)
            fallback = _key_for(it)
            if fallback and fallback not in keys:
                keys.append(fallback)
            applied = False
            for key in keys:
                cached = rating_cache.get(key)
                if not cached:
                    continue
                cr, cv = cached
                if cr not in (None, 0) and (it.rating is None or it.rating == 0):
                    it.rating = cr
                    applied = True
                if cv not in (None, 0) and (it.reviews is None or it.reviews == 0):
                    try:
                        it.reviews = int(cv)
                    except Exception:
                        pass
                    applied = True
                if applied:
                    updated += 1
                    break
        if updated:
            logger.info("Гидрировали рейтинг/отзывы из XHR: обновлено %s карточек (итого %s)", updated, len(items))
        return updated

    # XHR capture state (с запросами)
    last_ep: Optional[CapturedEndpoint] = None
    first_ep: Optional[CapturedEndpoint] = None
    next_hints: Dict[str, Optional[str]] = {}
    page_size_guess = 12

    results_ep: Optional[CapturedEndpoint] = None
    results_params_template: Dict[str, str] = {}
    results_pages_seen: Set[int] = set()
    results_base_url: Optional[str] = None

    # Буфер XHR-выдач, которые ещё не интегрированы в итоговый список
    pending_xhr_payloads: List[Tuple[List[Dict[str, Any]], Dict[str, Any]]] = []

    # Накопим, сколько карточек реально загрузил UI через этот же endpoint — стартовый offset для XHR
    captured_ids: Set[str] = set()
    captured_limit: Optional[int] = None
    captured_total: Optional[int] = None

    def _id_from_obj(obj: dict) -> Optional[str]:
        try:
            pid = (
                obj.get("productId")
                or obj.get("id")
                or obj.get("configSku")
                or obj.get("product_id")
                or ""
            )
            pid = str(pid) if pid is not None else ""
            pid = pid.strip()
            return pid or None
        except Exception:
            return None

    def _price_from_obj(val: Any) -> Optional[float]:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            try:
                return float(val)
            except Exception:
                return None
        if isinstance(val, str):
            return _regex_price_to_float(val)
        return None

    def _product_from_xhr_obj(obj: dict) -> Optional[Product]:
        if not isinstance(obj, dict):
            return None
        pid = _id_from_obj(obj)
        title = str(obj.get("title") or obj.get("name") or "").strip()
        if not title:
            return None
        raw_url = obj.get("shopLink") or obj.get("link") or obj.get("url")
        if isinstance(raw_url, str) and raw_url.startswith("/"):
            url = "https://kaspi.kz" + raw_url
        else:
            url = raw_url
        list_price = _price_from_obj(obj.get("unitPrice") or obj.get("price") or obj.get("basePrice"))
        sale_price = _price_from_obj(
            obj.get("unitSalePrice")
            or obj.get("salePrice")
            or obj.get("minPrice")
            or obj.get("priceMin")
        )
        rating = None
        try:
            if obj.get("rating") is not None:
                rating = float(obj.get("rating"))
        except Exception:
            rating = None
        reviews = None
        try:
            if obj.get("reviewsQuantity") is not None:
                reviews = int(obj.get("reviewsQuantity"))
            elif obj.get("reviewsCount") is not None:
                reviews = int(obj.get("reviewsCount"))
        except Exception:
            reviews = None
        offers_count = None
        try:
            if obj.get("merchantCount") is not None:
                offers_count = int(obj.get("merchantCount"))
            elif obj.get("offersCount") is not None:
                offers_count = int(obj.get("offersCount"))
        except Exception:
            offers_count = None
        best_merchant = None
        major_merchants = obj.get("majorMerchants")
        if isinstance(major_merchants, list) and major_merchants:
            first = major_merchants[0]
            if isinstance(first, dict):
                best_merchant = str(first.get("title") or first.get("name") or first.get("merchantName") or "").strip() or None
            elif isinstance(first, str):
                best_merchant = first.strip() or None
        prod = Product(
            product_id=pid,
            title=title,
            url=url,
            list_price=list_price,
            price_min=sale_price,
            price_default=sale_price or list_price,
            rating=rating,
            reviews=reviews,
            offers_count=offers_count,
            best_merchant=best_merchant,
        )
        return prod

    with sync_playwright() as p:
        if proxy:
            browser = p.chromium.launch(headless=not headful, proxy={"server": proxy})
        else:
            browser = p.chromium.launch(headless=not headful)
        ctx = browser.new_context(**_ctx_opts())
        page = ctx.new_page()
        _ensure_dir("logs")
        def _integrate_pending_xhr(reason: str = "") -> int:
            nonlocal pending_xhr_payloads
            if not pending_xhr_payloads:
                return 0
            added_total = 0
            batches = len(pending_xhr_payloads)
            for payload, meta in pending_xhr_payloads:
                for obj in payload:
                    prod = _product_from_xhr_obj(obj)
                    if not prod:
                        continue
                    _cache_rating(prod)
                    if prod.product_id:
                        captured_ids.add(prod.product_id)
                    if _upsert(prod):
                        added_total += 1
            _apply_rating_cache()
            if added_total:
                logger.info(
                    "XHR фолбэк%s: +%s карточек из %s пакетов (итого %s)",
                    f" ({reason})" if reason else "",
                    added_total,
                    batches,
                    len(items),
                )
            else:
                sizes = [len(payload) for payload, _ in pending_xhr_payloads]
                logger.info(
                    "XHR фолбэк%s: без новых карточек (пакеты=%s)",
                    f" ({reason})" if reason else "",
                    sizes,
                )
            pending_xhr_payloads = []
            return added_total

        def on_request_finished(req):
            nonlocal last_ep, next_hints, page_size_guess, captured_limit, captured_total
            nonlocal results_ep, results_params_template, results_pages_seen
            try:
                resp = req.response()
                if not resp:
                    return
                if not _is_json_like(dict(resp.headers)):
                    return
                method = req.method
                url = req.url
                body = None
                try:
                    body = req.post_data or None
                except Exception:
                    body = None
                data = None
                try:
                    data = resp.json()
                except Exception:
                    data = None
                if data is None:
                    return
                _save_dump("xhr_req", {"method": method, "url": url, "req_headers": dict(req.headers), "body": body or ""})
                _save_dump("xhr_resp", {"url": url, "data": data})
                arr, hints = _infer_array_and_next(data)
                ep = CapturedEndpoint(
                    method=method,
                    url=url,
                    req_headers=dict(req.headers),
                    req_body=body,
                    resp_json=data
                )
                # если массив карточек найден — это «хороший» эндпоинт
                if arr:
                    last_ep = ep
                    if first_ep is None:
                        first_ep = ep
                    try:
                        parsed = urlparse(url)
                        if parsed.path.endswith("/pl/results"):
                            qs = parse_qs(parsed.query)
                            results_ep = ep
                            tmp: Dict[str, str] = {}
                            for k, v in qs.items():
                                if not v:
                                    tmp[k] = ""
                                    continue
                                if k == "page":
                                    try:
                                        results_pages_seen.add(int(v[0]))
                                    except Exception:
                                        pass
                                    continue
                                tmp[k] = v[0]
                            if tmp:
                                results_params_template.update(tmp)
                            results_base_url = url
                        elif parsed.path.endswith("/pl/filters"):
                            qs = parse_qs(parsed.query)
                            base_url = url.replace("/pl/filters", "/pl/results")
                            template: Dict[str, str] = {}
                            for k, v in qs.items():
                                if not v:
                                    template[k] = ""
                                    continue
                                if k == "page":
                                    try:
                                        results_pages_seen.add(int(v[0]))
                                    except Exception:
                                        pass
                                    continue
                                template[k] = v[0]
                            meta_info = data.get("data") if isinstance(data, dict) else {}
                            if isinstance(meta_info, dict):
                                ext = meta_info.get("externalSearchQueryInfo")
                                if isinstance(ext, dict) and ext.get("queryID"):
                                    template["requestId"] = str(ext.get("queryID"))
                            if template:
                                for key in ("all", "page", "offset", "i"):
                                    template.pop(key, None)
                                results_params_template.update({k: v for k, v in template.items() if v is not None})
                                results_base_url = base_url
                    except Exception:
                        pass
                    next_hints = hints or next_hints
                    page_size_guess = max(page_size_guess, len(arr))
                    pending_xhr_payloads.append(
                        (
                            list(arr),
                            {
                                "url": url,
                                "captured_at": time.time(),
                                "size": len(arr),
                                "hints": hints or {},
                            },
                        )
                    )
                    try:
                        for obj in arr:
                            prod = _product_from_xhr_obj(obj)
                            if prod:
                                _cache_rating(prod)
                    except Exception:
                        pass
                    # Накопим ID карточек, которые уже пришли через UI XHR — чтобы стартовать offset с этого места
                    for obj in arr:
                        pid = _id_from_obj(obj) or ""
                        if pid:
                            captured_ids.add(pid)
                    try:
                        meta = data.get("data") if isinstance(data, dict) else {}
                        if isinstance(meta, dict):
                            if meta.get("limit") is not None:
                                captured_limit = int(meta.get("limit"))
                            if meta.get("total") is not None:
                                captured_total = int(meta.get("total"))
                    except Exception:
                        pass
            except Exception:
                pass

        page.on("requestfinished", on_request_finished)

        # Исправлено: используем правильную категорию из параметров.
        # В режиме category открываем ЧИСТУЮ категорию без text, чтобы не было ограничений поиска.
        if mode == "category":
            cat_url = f"https://kaspi.kz/shop/c/{category}/"
            if sort:
                cat_url = _add_query_param(cat_url, 'sort', sort)
        else:
            cat_url = f"https://kaspi.kz/shop/c/{category}/?text={quote(query_text)}"
            if sort:
                cat_url = _add_query_param(cat_url, 'sort', sort)
        search_url = f"https://kaspi.kz/shop/search/?q={quote(query_text)}"
        if sort:
            search_url = _add_query_param(search_url, 'sort', sort)

        entry_used = None
        logger.info("Try category URL: %s", cat_url)
        if mode in ("both", "category") and _open_listing_try(page, cat_url):
            entry_used = cat_url
        if not entry_used:
            logger.info("Try search URL: %s", search_url)
        if not entry_used and mode in ("both", "search") and _open_listing_try(page, search_url):
            entry_used = search_url
        if not entry_used:
            logger.info("Не удалось отрисовать карточки на входе — завершаем.")
            browser.close()
            return items

        # UI стр.1
        batch = _extract_products_on_page(page)
        logger.info("Стр.1 карточек: %s", len(batch))
        logger.info(
            "ID стартовых карточек (первые %s из %s): %s",
            min(30, len(batch)),
            len(batch),
            [it.product_id for it in batch[:30]],
        )
        # Зафиксируем c-параметр зоны/города при наличии и будем добавлять его в пагинацию
        c_param = _detect_c_param(page)
        for it in batch:
            _upsert(it)
        _apply_rating_cache()

        if items and len(items) < max_items:
            if not pending_xhr_payloads:
                wait_until = time.time() + max(1.2, delay)
                while not pending_xhr_payloads and time.time() < wait_until:
                    time.sleep(0.15)
            if pending_xhr_payloads:
                _integrate_pending_xhr("после стартовой HTML страницы")
            else:
                _apply_rating_cache()

        # Сначала пробуем load-more/скролл в любом случае
        grown = False
        if any(page.locator(s).first.count() for s in ALL_LOAD_MORE_SELECTORS):
            logger.info("Пробуем «Показать ещё».")
            before = len(_collect_visible_ids(page))
            _click_load_more_until_stop(page, max_clicks=60, delay=delay)
            after = len(_collect_visible_ids(page))
            grown = after > before
        else:
            before = len(_collect_visible_ids(page))
            for _ in range(12):
                try:
                    page.evaluate("window.scrollBy(0, Math.max(400, document.body.scrollHeight * 0.85))")
                except Exception:
                    pass
                time.sleep(delay)
            after = len(_collect_visible_ids(page))
            grown = after > before
        if grown:
            b = _extract_products_on_page(page)
            added = 0
            for it in b:
                _upsert(it)
                added += 1
            logger.info("Дозагрузили карточек: +%s (итого: %s)", added, len(items))

        if human_simulation:
            logger.info("Включен режим имитации человека: пробуем листать пагинацию через UI.")
            if human_brand:
                prev_ids = _collect_visible_ids(page)
                applied = _apply_brand_filter_ui(page, human_brand, delay)
                if applied:
                    _wait_items_changed(page, prev_ids, timeout_ms=15000)
                    filtered_batch = _extract_products_on_page(page)
                    new_added = 0
                    for it in filtered_batch:
                        if _upsert(it):
                            new_added += 1
                        if it.product_id:
                            captured_ids.add(it.product_id)
                    logger.info("Human режим: фильтр '%s' применён, +%s карточек (итого %s)", human_brand, new_added, len(items))
                else:
                    logger.info("Human режим: не удалось применить фильтр '%s'", human_brand)
            human_added = _simulate_human_pagination(
                page,
                pages=pages,
                delay=delay,
                upsert_cb=_upsert,
                captured_ids=captured_ids,
                get_items_len=lambda: len(items),
                max_items=max_items,
            )
            if human_added:
                logger.info("Human UI пагинация: +%s карточек (итого %s)", human_added, len(items))
            else:
                logger.info("Human UI пагинация не дала новых карточек (итого %s)", len(items))
                _integrate_pending_xhr("после human UI")
        else:
            # Далее HTML-пагинация (?page=, Следующая, номера)
            try:
                base_for_pages = entry_used or cat_url
                if c_param:
                    base_for_pages = _add_query_param(base_for_pages, 'c', c_param)
                more = _paginate_by_page_param(page, base_for_pages, start_page=2, pages=pages, delay=delay)
            except Exception:
                more = []
            try:
                extra = _paginate_by_nav_next(page, pages=pages, delay=delay)
            except Exception:
                extra = []
            if extra:
                more.extend(extra)
            try:
                extra2 = _paginate_by_nav_numbers(page, pages=pages, delay=delay)
            except Exception:
                extra2 = []
            if extra2:
                more.extend(extra2)
            if more:
                added = 0
                for it in more:
                    _upsert(it)
                    added += 1
                logger.info("HTML-пагинация ?page=/Следующая/цифры: +%s карточек (итого: %s)", added, len(items))

            brand_filters = DEFAULT_BRANDS if (category == "smartphones") else []
            for bname in brand_filters:
                if len(items) >= max_items:
                    break
                try:
                    logger.info("Фильтр производитель: %s", bname)
                    page.goto(entry_used or cat_url, wait_until="domcontentloaded")
                    _dismiss(page)
                    try:
                        page.wait_for_load_state("networkidle", timeout=8000)
                    except Exception:
                        pass
                    prev = _collect_visible_ids(page)
                    _wait_items_changed(page, prev, timeout_ms=12000)
                    b0 = _extract_products_on_page(page)
                    added0 = 0
                    for it in b0:
                        if _upsert(it):
                            added0 += 1
                    if added0:
                        logger.info("Стр. бренда '%s': +%s", bname, added0)
                    before = len(_collect_visible_ids(page))
                    if any(page.locator(s).first.count() for s in ALL_LOAD_MORE_SELECTORS):
                        _click_load_more_until_stop(page, max_clicks=60, delay=delay)
                    else:
                        for _ in range(12):
                            try:
                                page.evaluate("window.scrollBy(0, Math.max(400, document.body.scrollHeight * 0.85))")
                            except Exception:
                                pass
                            time.sleep(delay)
                    after = len(_collect_visible_ids(page))
                    if after > before:
                        b1 = _extract_products_on_page(page)
                        for it in b1:
                            _upsert(it)
                    base_for_pages = page.url
                    if c_param:
                        base_for_pages = _add_query_param(base_for_pages, 'c', c_param)
                    if sort:
                        base_for_pages = _add_query_param(base_for_pages, 'sort', sort)
                    try:
                        more_b = _paginate_by_page_param(page, base_for_pages, start_page=2, pages=pages, delay=delay)
                    except Exception:
                        more_b = []
                    try:
                        extra_b = _paginate_by_nav_next(page, pages=pages, delay=delay)
                    except Exception:
                        extra_b = []
                    try:
                        extra2_b = _paginate_by_nav_numbers(page, pages=pages, delay=delay)
                    except Exception:
                        extra2_b = []
                    brand_added = 0
                    for seq in (more_b, extra_b, extra2_b):
                        for it in seq:
                            if _upsert(it):
                                brand_added += 1
                    if brand_added:
                        logger.info("HTML пагинации по бренду '%s': +%s (итого %s)", bname, brand_added, len(items))
                except Exception as e:
                    logger.info("Фильтр по бренду '%s' не сработал: %s", bname, e)

            if len(items) < max_items:
                _integrate_pending_xhr("после HTML UI")

        if last_ep and len(items) < max_items:
            logger.info("Пробуем XHR-пагинацию (без UI).")
            req = ctx.request
            ep = last_ep
            rounds_left = max(0, pages - 1)
            base_headers = {
                "accept": "application/json, text/plain, */*",
                "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8",
                "user-agent": _ctx_opts()["user_agent"],
            }
            # Добавим x-ks-city и referer если можем
            try:
                if c_param:
                    base_headers["x-ks-city"] = str(c_param)
            except Exception:
                pass
            base_referer = entry_used or cat_url
            if base_referer:
                base_headers["referer"] = base_referer
            # Построим базовый URL без page, будем ходить по offset, начиная с числа карточек, реально полученных UI
            # Если UI ничего не подгружал — captured_ids может быть пустым (offset=0)
            def _strip_params_page(url: str) -> str:
                try:
                    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
                    pr = urlparse(url)
                    qs = parse_qs(pr.query)
                    if "page" in qs:
                        qs.pop("page", None)
                    new_q = urlencode({k: v[0] if isinstance(v, list) and v else v for k, v in qs.items()})
                    return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))
                except Exception:
                    return url

            # если limit/total ещё не известны — возьмём из ep.resp_json
            if captured_limit is None or captured_total is None:
                try:
                    meta0 = ep.resp_json.get("data") if isinstance(ep.resp_json, dict) else {}
                    if isinstance(meta0, dict):
                        if captured_limit is None and meta0.get("limit") is not None:
                            captured_limit = int(meta0.get("limit"))
                        if captured_total is None and meta0.get("total") is not None:
                            captured_total = int(meta0.get("total"))
                except Exception:
                    pass
            # В крайнем случае используем page_size_guess
            limit_eff = int(captured_limit or page_size_guess or 12)

            # Хелпер: убрать указанные query-параметры
            def _strip_params(url: str, keys: List[str]) -> str:
                try:
                    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
                    pr = urlparse(url)
                    qs = parse_qs(pr.query)
                    for k in keys:
                        qs.pop(k, None)
                    new_q = urlencode({k: v[0] if isinstance(v, list) and v else v for k, v in qs.items()})
                    return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))
                except Exception:
                    return url

            added_any_xhr = False

            def _ensure_request_id_via_filters(base_results_url: str) -> bool:
                nonlocal results_params_template, results_base_url
                if results_params_template.get("requestId"):
                    return True
                filters_base = base_results_url.replace("/pl/results", "/pl/filters")
                probe_params = dict(results_params_template)
                for key in ("page", "offset", "i"):
                    probe_params.pop(key, None)
                probe_params.setdefault("all", "true")
                probe_params.setdefault("fl", "true")
                if c_param:
                    probe_params.setdefault("c", str(c_param))
                if sort:
                    probe_params.setdefault("sort", str(sort))
                filters_url = _set_query_params(filters_base, probe_params)
                if no_zone:
                    filters_url = _strip_zone_in_q(filters_url)
                try:
                    r = req.get(filters_url, timeout=25000, headers=base_headers)
                except Exception as e:
                    logger.info("XHR /pl/filters для requestId упал: %s — пропуск.", e)
                    return False
                if not r.ok:
                    logger.info("XHR GET %s -> HTTP %s при попытке requestId — пропуск.", filters_url, r.status)
                    return False
                try:
                    data = r.json()
                except Exception:
                    logger.info("XHR /pl/filters для requestId вернул не JSON — пропуск.")
                    return False
                _save_dump("xhr_follow_req", {"method": "GET", "url": filters_url, "body": ""})
                _save_dump("xhr_follow_resp", {"url": filters_url, "data": data})
                req_id = _extract_request_id_from_data(data)
                if req_id:
                    results_params_template["requestId"] = req_id
                    try:
                        parsed = urlparse(filters_url)
                        qs = parse_qs(parsed.query)
                        sanitized = {k: (v[0] if v else "") for k, v in qs.items()}
                        for key in ("all", "page", "offset", "i"):
                            sanitized.pop(key, None)
                        if sanitized:
                            results_params_template.update(sanitized)
                    except Exception:
                        pass
                    try:
                        results_base_url = _strip_params(filters_url.replace("/pl/filters", "/pl/results"), ["offset", "i"])
                    except Exception:
                        results_base_url = filters_url.replace("/pl/filters", "/pl/results")
                    return True
                return False

            def _run_results_pagination() -> bool:
                nonlocal results_ep, results_base_url, next_hints, captured_limit, captured_total, last_ep
                if len(items) >= max_items:
                    return False
                base_candidate = results_ep.url if results_ep else results_base_url
                if not base_candidate:
                    source = (first_ep.url if first_ep else ep.url)
                    if source:
                        base_candidate = source.replace("/pl/filters", "/pl/results")
                        results_base_url = base_candidate
                if not base_candidate:
                    return False
                base_results_url = _strip_params(base_candidate, ["page", "offset", "i"])
                if no_zone:
                    base_results_url = _strip_zone_in_q(base_results_url)
                template_params = dict(results_params_template)
                for key in ("all", "page", "offset", "i"):
                    template_params.pop(key, None)
                if c_param:
                    template_params.setdefault("c", str(c_param))
                if sort:
                    template_params.setdefault("sort", str(sort))
                template_params.setdefault("ui", template_params.get("ui") or "d")
                template_params.setdefault("fl", template_params.get("fl") or "true")
                if not template_params.get("requestId"):
                    if not _ensure_request_id_via_filters(base_results_url):
                        return False
                    template_params = dict(results_params_template)
                    for key in ("all", "page", "offset", "i"):
                        template_params.pop(key, None)
                    template_params.setdefault("ui", template_params.get("ui") or "d")
                    template_params.setdefault("fl", template_params.get("fl") or "true")
                request_id_val = template_params.get("requestId")
                if not request_id_val:
                    return False
                template_params["requestId"] = request_id_val
                max_page_seen = max(results_pages_seen) if results_pages_seen else 1
                rounds_left_results = max(0, pages - max_page_seen)
                duplicate_results = 0
                next_page = max_page_seen + 1
                progress = False
                logger.info("Пробуем XHR пагинацию по /pl/results.")
                while rounds_left_results > 0 and len(items) < max_items:
                    params = dict(template_params)
                    params["page"] = str(next_page)
                    new_url = _set_query_params(base_results_url, params)
                    try:
                        r = req.get(new_url, timeout=25000, headers=base_headers)
                    except Exception as e:
                        logger.info("XHR /pl/results fetch упал: %s — стоп.", e)
                        break
                    if not r.ok:
                        logger.info("XHR GET %s -> HTTP %s — стоп.", new_url, r.status)
                        break
                    try:
                        data = r.json()
                    except Exception:
                        logger.info("XHR /pl/results ответ не JSON — стоп.")
                        break
                    _save_dump("xhr_follow_req", {"method": "GET", "url": new_url, "body": ""})
                    _save_dump("xhr_follow_resp", {"url": new_url, "data": data})
                    arr, hints = _infer_array_and_next(data)
                    if not arr and isinstance(data, dict):
                        payload = data.get("data")
                        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                            arr = payload
                            hints = {}
                    if not arr:
                        logger.info("XHR /pl/results: ответ без карточек — стоп.")
                        break
                    added = 0
                    new_ids: Set[str] = set()
                    for obj in arr:
                        pid = _id_from_obj(obj)
                        if pid:
                            new_ids.add(pid)
                        prod = _product_from_xhr_obj(obj)
                        if prod and _upsert(prod):
                            added += 1
                    for pid in new_ids:
                        captured_ids.add(pid)
                    results_pages_seen.add(next_page)
                    req_id_resp = _extract_request_id_from_data(data)
                    if req_id_resp and results_params_template.get("requestId") != req_id_resp:
                        results_params_template["requestId"] = req_id_resp
                    if added:
                        progress = True
                        duplicate_results = 0
                    else:
                        duplicate_results += 1
                    logger.info("XHR /pl/results page=%s: +%s карточек (итого %s)", next_page, added, len(items))
                    try:
                        meta = data.get("data") if isinstance(data, dict) else {}
                        if isinstance(meta, dict):
                            if meta.get("limit") is not None:
                                captured_limit = max(int(meta.get("limit") or 0), captured_limit or 0)
                            if meta.get("total") is not None:
                                captured_total = int(meta.get("total"))
                    except Exception:
                        pass
                    if arr and hints:
                        next_hints = hints or next_hints
                    ep_local = CapturedEndpoint(
                        method="GET",
                        url=new_url,
                        req_headers=results_ep.req_headers if results_ep else base_headers,
                        req_body=results_ep.req_body if results_ep else None,
                        resp_json=data,
                    )
                    results_ep = ep_local
                    last_ep = ep_local
                    rounds_left_results -= 1
                    next_page += 1
                    if duplicate_results >= 2:
                        break
                    time.sleep(delay + random.uniform(0.05, 0.2))
                return progress

            results_progress = _run_results_pagination()
            if results_progress:
                added_any_xhr = True

            if results_pages_seen:
                try:
                    rounds_left = max(0, pages - max(results_pages_seen))
                except Exception:
                    pass

            current_offset = max(len(captured_ids), len(items))

            # База — URL без page/i/offset; q санитайзим при необходимости
            base_url_for_offset = _strip_params_page((first_ep or ep).url)
            base_url_for_offset = _strip_params(base_url_for_offset, ["offset", "i", "start", "from"])
            if no_zone:
                base_url_for_offset = _strip_zone_in_q(base_url_for_offset)
            env_params: Dict[str, str] = {"ui": "d", "fl": "true"}
            if c_param:
                env_params["c"] = str(c_param)
            if sort:
                env_params["sort"] = str(sort)
            base_url_for_offset = _set_query_params(base_url_for_offset, env_params)

            preferred_all = "true"
            used_all_false = False
            duplicate_streak = 0
            while rounds_left > 0 and len(items) < max_items:
                stride = max(1, limit_eff)
                offset_to_use = current_offset
                raw_page_idx = 0
                try:
                    raw_page_idx = max(0, int(offset_to_use // stride))
                except Exception:
                    raw_page_idx = 0
                page_number = max(1, raw_page_idx + 1)

                param_variants: List[Dict[str, str]] = [
                    {"offset": str(offset_to_use), "page": str(page_number), "i": "-1"},
                    {"offset": str(offset_to_use), "page": str(page_number), "i": str(raw_page_idx)},
                    {"offset": str(offset_to_use), "page": str(page_number)},
                    {"offset": str(offset_to_use), "i": "-1"},
                    {"offset": str(offset_to_use), "i": str(raw_page_idx)},
                    {"offset": str(offset_to_use)},
                ]

                try:
                    logger.info(
                        "XHR offset-параметры: offset=%s stride=%s raw_page=%s page=%s variants=%s",
                        offset_to_use,
                        stride,
                        raw_page_idx,
                        page_number,
                        param_variants,
                    )
                except Exception:
                    pass

                all_sequence = [preferred_all] + [v for v in ("true", "false") if v != preferred_all]
                success_data = None
                success_arr: Optional[List[dict]] = None
                success_hints: Dict[str, Optional[str]] = {}
                success_url: Optional[str] = None
                chosen_all = preferred_all
                fallback_entry: Optional[Tuple[Dict[str, Any], List[dict], Dict[str, Optional[str]], str, str]] = None
                last_status: Optional[Tuple[str, str]] = None
                last_error: Optional[Exception] = None

                for all_val in all_sequence:
                    base_with_all = _set_query_params(base_url_for_offset, {"all": all_val})
                    base_clean = _strip_params(base_with_all, ["offset", "start", "from", "page", "p", "pageNumber", "page_num", "i"])
                    for variant in param_variants:
                        variant_clean = {k: v for k, v in variant.items() if v is not None}
                        new_url = _set_query_params(base_clean, variant_clean)
                        try:
                            r = req.get(new_url, timeout=25000, headers=base_headers)
                        except Exception as e:
                            last_error = e
                            continue
                        if not r.ok:
                            last_status = (new_url, str(r.status))
                            continue
                        try:
                            data = r.json()
                        except Exception:
                            last_status = (new_url, "non-json")
                            continue
                        if not results_params_template.get("requestId"):
                            try:
                                req_candidate = _extract_request_id_from_data(data)
                                if req_candidate:
                                    results_params_template["requestId"] = req_candidate
                                    try:
                                        parsed_follow = urlparse(new_url)
                                        qs_follow = parse_qs(parsed_follow.query)
                                        sanitized_follow = {k: (v[0] if v else "") for k, v in qs_follow.items()}
                                        for key in ("all", "page", "offset", "i"):
                                            sanitized_follow.pop(key, None)
                                        if sanitized_follow:
                                            results_params_template.update(sanitized_follow)
                                    except Exception:
                                        pass
                                    try:
                                        results_base_url = _strip_params(new_url.replace("/pl/filters", "/pl/results"), ["offset", "i"])
                                    except Exception:
                                        results_base_url = new_url.replace("/pl/filters", "/pl/results")
                            except Exception:
                                pass
                        arr, hints = _infer_array_and_next(data)
                        if not arr and isinstance(data, dict):
                            payload = data.get("data")
                            if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                                arr = payload
                                hints = {}
                        if not arr:
                            last_status = (new_url, "empty")
                            continue
                        has_new = False
                        for obj in arr:
                            pid = _id_from_obj(obj)
                            if pid and pid not in captured_ids:
                                has_new = True
                                break
                        if has_new:
                            success_data = data
                            success_arr = arr
                            success_hints = hints
                            success_url = new_url
                            chosen_all = all_val
                            break
                        if fallback_entry is None:
                            fallback_entry = (data, arr, hints, new_url, all_val)
                        last_status = (new_url, "no-new-ids")
                        continue
                    if success_data is not None:
                        break

                if success_data is None and fallback_entry is not None:
                    success_data, success_arr, success_hints, success_url, chosen_all = fallback_entry

                if success_data is None or success_arr is None or success_url is None:
                    if last_status:
                        logger.info("XHR GET %s -> %s — стоп.", last_status[0], last_status[1])
                    elif last_error:
                        logger.info("XHR fetch упал: %s — стоп.", last_error)
                    else:
                        logger.info("XHR offset-пагинация: не удалось подобрать параметры — стоп.")
                    break

                _save_dump("xhr_follow_req", {"method": "GET", "url": success_url, "body": ""})
                _save_dump("xhr_follow_resp", {"url": success_url, "data": success_data})

                data = success_data
                arr = success_arr
                hints = success_hints
                preferred_all = chosen_all
                if chosen_all == "false":
                    used_all_false = True

                # Актуализируем limit/total
                try:
                    meta = data.get("data") if isinstance(data, dict) else {}
                    if isinstance(meta, dict):
                        if meta.get("limit") is not None:
                            limit_eff = max(limit_eff, int(meta.get("limit") or limit_eff))
                        if meta.get("total") is not None:
                            captured_total = int(meta.get("total"))
                except Exception:
                    pass

                # Добавляем товары и пополняем internal seen для корректного шага offset
                added = 0
                new_ids_in_resp: Set[str] = set()
                unique_new_ids: Set[str] = set()
                for obj in arr:
                    pid = _id_from_obj(obj)
                    if pid:
                        new_ids_in_resp.add(pid)
                        if pid not in captured_ids:
                            unique_new_ids.add(pid)
                    title = obj.get("title") or obj.get("name") or ""
                    urlp = obj.get("shopLink") or obj.get("url") or obj.get("href") or None
                    if urlp and urlp.startswith("/"):
                        urlp = "https://kaspi.kz" + urlp
                    price_fields = [obj.get("unitSalePrice"), obj.get("unitPrice"), obj.get("priceFormatted"), obj.get("price"), obj.get("listPrice"), obj.get("minPrice"), obj.get("priceMin")]
                    lp = None
                    for pf in price_fields:
                        lp = _regex_price_to_float(pf)
                        if lp is not None:
                            break
                    rating_val = None
                    try:
                        rv = obj.get("rating")
                        if rv is not None:
                            rating_val = float(rv)
                    except Exception:
                        pass
                    reviews_val = None
                    try:
                        rq = obj.get("reviewsQuantity")
                        if isinstance(rq, (int, float)):
                            reviews_val = int(rq)
                    except Exception:
                        pass
                    if not title:
                        continue
                    it = Product(product_id=pid, title=title, url=urlp, list_price=lp, rating=rating_val, reviews=reviews_val)
                    if _upsert(it):
                        added += 1

                # обновим счётчики endpoint seen независимо от того, были ли дубли в общем items
                for pid in unique_new_ids:
                    captured_ids.add(pid)

                logger.info("XHR offset-пагинация: +%s карточек (итого %s)", added, len(items))
                if added > 0:
                    added_any_xhr = True

                # Подготовим следующий offset, но применим его только если получили прогресс
                prev_offset = offset_to_use
                candidate_offset = prev_offset + stride

                if unique_new_ids:
                    duplicate_streak = 0
                    used_all_false = False
                    current_offset = max(candidate_offset, len(captured_ids), len(items))
                else:
                    # если дубликаты — попробуем один раз переключиться на all=false и повторить тот же offset
                    if chosen_all != "false" and not used_all_false:
                        logger.info(
                            "XHR offset-пагинация: окно без новых ID при all=%s — пробуем all=false.",
                            chosen_all,
                        )
                        preferred_all = "false"
                        current_offset = prev_offset
                        time.sleep(delay + random.uniform(0.05, 0.2))
                        continue

                    duplicate_streak += 1
                    current_offset = prev_offset
                    if duplicate_streak >= 2:
                        logger.info("XHR offset-пагинация: два окна подряд без новых ID — стоп.")
                        break

                rounds_left -= 1
                next_hints = hints or next_hints

                # Стоп по total, если знаем
                if captured_total is not None and current_offset >= captured_total:
                    logger.info("Достигнут конец по total (%s) — стоп.", captured_total)
                    break

                # Обновим ep для следующего шага
                ep = CapturedEndpoint(
                    method="GET",
                    url=new_url,
                    req_headers=ep.req_headers,
                    req_body=ep.req_body,
                    resp_json=data,
                )
                time.sleep(delay + random.uniform(0.05, 0.2))

            if not results_progress and results_params_template.get("requestId") and len(items) < max_items:
                try:
                    retry_progress = _run_results_pagination()
                    if retry_progress:
                        results_progress = True
                        added_any_xhr = True
                except Exception as e:
                    logger.info("Повторная попытка /pl/results не удалась: %s", e)

            # Фолбэк: пробуем пагинацию по page=2..N, если ещё есть лимит по товарам
            if len(items) < max_items:
                try:
                    logger.info("Пробуем XHR по page=N.")
                    # на всякий случай уберём offset и служебные индексы, и возьмём базу из первого UI XHR
                    base_url_for_page = _strip_params((first_ep or ep).url, ["offset", "i"])  # убираем offset, i
                    if no_zone:
                        base_url_for_page = _strip_zone_in_q(base_url_for_page)
                    # добавим обязательные параметры окружения
                    if c_param:
                        base_url_for_page = _set_query_params(base_url_for_page, {"c": str(c_param)})
                    if sort:
                        base_url_for_page = _set_query_params(base_url_for_page, {"sort": str(sort)})

                    # Сначала попробуем с all=true, затем при отсутствии прогресса — all=false
                    def _page_loop(all_value: str) -> bool:
                        # независимый бюджет для page-фолбэка
                        page_rounds_left = max(0, pages - 1)
                        any_added = False
                        start_page_num = 2  # page=1 обычно первая выдача
                        # если в исходном URL уже был page — начнём с +1
                        try:
                            from urllib.parse import urlparse, parse_qs
                            pr0 = urlparse(base_url_for_page)
                            qs0 = parse_qs(pr0.query)
                            if qs0.get("page"):
                                sp = int(qs0["page"][0])
                                start_page_num = max(2, sp + 1)
                        except Exception:
                            pass
                        pg = start_page_num
                        # Две подряд пустые страницы — стоп
                        empty_streak = 0
                        while page_rounds_left > 0 and len(items) < max_items:
                            # усиливаем окружение: ui=d, fl=true, i=pg
                            url_with_env = _set_query_params(base_url_for_page, {"all": all_value, "ui": "d", "fl": "true"})
                            offset_val = max(0, pg * max(1, limit_eff))
                            new_url = _set_query_params(url_with_env, {"page": str(pg), "i": str(pg), "offset": str(offset_val)})
                            try:
                                r2 = req.get(new_url, timeout=25000, headers=base_headers)
                            except Exception as e:
                                logger.info("XHR page fetch упал: %s — стоп.", e)
                                break
                            if not r2.ok:
                                logger.info("XHR GET %s -> HTTP %s — стоп.", new_url, r2.status)
                                break
                            try:
                                data2 = r2.json()
                            except Exception:
                                logger.info("Ответ не JSON — стоп.")
                                break
                            _save_dump("xhr_follow_req", {"method": "GET", "url": new_url, "body": ""})
                            _save_dump("xhr_follow_resp", {"url": new_url, "data": data2})
                            arr2, _ = _infer_array_and_next(data2)
                            if not arr2:
                                logger.info("Ответ без массива карточек — стоп.")
                                break
                            added2 = 0
                            for obj in arr2:
                                pid = (obj.get("productId") or obj.get("id") or obj.get("configSku") or obj.get("product_id"))
                                pid = str(pid) if pid is not None else None
                                title = obj.get("title") or obj.get("name") or ""
                                urlp = obj.get("shopLink") or obj.get("url") or obj.get("href") or None
                                if urlp and urlp.startswith("/"):
                                    urlp = "https://kaspi.kz" + urlp
                                price_fields = [obj.get("unitSalePrice"), obj.get("unitPrice"), obj.get("priceFormatted"), obj.get("price"), obj.get("listPrice"), obj.get("minPrice"), obj.get("priceMin")]
                                lp = None
                                for pf in price_fields:
                                    lp = _regex_price_to_float(pf)
                                    if lp is not None:
                                        break
                                rating_val = None
                                try:
                                    rv = obj.get("rating")
                                    if rv is not None:
                                        rating_val = float(rv)
                                except Exception:
                                    pass
                                reviews_val = None
                                try:
                                    rq = obj.get("reviewsQuantity")
                                    if isinstance(rq, (int, float)):
                                        reviews_val = int(rq)
                                except Exception:
                                    pass
                                if not title:
                                    continue
                                it = Product(product_id=pid, title=title, url=urlp, list_price=lp, rating=rating_val, reviews=reviews_val)
                                if _upsert(it):
                                    added2 += 1
                            logger.info("XHR page=%s (all=%s): +%s карточек (итого %s)", pg, all_value, added2, len(items))
                            any_added = any_added or (added2 > 0)
                            page_rounds_left -= 1
                            pg += 1
                            empty_streak = empty_streak + 1 if added2 == 0 else 0
                            if empty_streak >= 2:
                                break
                            time.sleep(delay + random.uniform(0.05, 0.2))
                        return any_added

                    progress = _page_loop("true")
                    if not progress:
                        _page_loop("false")
                except Exception as e:
                    logger.info("Page-фолбэк не выполнился: %s", e)

        if pending_xhr_payloads:
            _integrate_pending_xhr("перед деталями")

        logger.info("Обогащение detail (rating/reviews + min=default=list_price) ...")
        items = _enrich_detail_min_price_and_meta(ctx, items, limit=min(detail_limit, len(items)), delay=max(0.4, delay-0.3))
        browser.close()

    return items

# ---------------------- Orchestrate brand-splitting ----------------------
DEFAULT_BRANDS = ["apple","samsung","xiaomi","realme","huawei","oppo","vivo","tecno","infinix"]

def _merge_products(dest: List[Product], src: Iterable[Product]) -> List[Product]:
    seen: Set[Tuple] = set()
    for it in dest:
        seen.add((it.product_id, it.title))
    added = 0
    for it in src:
        key = (it.product_id, it.title)
        if key in seen:
            continue
        seen.add(key)
        dest.append(it)
        added += 1
    if added:
        logger.info("Мердж: добавлено %s новых карточек (итого %s)", added, len(dest))
    return dest

# ---------------------- Public API ----------------------
def collect(query_text: str, pages:int, delay:float, headful:bool=False,
            mode:str="both", max_items:int=200, detail_limit:int=24,
            split_by_brand:bool=False, brands:Optional[List[str]]=None,
            no_zone:bool=True, category:str="smartphones", proxy: Optional[str] = "",
            sort: str = "", human_simulation: bool = False, human_brand: Optional[str] = None) -> List[Product]:

    if not split_by_brand:
        return _collect_one_query(
            query_text, pages, delay, headful, mode, max_items, detail_limit,
            no_zone, category=category, proxy=proxy, sort=sort,
            human_simulation=human_simulation, human_brand=human_brand,
        )

    brands = brands or DEFAULT_BRANDS
    all_items: List[Product] = []
    cap = max_items

    for b in brands:
        if len(all_items) >= cap:
            break
        q = f"{query_text} {b}"
        logger.info("=== Бренд '%s' ===", b)
        chunk = _collect_one_query(
            q, pages, delay, headful, mode,
            max_items=cap - len(all_items),
            detail_limit=detail_limit, no_zone=no_zone,
            category=category, proxy=proxy, sort=sort,
            human_simulation=human_simulation, human_brand=human_brand,
        )
        _merge_products(all_items, chunk)
        # небольшая пауза между брендами
        time.sleep(max(0.2, delay - 0.3))

    return all_items

def save_csv(items: List[Product], path: str):
    fieldnames = ["product_id","title","url","list_price","price_min","price_default",
                  "rating","reviews","offers_count","best_merchant","errors"]
    _ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in items:
            w.writerow(asdict(it))
    logger.info("CSV сохранён: %s (%s строк)", path, len(items))

def main():
    p = argparse.ArgumentParser(description="Kaspi scraper — UI + XHR авто-пагинация (зональный-санитайз + бренды)")
    p.add_argument("--category", default="smartphones")
    p.add_argument("--query", default="смартфон")
    p.add_argument("--pages", type=int, default=5)
    p.add_argument("--out", default="data/kaspi_smartphones.csv")
    p.add_argument("--delay", type=float, default=0.9)
    p.add_argument("--headful", action="store_true")
    p.add_argument("--save-dumps", action="store_true", help="Save XHR dumps to logs/ directory")
    p.add_argument("--proxy", default="", help="Playwright proxy, e.g., http://host:port")
    p.add_argument("--mode", choices=["both","category","search"], default="both")
    p.add_argument("--max-items", type=int, default=200)
    p.add_argument("--detail-limit", type=int, default=24)
    p.add_argument("--no-zone", action="store_true", default=True, help="Удалять :availableInZones:... из q при XHR-пагинации (вкл. по умолчанию)")
    p.add_argument("--split-by-brand", action="store_true", help="Дробить запрос по брендам и объединять результаты")
    p.add_argument("--brands", default="", help="Список брендов через запятую (исп. с --split-by-brand). Пусто = дефолтный набор.")
    p.add_argument("--sort", default="", help="Параметр сортировки листинга (например, popularity, price_asc, price_desc)")
    p.add_argument("--human-sim", action="store_true", help="Использовать имитацию пользователя (Playwright) для пагинации вместо XHR")
    p.add_argument("--human-brand", default="", help="Бренд для ручного выбора фильтра в human-sim режиме")
    args = p.parse_args()

    if args.pages < 1:
        logger.error("--pages должно быть >= 1")
        sys.exit(2)

    brands_list = None
    if args.split_by_brand and args.brands.strip():
        brands_list = [s.strip() for s in args.brands.split(",") if s.strip()]
    # enable dumps if requested
    global SAVE_DUMPS
    if args.save_dumps:
        SAVE_DUMPS = True

    human_brand = args.human_brand.strip() or None

    items = collect(
        query_text=args.query,
        pages=args.pages,
        delay=args.delay,
        headful=args.headful,
        mode=args.mode,
        max_items=args.max_items,
        detail_limit=args.detail_limit,
        split_by_brand=args.split_by_brand,
        brands=brands_list,
        no_zone=args.no_zone,
        category=args.category,
        proxy=args.proxy,
        sort=args.sort,
        human_simulation=args.human_sim,
        human_brand=human_brand,
    )
    save_csv(items, args.out)

if __name__ == "__main__":
    main()
