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
from typing import List, Dict, Optional, Set, Tuple, Iterable
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
                            if "hasNextPage" in pi:
                                hints["has_next"] = bool(pi["hasNextPage"])  # type: ignore
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
            for k in ("page","pageNumber","p"):
                if isinstance(vars.get(k), int):
                    vars[k] = int(vars[k]) + 1; got = True
            for k in ("offset","start","from"):
                if isinstance(vars.get(k), int):
                    vars[k] = int(vars[k]) + step; got = True
            if hints.get("cursor"):
                for k in ("cursor","after","endCursor"):
                    if k in vars:
                        vars[k] = hints["cursor"]; got = True
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
def _jsonld_rating_reviews(page) -> Dict[str, Optional[float]]:
    """Извлечение рейтинга и отзывов из HTML (JSON-LD устарел)"""
    meta = {"rating": None, "reviews": None}
    
    try:
        # Сначала пробуем старый метод JSON-LD для обратной совместимости
        try:
            script = page.locator('script[type="application/ld+json"]').first
            data = json.loads(script.inner_text(timeout=3000))
            if isinstance(data, list):
                prod = next((x for x in data if isinstance(x, dict) and x.get("@type") in ("Product", "ItemList")), None) or {}
            else:
                prod = data
            agg = prod.get("aggregateRating") or {}
            r = agg.get("ratingValue")
            c = agg.get("reviewCount")
            if r is not None:
                r = float(str(r).replace(",", "."))
            if c is not None:
                c = int(str(c).replace(" ", ""))
            if r is not None or c is not None:
                meta = {"rating": r, "reviews": c}
                return meta
        except Exception:
            pass
        
        # Новый метод: парсинг HTML селекторов
        # Ищем количество отзывов в тексте типа " (5873 отзыва)"
        try:
            # Различные селекторы для отзывов
            review_selectors = [
                '[class*="rating"]',
                '[data-testid*="rating"]', 
                '.rating',
                '.reviews',
                '[class*="review"]',
                'span:has-text("отзыв")',
                'span:has-text("отзыва")',
                'span:has-text("отзывов")'
            ]
            
            reviews_count = None
            for selector in review_selectors:
                try:
                    elements = page.locator(selector).all()
                    for element in elements:
                        text = element.inner_text(timeout=1000).strip()
                        if "отзыв" in text.lower():
                            # Ищем число в скобках или рядом с "отзыв"
                            import re
                            numbers = re.findall(r'(\d+)\s*отзыв', text)
                            if not numbers:
                                numbers = re.findall(r'\((\d+)', text)
                            if not numbers:
                                numbers = re.findall(r'(\d+)', text)
                            if numbers:
                                reviews_count = int(numbers[0])
                                break
                except Exception:
                    continue
                if reviews_count:
                    break
            
            if reviews_count:
                meta["reviews"] = reviews_count
                
        except Exception:
            pass
        
        # Ищем рейтинг в различных местах
        try:
            rating_selectors = [
                '.rating-stars',
                '[data-rating]',
                '[class*="rating"][class*="stars"]',
                '[class*="star"]',
                'span[title*="из 5"]'
            ]
            
            rating_value = None
            for selector in review_selectors:
                try:
                    elements = page.locator(selector).all()
                    for element in elements:
                        # Пробуем получить рейтинг из атрибутов
                        rating_attr = element.get_attribute('data-rating')
                        if rating_attr:
                            rating_value = float(rating_attr)
                            break
                        
                        # Пробуем из title
                        title_attr = element.get_attribute('title')
                        if title_attr and "из 5" in title_attr:
                            import re
                            match = re.search(r'(\d+\.?\d*)\s*из\s*5', title_attr)
                            if match:
                                rating_value = float(match.group(1))
                                break
                                
                        # Пробуем из текста
                        text = element.inner_text(timeout=1000).strip()
                        if "из 5" in text.lower():
                            import re
                            match = re.search(r'(\d+\.?\d*)\s*из\s*5', text)
                            if match:
                                rating_value = float(match.group(1))
                                break
                except Exception:
                    continue
                if rating_value:
                    break
            
            if rating_value:
                meta["rating"] = rating_value
                
        except Exception:
            pass
            
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
            meta = _jsonld_rating_reviews(page)
            it.rating = meta["rating"]
            it.reviews = meta["reviews"]
            if it.list_price is None:
                try:
                    meta_price = page.locator('[itemprop="price"]').first
                    content_val = meta_price.get_attribute("content") if meta_price.count() else None
                    if content_val:
                        val = _regex_price_to_float(content_val)
                    else:
                        ptext = page.locator('[data-test="price"], [itemprop="price"], .price__value').first.inner_text(timeout=2500)
                        val = _regex_price_to_float(ptext)
                    it.list_price = val
                except Exception:
                    pass
            if it.list_price is not None:
                it.price_default = it.list_price
                it.price_min = it.list_price
            time.sleep(delay + random.uniform(0.05, 0.2))
        except Exception:
            it.errors = (it.errors + "; " if it.errors else "") + "detail_visit_fail"
    page.close()
    return items

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
    return bool(page.locator('nav a:has-text("2"), nav a[aria-label="Следующая"], nav a:has-text("Следующая")').count())

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
                       max_items:int, detail_limit:int, no_zone:bool, proxy: Optional[str] = "") -> List[Product]:
    items: List[Product] = []
    seen: Set[str] = set()

    with sync_playwright() as p:
        if proxy:
            browser = p.chromium.launch(headless=not headful, proxy={"server": proxy})
        else:
            browser = p.chromium.launch(headless=not headful)
        ctx = browser.new_context(**_ctx_opts())
        page = ctx.new_page()
        _ensure_dir("logs")

        # XHR capture state (с запросами)
        last_ep: Optional[CapturedEndpoint] = None
        next_hints: Dict[str, Optional[str]] = {}
        page_size_guess = 12

        def on_request_finished(req):
            nonlocal last_ep, next_hints, page_size_guess
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
                    next_hints = hints or next_hints
                    page_size_guess = max(page_size_guess, len(arr))
            except Exception:
                pass

        page.on("requestfinished", on_request_finished)

        # Исправлено: используем чистые категории без поиска
        if mode == "category":
            cat_url = f"https://kaspi.kz/shop/c/{category}/"
        else:
            cat_url = f"https://kaspi.kz/shop/c/smartphones/?text={quote(query_text)}"
        search_url = f"https://kaspi.kz/shop/search/?q={quote(query_text)}"

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
        for it in batch:
            key = it.product_id or f"{it.title}|{it.list_price or ''}"
            if key in seen:
                continue
            seen.add(key)
            items.append(it)

        grown = False
        if not _pagination_exists(page):
            # load more
            if any(page.locator(s).first.count() for s in ALL_LOAD_MORE_SELECTORS):
                logger.info("Пагинации нет — кликаем «Показать ещё».")
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
                    key = it.product_id or f"{it.title}|{it.list_price or ''}"
                    if key in seen:
                        continue
                    seen.add(key)
                    items.append(it)
                    added += 1
                logger.info("Дозагрузили карточек: +%s (итого: %s)", added, len(items))

        # XHR-пагинация (без UI)
        if last_ep and len(items) < max_items:
            logger.info("Пробуем XHR-пагинацию (без UI).")
            req = ctx.request
            ep = last_ep
            rounds_left = max(0, pages - 1)
            while rounds_left > 0 and len(items) < max_items:
                new_url = None
                new_body = None
                if ep.method.upper() == "GET":
                    bumped = _bump_query_params(ep.url, step=page_size_guess)
                    if bumped and no_zone:
                        bumped = _strip_zone_in_q(bumped)
                    new_url = bumped
                else:
                    new_body = _bump_graphql_body(ep.req_body or "", next_hints, step=page_size_guess)
                    if not new_body:
                        # последняя попытка — даже у POST попробуем query
                        new_url = _bump_query_params(ep.url, step=page_size_guess)
                        if new_url and no_zone:
                            new_url = _strip_zone_in_q(new_url)
                if not new_url and not new_body:
                    logger.info("Не удалось вывести параметры следующей страницы — стоп XHR.")
                    break

                try:
                    if ep.method.upper() == "GET":
                        r = req.get(new_url or ep.url, timeout=25000, headers=ep.req_headers)
                    else:
                        headers = dict(ep.req_headers)
                        headers.setdefault("content-type", "application/json")
                        r = req.post(new_url or ep.url, data=new_body or (ep.req_body or ""), headers=headers, timeout=30000)
                except Exception as e:
                    logger.info("XHR fetch упал: %s — стоп.", e)
                    break

                if not r.ok:
                    logger.info("XHR %s %s -> HTTP %s — стоп.", ep.method, new_url or "(same URL)", r.status)
                    break

                try:
                    data = r.json()
                except Exception:
                    logger.info("Ответ не JSON — стоп.")
                    break

                _save_dump("xhr_follow_req", {
                    "method": ep.method, "url": new_url or ep.url,
                    "body": new_body or ep.req_body or ""
                })
                _save_dump("xhr_follow_resp", {"url": new_url or ep.url, "data": data})

                arr, hints = _infer_array_and_next(data)
                if not arr:
                    logger.info("Ответ без массива карточек — стоп.")
                    break

                added = 0
                for obj in arr:
                    pid = str(obj.get("productId") or obj.get("id") or obj.get("product_id") or "") or None
                    title = obj.get("title") or obj.get("name") or ""
                    urlp = obj.get("url") or obj.get("href") or None
                    if urlp and urlp.startswith("/"):
                        urlp = "https://kaspi.kz" + urlp
                    price_fields = [obj.get("price"), obj.get("listPrice"), obj.get("minPrice"), obj.get("priceMin")]
                    lp = None
                    for pf in price_fields:
                        lp = _regex_price_to_float(pf)
                        if lp is not None:
                            break
                    if not title:
                        continue
                    it = Product(product_id=pid, title=title, url=urlp, list_price=lp)
                    key = it.product_id or f"{it.title}|{it.list_price or ''}"
                    if key in seen:
                        continue
                    seen.add(key)
                    items.append(it)
                    added += 1

                logger.info("XHR-пагинация: +%s карточек (итого %s)", added, len(items))
                if added == 0:
                    break

                rounds_left -= 1
                next_hints = hints or next_hints
                try:
                    if isinstance(hints, dict) and hints.get("has_next") is False:
                        logger.info("No further pages according to has_next hint.")
                        break
                except Exception:
                    pass
                if new_url or new_body:
                    ep = CapturedEndpoint(
                        method=ep.method,
                        url=(new_url or ep.url),
                        req_headers=ep.req_headers,
                        req_body=(new_body or ep.req_body),
                        resp_json=data
                    )
                time.sleep(delay + random.uniform(0.05, 0.2))

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
            no_zone:bool=True, proxy: Optional[str] = "") -> List[Product]:

    if not split_by_brand:
        return _collect_one_query(query_text, pages, delay, headful, mode, max_items, detail_limit, no_zone, proxy=proxy)

    brands = brands or DEFAULT_BRANDS
    all_items: List[Product] = []
    cap = max_items

    for b in brands:
        if len(all_items) >= cap:
            break
        q = f"{query_text} {b}"
        logger.info("=== Бренд '%s' ===", b)
        chunk = _collect_one_query(q, pages, delay, headful, mode, max_items=cap - len(all_items),
                                   detail_limit=detail_limit, no_zone=no_zone, proxy=proxy)
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
        proxy=args.proxy
    )
    save_csv(items, args.out)

if __name__ == "__main__":
    main()
