from __future__ import annotations

from typing import Dict, Any
from .utils import jitter_delay, robust


@robust()
def scrape_product_page(product_id: str) -> Dict[str, Any]:
    jitter_delay()
    return {"product_id": product_id, "title": f"Product-{product_id}", "rating": 4.5}

