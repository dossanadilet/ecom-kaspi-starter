from __future__ import annotations

from typing import Dict, Any
from .utils import jitter_delay, robust


@robust()
def scrape_offers(product_id: str) -> Dict[str, Any]:
    jitter_delay()
    return {"product_id": product_id, "price_min": 270000, "price_default": 275000}

