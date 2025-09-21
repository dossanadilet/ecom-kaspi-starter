from __future__ import annotations

from typing import Dict, Any
from .utils import jitter_delay, robust


@robust()
def scrape_reviews(product_id: str) -> Dict[str, Any]:
    jitter_delay()
    return {"product_id": product_id, "reviews": 120, "rating": 4.6}

