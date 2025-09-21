from __future__ import annotations

from typing import List
from .utils import jitter_delay, robust


@robust()
def scrape_listings(category: str, pages: int = 1) -> List[str]:
    """Stub: return a few product_ids for demo."""
    jitter_delay()
    return ["102298404", "113137790", "114695323"][: max(1, min(3, pages * 3))]

