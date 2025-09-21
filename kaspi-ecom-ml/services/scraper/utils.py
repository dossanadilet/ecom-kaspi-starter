from __future__ import annotations

import random, time
from tenacity import retry, stop_after_attempt, wait_fixed


def jitter_delay(base: float = 0.5, spread: float = 0.3) -> None:
    time.sleep(max(0.0, base + random.uniform(-spread, spread)))


def robust(retries: int = 3, wait: float = 0.5):
    return retry(stop=stop_after_attempt(retries), wait=wait_fixed(wait))

