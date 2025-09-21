from __future__ import annotations

from datetime import date


def yesterday() -> date:
    from datetime import timedelta

    return date.today() - timedelta(days=1)

