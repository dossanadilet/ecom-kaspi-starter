from __future__ import annotations

from pydantic import BaseModel


class PriceReco(BaseModel):
    sku: str
    reco_price: float
    expected_qty: float
    expected_profit: float
    explain: str
    model_ver: str

