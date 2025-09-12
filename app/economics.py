from dataclasses import dataclass

@dataclass
class CostInputs:
    purchase_cn: float  # закупка в тг за шт (эквивалент)
    intl_ship: float    # международная доставка на шт
    customs: float      # пошлина/НДС/оформление на шт
    last_mile: float    # внутренняя логистика/фулфилмент на шт
    pack: float         # упаковка
    return_rate: float  # доля возвратов, например 0.03
    mp_fee: float       # комиссия маркетплейса, например 0.10
    ads_alloc: float    # рекламные расходы в расчете на шт (распределенные)
    overhead: float     # общие накладные на шт (аренда/ПО/зарплаты частично)

def landed_cost(c: CostInputs) -> float:
    """Полная себестоимость единицы с учетом возвратов и накладных."""
    base = (c.purchase_cn + c.intl_ship + c.customs +
            c.last_mile + c.pack + c.ads_alloc + c.overhead)
    return base * (1 + c.return_rate)

def min_price_for_margin(c_land: float, target_margin: float = 0.2) -> float:
    """Минимальная цена при заданной целевой марже (без учета комиссии MP)."""
    return c_land / (1 - target_margin)

def profit_per_unit(price: float, c_land: float, mp_fee: float) -> float:
    """Прибыль на единицу с учетом комиссии маркетплейса."""
    return (price * (1 - mp_fee)) - c_land

def roi_on_turnover(price: float, c_land: float, mp_fee: float) -> float:
    """ROI на оборот = (выручка - затраты) / выручка."""
    rev = price
    cost = c_land + price * mp_fee
    profit = rev - cost
    return profit / rev if rev else 0.0
