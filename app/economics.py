from dataclasses import dataclass

# ======== Unit economics ========

@dataclass
class CostInputs:
    purchase_cn: float  # закупка в тг/шт (эквивалент)
    intl_ship: float    # международная доставка на шт
    customs: float      # пошлина/НДС/оформление на шт
    last_mile: float    # внутренняя логистика/фулфилмент на шт
    pack: float         # упаковка
    return_rate: float  # доля возвратов, например 0.03
    mp_fee: float       # комиссия маркетплейса (доля), например 0.10
    ads_alloc: float    # распределённая реклама на шт
    overhead: float     # накладные на шт

def landed_cost(c: CostInputs) -> float:
    base = (c.purchase_cn + c.intl_ship + c.customs +
            c.last_mile + c.pack + c.ads_alloc + c.overhead)
    return base * (1 + c.return_rate)

def min_price_for_margin(c_land: float, target_margin: float = 0.2) -> float:
    return c_land / (1 - target_margin)

def profit_per_unit(price: float, c_land: float, mp_fee: float) -> float:
    return (price * (1 - mp_fee)) - c_land

def roi_on_turnover(price: float, c_land: float, mp_fee: float) -> float:
    rev = price
    cost = c_land + price * mp_fee
    profit = rev - cost
    return profit / rev if rev else 0.0

# ======== Inventory math: ROP / Safety Stock / EOQ ========

def z_value_for_service(level: float) -> float:
    """
    0.90 -> 1.28, 0.95 -> 1.65, 0.97 -> 1.88, 0.98 -> 2.05, 0.99 -> 2.33
    """
    table = {0.90: 1.28, 0.95: 1.65, 0.97: 1.88, 0.98: 2.05, 0.99: 2.33}
    closest = min(table.keys(), key=lambda k: abs(k - level))
    return table[closest]

def weekly_to_period(weekly_mean: float, weeks: float) -> float:
    return max(0.0, weekly_mean * weeks)

def safety_stock(weekly_sigma: float, lead_time_days: int, review_days: int, service_level: float = 0.95) -> float:
    z = z_value_for_service(service_level)
    weeks = max(0.0, (lead_time_days + review_days) / 7.0)
    return max(0.0, z * weekly_sigma * (weeks ** 0.5))

def reorder_point(weekly_mean: float, weekly_sigma: float, lead_time_days: int, review_days: int, service_level: float = 0.95) -> float:
    weeks = max(0.0, (lead_time_days + review_days) / 7.0)
    demand_period = weekly_to_period(weekly_mean, weeks)
    ss = safety_stock(weekly_sigma, lead_time_days, review_days, service_level)
    return demand_period + ss

def eoq(annual_demand_units: float, setup_cost_per_order: float, annual_holding_cost_per_unit: float) -> float:
    if annual_demand_units <= 0 or setup_cost_per_order <= 0 or annual_holding_cost_per_unit <= 0:
        return 0.0
    from math import sqrt
    return sqrt((2.0 * annual_demand_units * setup_cost_per_order) / annual_holding_cost_per_unit)
