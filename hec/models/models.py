from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass(frozen=True)
class PricePoint:
    """Contains raw price point information from API sources"""
    timestamp_utc: datetime
    price_eur_per_mwh: float
    position: int
    resolution_minutes: int


@dataclass(frozen=True, order=True)
class NetElectricityPriceInterval:
    """Contains net electricity prices for various contract types at a time interval"""
    interval_start_local: datetime = field(init=True, repr=True)
    resolution_minutes: int
    active_contract_type: str  # "dynamic", "fixed"
    net_buy_price_eur_per_kwh: float
    net_sell_price_eur_per_kwh: float
    comparison_prices_eur_per_kwh: Optional[Dict[str, Dict[str, Optional[float]]]] = None  # Other types and prices

    def to_dict(self) -> Dict[str, Any]:  # Helper to store in AppState
        d = asdict(self)
        d['interval_start_local'] = self.interval_start_local.isoformat()
        return d
