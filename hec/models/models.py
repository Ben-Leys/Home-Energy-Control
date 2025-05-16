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

    def __repr__(self) -> str:
        return (
            f"PricePoint: (timestamp_utc = {self.timestamp_utc.strftime("%Y-%m-%d %H:%M")}, "
            f"price_eur_per_mwh = {self.price_eur_per_mwh:.2f}, "
            f"minutes = {self.resolution_minutes})"
        )


@dataclass(frozen=True, order=True)
class NetElectricityPriceInterval:
    """Contains net electricity prices for various contract types at a time interval"""
    interval_start_local: datetime = field(init=True, repr=True)
    resolution_minutes: int
    active_contract_type: str  # "dynamic", "fixed"
    net_prices_eur_per_kwh: Optional[Dict[str, Dict[str, Optional[float]]]] = None  # Other types and prices

    def to_dict(self) -> Dict[str, Any]:  # Helper to store in AppState
        d = asdict(self)
        d['interval_start_local'] = self.interval_start_local.isoformat()
        return d

    def __repr__(self) -> str:
        prices = {
            contract_type: {key: round(value, 6) for key, value in prices.items() if value is not None}
            for contract_type, prices in (self.net_prices_eur_per_kwh or {}).items()
        }
        return (
            "NEPI: {"
            f"start: {self.interval_start_local.isoformat()}, "
            f"minutes: {self.resolution_minutes}, "
            f"contract: '{self.active_contract_type}', "
            f"prices: {prices}"
            "}"
        )
