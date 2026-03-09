import json
from dataclasses import dataclass, asdict, field, fields
from datetime import datetime, timezone
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
        """Returns a JSON-like string representation."""
        # Convert to a dictionary and use `json.dumps` for consistent formatting
        as_dict = self.to_dict()
        return json.dumps(as_dict, indent=None, separators=(',', ':'))

    # def __repr__(self) -> str:
    #     prices = {
    #         contract_type: {key: round(value, 6) for key, value in prices.items() if value is not None}
    #         for contract_type, prices in (self.net_prices_eur_per_kwh or {}).items()
    #     }
    #     return (
    #         '{"NEPI": {'
    #         f'"start": "{self.interval_start_local.isoformat()}", '
    #         f'"minutes": {self.resolution_minutes}, '
    #         f'"contract": "{self.active_contract_type}", '
    #         f'"prices": {prices}'
    #         '}}'
    #     )


@dataclass(frozen=True)
class EVCCOverallState:
    timestamp_utc_iso: str = datetime.now(timezone.utc).isoformat()
    residual_power: int = 0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'EVCCOverallState':
        field_names = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in d.items() if k in field_names}
        return cls(**filtered_data)


@dataclass(frozen=True)
class EVCCLoadpointState:
    loadpoint_id: int = 1
    is_connected: bool = False
    is_charging: bool = False
    charge_current: int = 0
    min_current: int = 6
    max_current: int = 32
    enable_threshold: int = -1200
    disable_threshold: int = 0
    limit_energy: int = 0
    mode: str = 'off'
    session_energy: int = 0
    smart_cost_active: bool = False
    plan_active: bool = False

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'EVCCLoadpointState':
        field_names = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in d.items() if k in field_names}
        return cls(**filtered_data)
