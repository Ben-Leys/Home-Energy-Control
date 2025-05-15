from datetime import datetime


class PricePoint:
    def __init__(self, timestamp_utc: datetime, price_eur_per_mwh: float, position: int, resolution_minutes: int):
        self.timestamp_utc = timestamp_utc  # Start of the interval (UTC)
        self.price_eur_per_mwh = price_eur_per_mwh
        self.position = position  # Original position from API (1-based)
        self.resolution_minutes = resolution_minutes

    def __repr__(self):
        return (f"PricePoint(ts: '{self.timestamp_utc.isoformat()} UTC', price: {self.price_eur_per_mwh} €/MWh, "
                f"pos: {self.position}, res: {self.resolution_minutes} min)")
