from typing import Dict, Optional

from hec.core.app_logging import sync_app_status_from_incidents
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.notifications import NotificationDispatcher


def record_peak_consumption_incident(
    db_handler,
    avg_kw: Dict[str, float],
    limit_kw: float,
    notification_type: str = "peak_consumption",
):
    if not db_handler:
        return None

    message = (
        "Peak consumption threshold exceeded: "
        f"5m={avg_kw.get('5m', 0):.2f} kW, "
        f"10m={avg_kw.get('10m', 0):.2f} kW, "
        f"15m={avg_kw.get('15m', 0):.2f} kW, "
        f"limit={limit_kw:.2f} kW"
    )
    incident = db_handler.record_incident(
        severity="warning",
        source="peak_consumption",
        message=message,
        notification_type=notification_type,
        fingerprint_key=f"peak_consumption:{limit_kw:.2f}",
    )
    NotificationDispatcher(db_handler).dispatch_incident(
        incident,
        notification_type=notification_type,
        title="Peak consumption",
        message=message,
    )
    sync_app_status_from_incidents(GLOBAL_APP_STATE, db_handler)
    return incident
