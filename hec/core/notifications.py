import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class NotificationDispatcher:
    """Queues user notifications through the configured lightweight providers."""

    def __init__(self, db_handler):
        self.db_handler = db_handler

    def dispatch_incident(
        self,
        incident: Dict[str, Any],
        notification_type: Optional[str] = None,
        title: Optional[str] = None,
        message: Optional[str] = None,
    ) -> int:
        if not self.db_handler or not incident or not incident.get("should_notify"):
            return 0

        final_type = notification_type or incident.get("notification_type") or incident.get("severity")
        final_title = title or self._title_for_incident(incident, final_type)
        final_message = message or incident.get("message") or ""
        dedupe_key = f"incident:{incident.get('id')}:{incident.get('occurrence_count')}:{final_type}"

        try:
            return self.db_handler.queue_notification(
                notification_type=final_type,
                title=final_title,
                message=final_message,
                incident_id=incident.get("id"),
                dedupe_key=dedupe_key,
            )
        except Exception:
            logger.debug("Failed to queue incident notification.", exc_info=True)
            return 0

    @staticmethod
    def _title_for_incident(incident: Dict[str, Any], notification_type: Optional[str]) -> str:
        if notification_type == "peak_consumption":
            return "Peak consumption"
        severity = str(incident.get("severity") or "warning").capitalize()
        source = incident.get("source") or "application"
        return f"{severity}: {source}"
