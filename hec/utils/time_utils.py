from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


DEFAULT_TIMEZONE = "Europe/Brussels"


def get_zone(timezone_name: str | ZoneInfo | None = None) -> ZoneInfo:
    if isinstance(timezone_name, ZoneInfo):
        return timezone_name
    return ZoneInfo(timezone_name or DEFAULT_TIMEZONE)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def local_now(timezone_name: str | ZoneInfo | None = None) -> datetime:
    return utc_now().astimezone(get_zone(timezone_name))


def ensure_aware(value: datetime, timezone_name: str | ZoneInfo | None = None) -> datetime:
    if value.tzinfo is not None:
        return value
    return value.replace(tzinfo=get_zone(timezone_name))


def to_utc(value: datetime, timezone_name: str | ZoneInfo | None = None) -> datetime:
    return ensure_aware(value, timezone_name).astimezone(timezone.utc)


def local_day_bounds(target_day: date | datetime, timezone_name: str | ZoneInfo | None = None) -> tuple[datetime, datetime]:
    zone = get_zone(timezone_name)
    target_date = target_day.date() if isinstance(target_day, datetime) else target_day
    start_local = datetime.combine(target_date, time.min, tzinfo=zone)
    end_local = datetime.combine(target_date + timedelta(days=1), time.min, tzinfo=zone)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)
