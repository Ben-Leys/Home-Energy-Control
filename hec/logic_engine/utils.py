# hec/logic_engine/utils.py
import logging

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from hec import constants
from hec.data_sources import day_ahead_price_api
from hec.core.app_state import GLOBAL_APP_STATE


logger = logging.getLogger(__name__)


def convert_utc_price_points_to_local_intervals(
        utc_price_points: list[day_ahead_price_api.PricePoint], local_tz) -> list[dict]:
    """
    Converts a list of UTC PricePoint objects to a list of dictionaries,
    each representing a price interval with its local start time and other details.
    Iterating through a list of dicts should be less complex than truncating local time converted to a string
    to find the key that could possibly be a changing resolution time in a dict.
    """
    if not utc_price_points:
        return []

    local_interval_prices = []
    for pp in utc_price_points:
        # Convert the UTC timestamp of the price point to local time
        interval_start_local = pp.timestamp_utc.astimezone(local_tz)

        local_interval_prices.append({
            "interval_start_local": interval_start_local.isoformat(),  # ISO format string with TZ offset
            "price_eur_per_mwh": pp.price_eur_per_mwh,
            "price_eur_per_kwh": pp.price_eur_per_mwh / 1000.0 if pp.price_eur_per_mwh is not None else None,
            "resolution_minutes": pp.resolution_minutes,
        })

    logger.debug(f"Converted {len(utc_price_points)} UTC price points to {len(local_interval_prices)} local intervals.")
    return local_interval_prices


def get_current_interval_price_data(now_local: datetime, daily_intervals: Optional[List[Dict[str, Any]]]) \
        -> Optional[Dict[str, Any]]:
    """
    Finds the price data for the interval that 'now_local' falls into.
    'daily_intervals' is a list of dicts, each with 'interval_start_local' (ISO string)
    and 'resolution_minutes'.
    """
    if not daily_intervals:
        return None

    for interval_data in daily_intervals:
        try:
            interval_start = datetime.fromisoformat(interval_data["interval_start_local"])
            # Interval_start is timezone-aware because 'now_local' is too
            if now_local.tzinfo and interval_start.tzinfo is None:
                interval_start = interval_start.replace(tzinfo=now_local.tzinfo)

            resolution = interval_data.get("resolution_minutes")
            if resolution is None:
                logger.warning("Interval data missing 'resolution_minutes'. Skipping.")
                continue

            interval_end = interval_start + timedelta(minutes=resolution)

            if interval_start <= now_local < interval_end:
                return interval_data
        except Exception as e:
            logger.error(f"Error processing interval data: {interval_data}. Error: {e}", exc_info=True)
            GLOBAL_APP_STATE.set("app_state", constants.AppStatus.ALARM)
            pass  # Continue to next interval if current one is malformed

    logger.warning(f"No current interval found for {now_local.isoformat()} in provided list.")
    return None
