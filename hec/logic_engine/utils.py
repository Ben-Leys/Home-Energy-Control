# hec/logic_engine/utils.py
import logging

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

import hec.models.models
from hec.core.app_state import GLOBAL_APP_STATE
from hec.database_ops.db_handler import DatabaseHandler

logger = logging.getLogger(__name__)


def convert_utc_price_points_to_local(
        utc_price_points: list[hec.models.models.PricePoint], local_tz) -> list[dict]:
    """
    Converts a list of UTC PricePoint objects to a list of dictionaries,
    each representing a price interval with its local start time and other details.
    Iterating through a list of dicts to find the current price should be less complex than truncating local time,
    convert to a string, to find the key that could possibly be a changing resolution time in a dict.
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
            pass  # Continue to next interval if current one is malformed

    logger.warning(f"No current interval found for {now_local.isoformat()} in provided list.")
    return None


def parse_hh_mm_time_string(time_str: str) -> Optional[Tuple[int, int]]:
    """
    Parses a time string in "HH:MM" format and returns the hour and minute as integers.

    Args:
        time_str (str): The time string to parse (e.g., "13:05", "08:30").

    Returns:
        Optional[Tuple[int, int]]: A tuple (hour, minute) if parsing is successful,
                                     None otherwise.
    """
    if not isinstance(time_str, str):
        logger.error(f"Invalid input type for time string: expected str, got {type(time_str)}")
        return None

    parts = time_str.split(':')
    if len(parts) != 2:
        logger.error(f"Invalid time string format: '{time_str}'. Expected HH:MM.")
        return None

    try:
        hour = int(parts[0])
        minute = int(parts[1])

        if not (0 <= hour <= 23):
            logger.error(f"Invalid hour value in time string: '{time_str}'. Hour must be 0-23.")
            return None

        if not (0 <= minute <= 59):
            logger.error(f"Invalid minute value in time string: '{time_str}'. Minute must be 0-59.")
            return None

        return hour, minute
    except ValueError:
        logger.error(f"Could not parse hour or minute as integer from time string: '{time_str}'.")
        return None


def process_price_points_to_app_state(price_points: list, target_day: datetime,
                                      app_state_key: str, db_handler: DatabaseHandler = None):
    """
    Processes price points by storing them in the database in raw format and updating the AppState with net prices.

    Args:
        price_points (list): List of price points retrieved from the API.
        target_day (datetime): The target day for the price points (timezone-aware).
        app_state_key (str): The key under which to store the processed price points in the AppState.
        db_handler (DatabaseHandler): Database handler for storing the price points if necessary.

    Returns:
        True in case of success, False in case of failure.
    """
    if price_points is None:
        logger.error(f"Critical API fetch error for {target_day.date()}.")
        GLOBAL_APP_STATE.set(app_state_key, [])
        return False

    elif not price_points:
        logger.debug(f"No price points available for {target_day.date()} (API data not yet published).")
        GLOBAL_APP_STATE.set(app_state_key, [])
        return False

    logger.info(f"Processing {len(price_points)} price points for {target_day.date()}.")

    # Store raw price points in the database
    if db_handler:
        db_handler.store_da_prices(price_points)
        logger.debug(f"Stored {len(price_points)} price points in the database.")

    # Convert and process price points for the AppState
    local_tz = target_day.tzinfo if target_day.tzinfo else datetime.now().astimezone().tzinfo
    processed_prices = convert_utc_price_points_to_local(price_points, local_tz)

    # Update AppState
    GLOBAL_APP_STATE.set(app_state_key, processed_prices)
    logger.info(f"Updated AppState with {len(processed_prices)} price points for '{app_state_key}'.")

    return True
