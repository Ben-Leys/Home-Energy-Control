# hec/logic_engine/utils.py
import logging

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

from hec.core.app_state import GLOBAL_APP_STATE
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.cost_calculator import calculate_net_intervals_for_day
from hec.models.models import NetElectricityPriceInterval

logger = logging.getLogger(__name__)


def get_interval_from_list(target_local: datetime, intervals: List[NetElectricityPriceInterval]) \
        -> Optional[NetElectricityPriceInterval]:
    """
    Finds the active NetElectricityPriceInterval for the given 'target_local' datetime.

    Args:
        target_local: The datetime for which the active interval is being sought.
        intervals: A list of NetElectricityPriceInterval objects.

    Returns:
        The active NetElectricityPriceInterval if found, otherwise None.
    """
    for interval in intervals:
        try:
            interval_start = interval.interval_start_local

            # Ensure timezone alignment between target_local and interval_start
            if target_local.tzinfo and interval_start.tzinfo is None:
                interval_start = interval_start.replace(tzinfo=target_local.tzinfo)

            interval_end = interval_start + timedelta(minutes=interval.resolution_minutes)

            if interval_start <= target_local < interval_end:
                return interval
        except Exception as e:
            logger.error(f"Error processing interval: {interval}. Error: {e}", exc_info=True)

    logger.warning(f"No active interval found for {target_local.isoformat()} in the provided list.")
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
                                      app_state_key: str, app_config, db_handler: DatabaseHandler = None):
    """
    Processes price points by storing them in the database in raw format and updating the AppState with net prices.

    Args:
        price_points (list): List of price points retrieved from the API.
        target_day (datetime): The target day for the price points (timezone-aware).
        app_state_key (str): The key under which to store the processed price points in the AppState.
        app_config: Dict with application configuration data.
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
    nepis = calculate_net_intervals_for_day(db_handler, app_config, target_day, price_points)

    # Update AppState
    GLOBAL_APP_STATE.set(app_state_key, nepis)
    logger.info(f"Updated AppState with {len(nepis)} price points for '{app_state_key}'.")

    return True
