import logging
from datetime import datetime, timedelta, time

from hec.core.app_state import GLOBAL_APP_STATE
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.utils import process_price_points_to_app_state

logger = logging.getLogger(__name__)


def populate_appstate_with_price_data(db_handler: DatabaseHandler, app_config: dict,
                                      force_api_fetch_if_missing: bool = False):
    """
    Ensures price data for today and tomorrow is in AppState.
    Tries DB first. If missing and force_api_fetch_if_missing is True, tries API.
    """
    logger.info(f"Populating price data for AppState")

    # Target day is timezone-aware
    local_now = datetime.now().astimezone()
    local_tomorrow = local_now + timedelta(days=1)

    for day, key in [(local_now, "electricity_prices_today"),
                     (local_tomorrow, "electricity_prices_tomorrow")]:
        # Try to get from database
        price_points = db_handler.get_da_prices(day)

        # If DB is empty and API fetching is allowed, fetch from API
        store_to_db = False
        if not price_points and force_api_fetch_if_missing:
            logger.info(f"No DB data for '{key}' on {day.date()}, attempting API fetch.")
            price_points = api_entsoe_day_ahead_price.fetch_entsoe_prices(day, app_config)
            store_to_db = True if price_points else False

        # Process the price points (if any)
        process_price_points_to_app_state(price_points, day, key, app_config,
                                          db_handler if store_to_db else None)

    if not GLOBAL_APP_STATE.get("electricity_prices_today"):
        logger.warning("No 'electricity_prices_today' found in AppState. Price-based decisions will fail.")


def populate_appstate_with_forecast_data(db_handler: DatabaseHandler):
    """Loads forecast data for target_day_local."""

    local_now = datetime.combine(datetime.now().astimezone(), time.min)

    logger.info("Populating AppState with forecast data...")
    forecast_days = {"wind": 5, "solar": 5, "grid_load": 4}
    forecasts = {
        f_type: db_handler.get_elia_forecasts(f_type, local_now, local_now + timedelta(days=days))
        for f_type, days in forecast_days.items()
    }
    GLOBAL_APP_STATE.set("forecasts", forecasts)
