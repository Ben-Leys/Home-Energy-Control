# hec/core/app_initializer.py
import logging
from typing import Optional
from datetime import datetime, timedelta

from hec.core import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.data_sources import day_ahead_price_api
from hec.data_sources.p1_meter_homewizard import P1MeterHomeWizard
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.utils import convert_utc_price_points_to_local


logger = logging.getLogger(__name__)


def populate_app_state(db_handler: DatabaseHandler, app_config: dict):
    """Populate app state with necessary data from data sources"""

    # Price data from DB or api
    logger.info("Attempting to populate initial AppState with price data...")
    try:
        local_now = datetime.now().astimezone()  # Get timezone-aware current time
        local_tomorrow = local_now + timedelta(days=1)

        populate_price_data_in_appstate(db_handler, app_config, local_now, "electricity_prices_today",
                                        force_api_fetch_if_missing=True)
        populate_price_data_in_appstate(db_handler, app_config, local_tomorrow, "electricity_prices_tomorrow",
                                        force_api_fetch_if_missing=True)

        if not GLOBAL_APP_STATE.get("electricity_prices_today"):
            logger.warning("AppState 'electricity_prices_today' is empty after initial population attempt. "
                           "Price-based decisions will fail.")
            GLOBAL_APP_STATE.set("app_state", c.AppStatus.ALARM)
    except Exception as e:
        logger.error(f"Error during initial AppState population for prices: {e}", exc_info=True)


def initialize_database_handler(app_config: dict) -> Optional[DatabaseHandler]:
    try:
        db_handler = DatabaseHandler(app_config['database'])
        db_handler.initialize_database()
        return db_handler
    except KeyError:
        logger.critical("Database configuration missing in config.yaml. Exiting.")
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.ALARM)
        return None
    except Exception as e:
        logger.critical(f"Failed to initialize database: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.ALARM)
        return None


def initialize_p1_meter_client(app_config: dict):
    """Initializes and *returns* the P1MeterHomeWizard client instance."""

    logger.info("Initializing data source clients...")
    try:
        p1_config = app_config.get('p1_meter', {})
        p1_host = p1_config.get('host')

        if p1_host:
            p1_client_instance = P1MeterHomeWizard(host=p1_host)
        else:
            logger.warning("P1 meter host not configured. P1 data source will be disabled.")
            p1_client_instance = None

        if p1_client_instance and not p1_client_instance.is_initialized:
            logger.error("P1 meter client failed to initialize properly.")
            GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)
            p1_client_instance = None

    except Exception as e:
        logger.error(f"Error initializing P1 meter client: {e}", exc_info=True)
        p1_client_instance = None

    return p1_client_instance


def populate_price_data_in_appstate(db_handler: DatabaseHandler, target_day_local: datetime, app_config: dict,
                                    app_state_key: str, force_api_fetch_if_missing: bool = False):
    """
    Ensures price data for target_day_local is in AppState.
    Tries DB first. If missing and force_api_fetch_if_missing is True, tries API.
    """
    logger.info(f"Populating price data for AppState key '{app_state_key}' (date: {target_day_local.strftime('%Y-%m-%d')})")

    # Target_day_local is timezone-aware
    local_tz = target_day_local.tzinfo if target_day_local.tzinfo else datetime.now().astimezone().tzinfo
    target_day_local_aware = target_day_local.replace(tzinfo=local_tz)

    # Try to get from Database
    price_points_db = db_handler.get_da_prices(target_day_local_aware)

    if price_points_db:
        processed_prices = convert_utc_price_points_to_local(price_points_db, local_tz)
        GLOBAL_APP_STATE.set(app_state_key, processed_prices)
        logger.info(f"Loaded {len(processed_prices)} price intervals for '{app_state_key}' from DB into AppState.")
        return True  # Data loaded from DB

    logger.info(f"No prices for '{app_state_key}' ({target_day_local_aware.date()}) found in DB.")

    # If missing in DB and force_api_fetch_if_missing is True, try API
    if force_api_fetch_if_missing:
        logger.info(f"Attempting API fetch for {target_day_local_aware.date()} for AppState key '{app_state_key}'.")

        price_points = day_ahead_price_api.fetch_entsoe_prices(target_day_local_aware, app_config)

        if price_points:  # API returned some data (could be empty list if not published)
            logger.debug(f"API fetch returned {len(price_points)} price points for {target_day_local_aware.date()}.")
            if len(price_points):  # Actually got price data
                db_handler.store_da_prices(price_points)  # Store it in DB
                processed_prices = convert_utc_price_points_to_local(price_points, local_tz)
                GLOBAL_APP_STATE.set(app_state_key, processed_prices)
                logger.debug(f"Loaded {len(processed_prices)} price points for '{app_state_key}' into AppState.")
                return True
            else:  # API returned empty list (data not published yet for that day)
                logger.debug(f"API fetch for {target_day_local_aware.date()} returned no data (not published yet?)")
                GLOBAL_APP_STATE.set(app_state_key, [])
                return False
        else:  # API call failed critically
            logger.error(f"Critical API fetch error for {target_day_local_aware.date()}.")
            GLOBAL_APP_STATE.set(app_state_key, [])
            return False
    else:
        # Not forcing API fetch, and not found in DB
        GLOBAL_APP_STATE.set(app_state_key, [])
        return False
