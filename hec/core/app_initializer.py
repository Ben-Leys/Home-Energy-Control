# hec/core/app_initializer.py
import logging
from typing import Optional
from datetime import datetime, timedelta

from hec import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine import scheduled_tasks # For ensure_daily_price_data or similar


logger = logging.getLogger(__name__)


def initialize_application_state(db_handler: DatabaseHandler, app_config: dict):
    logger.info("Attempting to populate initial AppState with price data...")
    try:
        local_now = datetime.now().astimezone()  # Get timezone-aware current time
        local_tomorrow = local_now + timedelta(days=1)

        scheduled_tasks.populate_price_data_in_appstate(db_handler, local_now, "electricity_prices_today",
                                                        force_api_fetch_if_missing=True)
        scheduled_tasks.populate_price_data_in_appstate(db_handler, local_tomorrow, "electricity_prices_tomorrow",
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
