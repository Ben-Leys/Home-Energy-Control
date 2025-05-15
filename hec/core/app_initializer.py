# hec/core/app_initializer.py
import logging
from typing import Optional
from datetime import datetime, timedelta

from hec.core import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.logic_engine.scheduled_tasks import populate_price_data_in_appstate
from hec.data_sources.p1_meter_homewizard import P1MeterHomeWizard
from hec.database_ops.db_handler import DatabaseHandler

logger = logging.getLogger(__name__)


def populate_app_state(db_handler: DatabaseHandler, app_config: dict):
    """Populate app state with necessary data from data sources"""

    # Price data from DB or api
    logger.info("Attempting to populate initial AppState with price data...")
    try:
        local_now = datetime.now().astimezone()  # Get timezone-aware current time
        local_tomorrow = local_now + timedelta(days=1)

        populate_price_data_in_appstate(db_handler, local_now, app_config, "electricity_prices_today",
                                        force_api_fetch_if_missing=True)
        populate_price_data_in_appstate(db_handler, local_tomorrow, app_config, "electricity_prices_tomorrow",
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
