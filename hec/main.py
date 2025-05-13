import logging
import os
import time

from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from hec.core.config_loader import load_app_config
from hec.core.logging_setup import start_logger
from hec.core.app_state import GLOBAL_APP_STATE
from hec.data_sources.day_ahead_price_api import fetch_entsoe_prices
from hec.database_ops.db_handler import DatabaseHandler
from hec.core.scheduler_setup import setup_scheduler
from hec.logic_engine import scheduled_tasks
from hec import constants


try:
    APP_CONFIG = load_app_config()
except FileNotFoundError as e:
    print(f"CRITICAL: Configuration file not found. {e}. Exiting.")
    exit(1)
except ValueError as e:
    print(f"CRITICAL: Error parsing configuration file. {e}. Exiting.")
    exit(1)

start_logger(APP_CONFIG)
logger = logging.getLogger(__name__)
logger.info("*************************************************")
logger.info("*** Starting Home Energy Control Application  ***")
logger.info("*************************************************")


def run_application():
    logger.info("Application run_application() sequence started.")

    logger.debug(f"Initial AppState: {GLOBAL_APP_STATE.get_all()}")
    GLOBAL_APP_STATE.set("app_state", constants.AppStatus.STARTING)

    # Setup database
    try:
        db_handler = DatabaseHandler(APP_CONFIG['database'])
        db_handler.initialize_database()
        logger.info("Database handler initialized and tables checked or created.")
    except KeyError:
        logger.critical("Database configuration missing in config.yaml. Exiting.")
        GLOBAL_APP_STATE.set("app_state", constants.AppStatus.ALARM)
        return
    except Exception as e:
        logger.critical(f"Failed to initialize database: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", constants.AppStatus.ALARM)
        return

    # Populate initial AppState from DB or API
    logger.info("Attempting to populate initial AppState with price data...")
    try:
        local_now_aware = datetime.now().astimezone()  # Get timezone-aware current time
        today_local_start = local_now_aware.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_local_start = (local_now_aware + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        scheduled_tasks.populate_price_data_in_appstate(db_handler, today_local_start, "electricity_prices_today",
                                                        force_api_fetch_if_missing=True)
        scheduled_tasks.populate_price_data_in_appstate(db_handler, tomorrow_local_start, "electricity_prices_tomorrow",
                                                        force_api_fetch_if_missing=True)

        if not GLOBAL_APP_STATE.get("electricity_prices_today"):
            logger.warning("AppState 'electricity_prices_today' is empty after initial population attempt. "
                           "Price-based decisions will fail.")
            GLOBAL_APP_STATE.set("app_state", constants.AppStatus.DEGRADED)
    except Exception as e:
        logger.error(f"Error during initial AppState population for prices: {e}", exc_info=True)

    # Set up the scheduler
    run_scheduler_in_background = APP_CONFIG.get('scheduler', {}).get('run_in_background', False)
    scheduler = setup_scheduler(APP_CONFIG, run_in_background=run_scheduler_in_background)

    # Add jobs to the scheduler
    logger.info("Registering scheduled jobs...")
    try:
        scheduled_tasks.register_all_jobs(scheduler, db_handler, APP_CONFIG)
    except Exception as e:
        logger.critical(f"Failed to register scheduled jobs: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("application_status", constants.AppStatus.ALARM)
        db_handler.close_connection()  # Clean up
        return

    # Start the scheduler
    logger.info("Starting scheduler...")
    GLOBAL_APP_STATE.set("app_state", constants.AppStatus.NORMAL)
    try:
        scheduler.start()
        if run_scheduler_in_background:
            logger.info("BackgroundScheduler started. Main thread will keep alive.")
            while True:
                time.sleep(3600)  # Sleep for a long time, scheduler runs in background
    except (KeyboardInterrupt, SystemExit):
        logger.info("Application interrupt received. Shutting down...")
        GLOBAL_APP_STATE.set("app_state", constants.AppStatus.SHUTDOWN)
    except Exception as e:
        logger.critical(f"An critical error occurred with the scheduler or main loop: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", constants.AppStatus.ALARM)
    finally:
        if scheduler and scheduler.running:
            logger.info("Shutting down scheduler...")
            scheduler.shutdown()
        if db_handler:
            db_handler.close_connection()
        logger.info("Application shut down gracefully.")
        logging.shutdown()


if __name__ == "__main__":
    run_application()
