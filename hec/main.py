import logging
import time
from threading import Thread

from hec.core import constants as c
from hec.core.api_setup import run_api_server
from hec.core.app_initializer import (populate_app_state, initialize_database_handler,
                                      initialize_p1_meter_client)
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.config_loader import load_app_config
from hec.core.logging_setup import start_logger
from hec.core.scheduler_setup import setup_scheduler
from hec.core.tariff_manager import initialize_tariff_manager
from hec.logic_engine import scheduled_tasks

try:
    APP_CONFIG = load_app_config()
except FileNotFoundError as e:
    print(f"CRITICAL: Configuration file not found. {e}. Exiting.")
    exit(1)
except ValueError as e:
    print(f"CRITICAL: Error parsing configuration file. {e}. Exiting.")
    exit(1)

start_logger(APP_CONFIG, GLOBAL_APP_STATE)
logger = logging.getLogger(__name__)
logger.info("*************************************************")
logger.info("*** Starting Home Energy Control Application  ***")
logger.info("*************************************************")


def run_application():
    logger.debug(f"Initial AppState: {GLOBAL_APP_STATE.get_all()}")
    GLOBAL_APP_STATE.set("app_state", c.AppStatus.STARTING)

    # --- LOAD DATA ---
    tariff_manager = initialize_tariff_manager(APP_CONFIG)

    # --- SETUP DATABASE ---
    db_handler = initialize_database_handler(APP_CONFIG)

    # --- INITIALIZE DATA SOURCES ---
    p1_meter_client = initialize_p1_meter_client(APP_CONFIG)

    # --- POPULATE APP STATE ---
    populate_app_state(db_handler, APP_CONFIG)

    # --- START API SERVER ---
    api_thread = None
    if APP_CONFIG.get('api_server', {}).get('enabled', True):
        api_thread = Thread(target=run_api_server, args=(APP_CONFIG,), daemon=True)
        api_thread.start()
    else:
        logger.info("API server is disabled in configuration.")

    # Continue if no ALARM
    if GLOBAL_APP_STATE.get("app_state") == c.AppStatus.ALARM:
        logger.critical("Application initialization failed. Exiting.")
        if db_handler:
            db_handler.close_connection()
        return

    # --- SET UP SCHEDULER ---
    run_scheduler_in_background = APP_CONFIG.get('scheduler', {}).get('run_in_background', True)
    scheduler = setup_scheduler(APP_CONFIG, run_in_background=run_scheduler_in_background)
    scheduled_tasks.register_all_jobs(scheduler, db_handler, APP_CONFIG, p1_meter_client)

    logger.info("Starting scheduler...")
    try:
        scheduler.start()
        if run_scheduler_in_background:
            if GLOBAL_APP_STATE.get("app_state") == c.AppStatus.STARTING:
                GLOBAL_APP_STATE.set("app_state", c.AppStatus.NORMAL)
            logger.info("BackgroundScheduler started. Main thread will wait for API thread or interrupt.")
            if api_thread:
                api_thread.join()
            else:
                while True:
                    time.sleep(3600)  # Sleep, scheduler runs in background
    except (KeyboardInterrupt, SystemExit):
        logger.info("Application interrupt received. Shutting down...")
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.SHUTDOWN)
    except Exception as e:
        logger.critical(f"A critical error occurred with the scheduler or main loop: {e}", exc_info=True)
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
