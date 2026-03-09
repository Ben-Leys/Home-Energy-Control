import logging
import sys
import time
from threading import Thread

from hec.core import constants as c
from hec.core.api_server import run_api_server
from hec.core.app_initializer import (populate_app_state, initialize_database_handler,
                                      initialize_external_clients, setup_scheduler, load_app_config,
                                      check_historic_data)
from hec.core.app_logging import start_logger
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.tariff_manager import initialize_tariff_manager
from hec.logic_engine import scheduled_tasks
from hec.logic_engine.system_mediator import SystemMediator

try:
    APP_CONFIG = load_app_config()
except FileNotFoundError as err:
    print(f"CRITICAL: Configuration file not found. {err}. Exiting.")
    exit(1)
except ValueError as err:
    print(f"CRITICAL: Error parsing configuration file. {err}. Exiting.")
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
    fetch_entsoe, fetch_elia = check_historic_data(db_handler, APP_CONFIG)

    # --- INITIALIZE EXTERNAL CLIENTS ---
    p1_meter_client, inverter_client, evcc_client, battery_clients = initialize_external_clients(APP_CONFIG)

    # --- POPULATE APP STATE ---
    GLOBAL_APP_STATE.set_db_handler(db_handler)
    GLOBAL_APP_STATE.load_persisted_settings()
    populate_app_state(db_handler, APP_CONFIG, evcc_client)

    # --- INITIALIZE SYSTEM MEDIATOR ---
    system_mediator = SystemMediator(APP_CONFIG, evcc_client, inverter_client, p1_meter_client)

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
        return

    # --- SET UP SCHEDULER ---
    run_scheduler_in_background = APP_CONFIG.get('scheduler', {}).get('run_in_background', True)
    scheduler = setup_scheduler(APP_CONFIG, run_in_background=run_scheduler_in_background)
    scheduled_tasks.register_all_jobs(scheduler, db_handler, APP_CONFIG, p1_meter_client, inverter_client,
                                      evcc_client, tariff_manager, system_mediator, battery_clients,
                                      fetch_entsoe, fetch_elia)

    logger.info("Starting scheduler...")
    exit_code = 0
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
        exit_code = 0
    except Exception as e:
        logger.critical(f"A critical error occurred with the scheduler or main loop: {e}", exc_info=True)
        exit_code = 1
    finally:
        if scheduler and scheduler.running:
            logger.info("Shutting down scheduler...")
            scheduler.shutdown()
        if db_handler:
            db_handler.close_connection()
        logger.info("Application shut down gracefully.")
        logging.shutdown()

    sys.exit(exit_code)


if __name__ == "__main__":
    run_application()
