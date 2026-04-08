# hec/core/app_initializer.py
import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

import yaml
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

from hec.controllers import modbus_sma_inverter
from hec.controllers.api_evcc import EvccApiClient
from hec.core import constants as c
from hec.data_sources import api_p1_meter_homewizard, api_battery_homewizard
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.data_processors import populate_appstate_with_price_data
from hec.logic_engine.scheduled_tasks import task_poll_evcc_state

logger = logging.getLogger(__name__)

CONFIG_FILE_NAME = "config.yaml"
BASE_DIR = Path(__file__).resolve().parent.parent


def populate_app_state(db_handler: DatabaseHandler, app_config: dict, evcc_client: EvccApiClient):
    """Populate app state with necessary data from data sources."""
    try:
        # Populate price data
        populate_appstate_with_price_data(db_handler, app_config, False)

        # Populate evcc data
        task_poll_evcc_state(evcc_client)

    except Exception as e:
        logger.error(f"Error during AppState population: {e}", exc_info=True)


def check_historic_data(db_handler: DatabaseHandler, app_config: dict):
    """
    Check if historic data is available in the database for the price predictor.
    If not, this is probably the first run of the system.
    """
    hist_start_date = datetime.combine(date.fromisoformat(app_config.get('historic_data').get('start_date')),
                                       time.min, tzinfo=timezone.utc)
    days = (datetime.now().date() - hist_start_date.date()).days
    fetch_entsoe, fetch_elia = False, False

    # Check for day-ahead prices
    da_hist_start = db_handler.get_da_prices(hist_start_date)
    if not da_hist_start:
        logger.info(f"No historic data available for day-ahead prices. Fetching {days} days with task...")
        fetch_entsoe = True
    else:
        logger.info(f"Historic day-ahead price data available for {days} days.")

    # Check for Elia forecasts
    forecast_hist_start = db_handler.get_elia_forecasts("solar", hist_start_date,
                                                        hist_start_date + timedelta(days=1))
    if not forecast_hist_start:
        logger.info(f"No historic data available for Elia forecasts. Fetching {days} days with task...")
        fetch_elia = True
    else:
        logger.info(f"Historic forecast data available for {days} days.")

    return fetch_entsoe, fetch_elia


def initialize_database_handler(app_config: dict) -> Optional[DatabaseHandler]:
    try:
        db_handler = DatabaseHandler(app_config['database'])
        db_handler.initialize_database()
        return db_handler
    except KeyError:
        logger.critical("Database configuration missing in config.yaml. Exiting.")
        return None
    except Exception as e:
        logger.critical(f"Failed to initialize database: {e}", exc_info=True)
        return None


def initialize_external_clients(app_config: dict):
    """Initializes and *returns* the data sources client instance."""

    logger.info("Initializing data source clients...")
    p1_client = None
    inverter_client = None
    evcc_client = None

    # --- P1 Meter ---
    try:
        p1_conf = app_config.get("p1_meter", {})
        host = p1_conf.get("host")
        token = os.getenv("P1_METER")
        if not host:
            logger.warning("P1 meter host not configured. P1 data source will be disabled.")
        else:
            client = api_p1_meter_homewizard.P1MeterHomewizardClient(host=host, token=token)
            if client.is_initialized:
                p1_client = client
                logger.info("P1 meter client initialized successfully.")
            else:
                logger.warning("P1 meter client failed to initialize.")
    except Exception as e:
        logger.error(f"Error initializing P1 meter client. {e}", exc_info=True)

    # --- SMA Inverter ---
    try:
        inv_config = app_config.get('inverter', {})
        host = inv_config.get('host')

        if not host:
            logger.warning("Inverter host not configured. Inverter client will be disabled.")
        else:
            kwargs = {
                "host": host,
                "port": inv_config.get("port"),
                "modbus_unit_id": inv_config.get("modbus_unit_id"),
                "grid_guard_code": inv_config.get("grid_guard_code"),
                "standard_power_limit": inv_config.get("standard_power_limit"),
                "timeout_sec": inv_config.get("timeout_sec"),
            }
            client = modbus_sma_inverter.InverterSmaModbusClient(**kwargs)
            status = client.get_operational_status()
            if status != c.InverterStatus.UNKNOWN and status != c.InverterStatus.OFFLINE:
                inverter_client = client
                logger.info(f"Inverter client status: {status.value.lower()}")
            else:
                logger.warning(f"Inverter client unknown or offline at startup.")
    except Exception as e:
        logger.error(f"Error initializing Inverter SMA Modbus client: {e}", exc_info=True)

    # --- EVCC client ---
    try:
        evcc_config = app_config.get('evcc', {})
        api_url = evcc_config.get('api_url')
        if api_url:
            evcc_client = EvccApiClient(
                base_api_url=api_url,
                default_loadpoint_id=evcc_config.get('default_loadpoint_id'),
                max_current=evcc_config.get('max_current'),
                request_timeout=evcc_config.get('request_timeout_seconds')
            )
            if not evcc_client.is_available:
                logger.warning("EVCC client initialized, but EVCC API seems unavailable at startup.")
                evcc_client = None
        else:
            logger.warning("EVCC API URL not configured. EVCC client will be disabled.")
    except Exception as e:
        logger.error(f"Error initializing EVCC API client: {e}", exc_info=True)

    # --- Battery ---
    battery_clients = {}
    try:
        battery_config = app_config.get('batteries', {})
        if not battery_config:
            logger.warning("No batteries configured in app_config.")
        else:
            for b in battery_config:
                name = b.get("name")
                host = b.get("host")

                if not all([name, host]):
                    logger.warning(f"Skipping incomplete battery config: {b}")
                    continue

                client = api_battery_homewizard.BatteryHomeWizard(name=name, host=host)
                if client.is_initialized:
                    battery_clients[name] = client
                    logger.info(f"Battery '{name}' initialized successfully.")
                else:
                    logger.warning(f"Battery '{name}' failed to initialize.")
    except Exception as e:
        logger.error(f"Error initializing battery clients: {e}", exc_info=True)

    return p1_client, inverter_client, evcc_client, battery_clients


def setup_scheduler(config: dict, run_in_background: bool = False):
    """
    Initializes and configures the APScheduler.

    Args:
        config (dict): Application configuration.
        run_in_background (bool): If True use BackgroundScheduler, otherwise BlockingScheduler.

    Returns:
        APScheduler instance (BlockingScheduler or BackgroundScheduler).
    """
    scheduler_config = config.get('scheduler', {})  # Get scheduler specific config

    # Define executors
    executors = {
        'default': ThreadPoolExecutor(scheduler_config.get('thread_pool_max_workers', 10)),
    }

    job_defaults = {
        'coalesce': scheduler_config.get('coalesce_jobs', True),
        'max_instances': scheduler_config.get('max_instances_per_job', 3),
        'misfire_grace_time': scheduler_config.get('misfire_grace_time_seconds', 60)
    }

    if run_in_background:
        logger.info("Initializing BackgroundScheduler.")
        scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults,
                                        timezone=scheduler_config.get('timezone', 'Europe/Brussels'))
    else:
        logger.info("Initializing BlockingScheduler.")
        scheduler = BlockingScheduler(executors=executors, job_defaults=job_defaults,
                                      timezone=scheduler_config.get('timezone', 'Europe/Brussels'))

    logger.info(f"APScheduler initialized with timezone: {scheduler.timezone}")
    return scheduler


def load_app_config():
    """Loads application configuration from YAML and .env files."""
    # Load .env file
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        logger.debug(f"Loading .env from {env_path}")
        load_dotenv(dotenv_path=env_path)
    else:
        print(f"Warning: .env file not found at {env_path}")

    # Load YAML configuration
    config_path = BASE_DIR / CONFIG_FILE_NAME
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file '{CONFIG_FILE_NAME}' not found at {config_path}")

    with open(config_path, 'r') as f:
        try:
            logger.debug(f"Loading configuration from {config_path}")
            config: Dict = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration file: {e}")

    logging.info(f"Loaded files .env and {CONFIG_FILE_NAME}")
    return config
