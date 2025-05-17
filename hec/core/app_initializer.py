# hec/core/app_initializer.py
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict

import yaml
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

from hec.core.app_state import GLOBAL_APP_STATE
from hec.data_sources.api_homewizard_p1_meter import P1MeterHomeWizard
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.scheduled_tasks import populate_price_data_in_appstate

logger = logging.getLogger(__name__)

CONFIG_FILE_NAME = "config.yaml"
BASE_DIR = Path(__file__).resolve().parent.parent


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
    except Exception as e:
        logger.error(f"Error during initial AppState population for prices: {e}", exc_info=True)


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
            logger.warning("P1 meter client failed to initialize properly.")
            p1_client_instance = None

    except Exception as e:
        logger.error(f"Error initializing P1 meter client: {e}", exc_info=True)
        p1_client_instance = None

    return p1_client_instance


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
