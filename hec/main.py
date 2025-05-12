import logging
import time
from pathlib import Path
from core.config_loader import load_app_config
from core.logging_setup import start_logger
from core.app_state import GLOBAL_APP_STATE


APP_CONFIG = load_app_config()
start_logger(APP_CONFIG)
logger = logging.getLogger(__name__)
logger.info("Main application logging active")


def main():
    logger.info("Starting Home Energy Control main")

    print(APP_CONFIG['database']['path'])

    GLOBAL_APP_STATE.set("ev_soc_percent", 75)
    current_soc = GLOBAL_APP_STATE.get("ev_soc_percent")
    print(current_soc)


if __name__ == "__main__":
    main()