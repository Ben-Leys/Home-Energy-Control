import logging
import sys
from pathlib import Path

if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(project_root))

from hec.core.app_initializer import load_app_config
from hec.core.app_logging import start_logger
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.runtime import ApplicationRuntime


def run_application():
    try:
        app_config = load_app_config()
    except FileNotFoundError as err:
        print(f"CRITICAL: Configuration file not found. {err}. Exiting.")
        return 1
    except ValueError as err:
        print(f"CRITICAL: Error parsing configuration file. {err}. Exiting.")
        return 1

    start_logger(app_config, GLOBAL_APP_STATE)
    logger = logging.getLogger(__name__)
    logger.info("*************************************************")
    logger.info("*** Starting Home Energy Control Application  ***")
    logger.info("*************************************************")

    runtime = ApplicationRuntime(app_config, app_state=GLOBAL_APP_STATE)
    exit_code = runtime.run()
    logging.shutdown()
    return exit_code


if __name__ == "__main__":
    sys.exit(run_application())
