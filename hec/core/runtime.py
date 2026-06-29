import logging
import time
from enum import Enum
from threading import Thread
from typing import Any, Dict, Optional

from hec.core import constants as c
from hec.core.api_server import run_api_server
from hec.core.app_initializer import (
    check_historic_data,
    initialize_database_handler,
    initialize_external_clients,
    populate_app_state,
    setup_scheduler,
)
from hec.core.app_logging import inject_db_to_logging
from hec.core.app_state import GLOBAL_APP_STATE, AppState
from hec.core.tariff_manager import initialize_tariff_manager
from hec.logic_engine import scheduled_tasks
from hec.logic_engine.system_mediator import SystemMediator

logger = logging.getLogger(__name__)


class RuntimeExitReason(Enum):
    STOPPED = "stopped"
    INTERRUPTED = "interrupted"
    STARTUP_FAILED = "startup_failed"
    CRASHED = "crashed"
    RESTART_REQUESTED = "restart_requested"


class RuntimeInitializer:
    """Builds the runtime resources without starting the scheduler."""

    def initialize(
        self,
        app_config: Dict[str, Any],
        app_state: AppState,
        api_runner=run_api_server,
    ) -> Dict[str, Any]:
        logger.debug("Initial AppState: %s", app_state.get_all())
        app_state.set("app_state", c.AppStatus.STARTING)
        app_state.set("restart_status", "idle")
        app_state.set("restart_message", None)
        app_state.set("reboot_request", False)

        tariff_manager = initialize_tariff_manager(app_config)

        db_handler = initialize_database_handler(app_config)
        if db_handler is None:
            app_state.set("app_state", c.AppStatus.ALARM)
            return {"db_handler": None, "scheduler": None, "api_thread": None}

        inject_db_to_logging(db_handler)
        fetch_entsoe, fetch_elia = check_historic_data(db_handler, app_config)

        p1_meter_client, inverter_client, evcc_client, battery_clients, battery_gateway = initialize_external_clients(
            app_config
        )

        app_state.set_db_handler(db_handler)
        app_state.load_persisted_settings()
        populate_app_state(db_handler, app_config, evcc_client)

        system_mediator = SystemMediator(
            app_config,
            evcc_client,
            inverter_client,
            p1_meter_client,
            battery_gateway=battery_gateway,
        )

        api_thread = None
        if app_config.get("api_server", {}).get("enabled", True):
            api_thread = Thread(
                target=api_runner,
                args=(app_config, db_handler),
                daemon=True,
                name="hec-api-server",
            )
            api_thread.start()
        else:
            logger.info("API server is disabled in configuration.")

        run_scheduler_in_background = app_config.get("scheduler", {}).get("run_in_background", True)
        scheduler = setup_scheduler(app_config, run_in_background=run_scheduler_in_background)
        scheduled_tasks.register_all_jobs(
            scheduler,
            db_handler,
            app_config,
            p1_meter_client,
            inverter_client,
            evcc_client,
            tariff_manager,
            system_mediator,
            battery_clients,
            battery_gateway,
            fetch_entsoe,
            fetch_elia,
        )

        return {
            "db_handler": db_handler,
            "scheduler": scheduler,
            "api_thread": api_thread,
            "run_scheduler_in_background": run_scheduler_in_background,
        }


class ApplicationRuntime:
    """Owns application startup, restart detection, and graceful cleanup."""

    def __init__(
        self,
        app_config: Dict[str, Any],
        app_state: AppState = GLOBAL_APP_STATE,
        initializer: Optional[RuntimeInitializer] = None,
        sleep_func=time.sleep,
    ):
        self.app_config = app_config
        self.app_state = app_state
        self.initializer = initializer or RuntimeInitializer()
        self.sleep_func = sleep_func
        self.db_handler = None
        self.scheduler = None
        self.api_thread = None
        self.run_scheduler_in_background = True
        self.exit_reason = RuntimeExitReason.STOPPED

    def start(self) -> bool:
        resources = self.initializer.initialize(self.app_config, self.app_state)
        self.db_handler = resources.get("db_handler")
        self.scheduler = resources.get("scheduler")
        self.api_thread = resources.get("api_thread")
        self.run_scheduler_in_background = resources.get("run_scheduler_in_background", True)

        if self.app_state.get("app_state") == c.AppStatus.ALARM:
            logger.critical("Application initialization failed. Exiting.")
            self.exit_reason = RuntimeExitReason.STARTUP_FAILED
            return False

        if self.scheduler is None:
            logger.critical("Scheduler was not initialized. Exiting.")
            self.app_state.set("app_state", c.AppStatus.ALARM)
            self.exit_reason = RuntimeExitReason.STARTUP_FAILED
            return False

        logger.info("Starting scheduler...")
        self.scheduler.start()

        if self.run_scheduler_in_background and self.app_state.get("app_state") == c.AppStatus.STARTING:
            self.app_state.set("app_state", c.AppStatus.NORMAL)

        return True

    def run(self) -> int:
        try:
            if not self.start():
                return 1

            if self.run_scheduler_in_background:
                logger.info("BackgroundScheduler started. Runtime loop is monitoring API and restart requests.")
                self._monitor_until_stop_requested()
            else:
                logger.info("BlockingScheduler started. Runtime restart monitoring is unavailable in blocking mode.")

            return self._exit_code()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Application interrupt received. Shutting down...")
            self.exit_reason = RuntimeExitReason.INTERRUPTED
            return 0
        except Exception as exc:
            logger.critical("A critical error occurred with the scheduler or main loop: %s", exc, exc_info=True)
            self.exit_reason = RuntimeExitReason.CRASHED
            return 1
        finally:
            if self.exit_reason == RuntimeExitReason.RESTART_REQUESTED:
                self.app_state.set("restart_status", "stopping")
                self.app_state.set(
                    "restart_message",
                    "Restart requested. Shutting down so the external supervisor can start a fresh process.",
                )
            self.shutdown(c.AppStatus.SHUTDOWN)

    def shutdown(self, app_status: c.AppStatus = c.AppStatus.SHUTDOWN):
        self.app_state.set("app_state", app_status)

        if self.scheduler is not None and getattr(self.scheduler, "running", False):
            logger.info("Shutting down scheduler...")
            self.scheduler.shutdown()

        if self.db_handler is not None:
            self.db_handler.close_connection()

        logger.info("Application shut down gracefully.")

    def _monitor_until_stop_requested(self):
        loop_sleep_seconds = self.app_config.get("runtime", {}).get("main_loop_sleep_seconds", 1.0)

        while True:
            if self.app_state.get("reboot_request", False):
                logger.warning("Restart requested. Runtime will perform graceful shutdown.")
                self.exit_reason = RuntimeExitReason.RESTART_REQUESTED
                return

            if self.api_thread is not None and not self.api_thread.is_alive():
                logger.warning("API thread exited. Runtime will shut down.")
                self.exit_reason = RuntimeExitReason.STOPPED
                return

            self.sleep_func(loop_sleep_seconds)

    def _exit_code(self) -> int:
        if self.exit_reason == RuntimeExitReason.RESTART_REQUESTED:
            runtime_config = self.app_config.get("runtime", {})
            return int(runtime_config.get("restart_exit_code", 75))
        if self.exit_reason == RuntimeExitReason.CRASHED:
            return 1
        return 0
