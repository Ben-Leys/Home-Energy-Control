# core/app_state.py
import copy
import logging
import threading
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, List, Optional

from hec.core import constants as c
from hec.database_ops import db_handler

logger = logging.getLogger(__name__)


class AppState:
    """
    Class to hold and manage the shared operational state and
    a structured way to access and update global data.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._thread_context = threading.local()
        self.current_values = {
            # General app values
            "state_version": 0,
            "app_state": c.AppStatus.STARTING,
            "app_operating_mode": c.OperatingMode.MODE_MANUAL,
            "app_mediator_goal": c.MediatorGoal.NO_CHARGING,
            "reboot_request": False,
            "restart_status": "idle",
            "restart_message": None,
            "summary_request": False,
            "summary_job_status": {
                "state": "idle",
                "message": "No summary requested",
                "updated_at_utc": None,
            },
            "sunrise": None,
            "sunset": None,
            # P1 meter data, recent import/export samples and averages
            "p1_meter_data": None,
            "p1_meter_last_stored_boundary_slot_utc_iso": None,
            "recent_p1_import_kwh_samples": None,
            "recent_p1_export_kwh_samples": None,
            "average_grid_import_watts": None,
            "average_grid_export_watts": None,
            # Inverter data, recent import/export samples and averages
            "inverter_data": {"operational_status": c.InverterStatus.UNKNOWN},
            "inverter_manual_state": None,
            "inverter_manual_limit": None,
            "recent_solar_production_wh_samples": None,
            "average_solar_production_watts": None,
            # Electricity prices and solar, wind and grid_load forecasts (forecasts deprecated 19/03/2026)
            "electricity_prices_today": None,
            "electricity_prices_tomorrow": None,
            "forecasts": None,
            # EVCC data
            "evcc_overall_state": None,
            "evcc_loadpoint_state": None,
            "evcc_manual_state": None,
            "evcc_manual_limit": None,
            "evcc_last_logged_slot": None,
            # Battery control
            "battery_data": None,
            "battery_records": [],
            "battery_manual_mode": None,
            "prediction_plan": None,
            "plan_generation_date": None,
            "empty_since": None,
            "sunrise_block_until": None
        }
        self.prediction_plan_df = None

        self.db_handler: Optional[db_handler] = None
        self.persisted_keys: List[str] = ["app_operating_mode", "app_mediator_goal", "inverter_manual_state",
                                          "inverter_manual_limit", "evcc_manual_state", "evcc_manual_limit",
                                          "battery_manual_mode", "empty_since"]

    def get(self, key, default=None):
        snapshot = self._active_snapshot()
        if snapshot is not None:
            if key == "prediction_plan_df":
                return self._copy_value(snapshot.get("prediction_plan_df", default))
            return self._copy_value(snapshot["current_values"].get(key, default))

        with self._lock:
            self._ensure_state_version_locked()
            if key == "prediction_plan_df":
                return self._copy_value(self.prediction_plan_df)
            return self._copy_value(self.current_values.get(key, default))

    def set(self, key, value):
        if key == "state_version":
            logger.warning("Attempted to directly update read-only state key: state_version")
            return

        with self._lock:
            self._ensure_state_version_locked()
            if key in self.current_values:
                self.current_values[key] = self._copy_value(value)
                self._bump_state_version_locked()
                truncated_value = str(value)[:500]
                logger.debug(f"App state updated: {key} = {truncated_value}")
            elif key == "prediction_plan_df":
                self.prediction_plan_df = self._copy_value(value)
                self._bump_state_version_locked()
            else:
                logger.warning(f"Attempted to update non-existent state key: {key}")
                return

            should_persist = key in self.persisted_keys

        if should_persist:
            if self.db_handler:
                self.db_handler.save_setting(key, value)  # Pass the original value
            else:
                logger.warning(f"AppState: db_handler not set. Cannot persist setting '{key}'.")

    def mutate(self, key: str, mutator: Callable[[Any], Any]):
        """
        Updates a mutable state value under the AppState lock and bumps state_version.
        The mutator may update its argument in place and return None, or return a
        replacement value.
        """
        if key == "state_version":
            logger.warning("Attempted to directly mutate read-only state key: state_version")
            return None

        with self._lock:
            self._ensure_state_version_locked()
            if key in self.current_values:
                current_value = self._copy_value(self.current_values[key])
                mutated_value = mutator(current_value)
                new_value = current_value if mutated_value is None else mutated_value
                self.current_values[key] = self._copy_value(new_value)
                self._bump_state_version_locked()
                returned_value = self._copy_value(self.current_values[key])
            elif key == "prediction_plan_df":
                current_value = self._copy_value(self.prediction_plan_df)
                mutated_value = mutator(current_value)
                new_value = current_value if mutated_value is None else mutated_value
                self.prediction_plan_df = self._copy_value(new_value)
                self._bump_state_version_locked()
                returned_value = self._copy_value(self.prediction_plan_df)
            else:
                logger.warning(f"Attempted to mutate non-existent state key: {key}")
                return None

            should_persist = key in self.persisted_keys

        if should_persist:
            if self.db_handler:
                self.db_handler.save_setting(key, returned_value)
            else:
                logger.warning(f"AppState: db_handler not set. Cannot persist setting '{key}'.")

        return returned_value

    def has_key(self, key: str) -> bool:
        with self._lock:
            return key in self.current_values or key == "prediction_plan_df"

    def get_state_version(self) -> int:
        with self._lock:
            self._ensure_state_version_locked()
            return int(self.current_values["state_version"])

    def get_all(self):
        """Returns a copied snapshot of the current JSON-facing state."""
        with self._lock:
            self._ensure_state_version_locked()
            return {key: self._copy_value(value) for key, value in self.current_values.items()}

    def snapshot(self) -> Dict[str, Any]:
        """Returns a copied read snapshot for one logical decision tick."""
        with self._lock:
            self._ensure_state_version_locked()
            return {
                "current_values": {key: self._copy_value(value) for key, value in self.current_values.items()},
                "prediction_plan_df": self._copy_value(self.prediction_plan_df),
                "state_version": self.current_values["state_version"],
            }

    @contextmanager
    def snapshot_context(self) -> Iterator[Dict[str, Any]]:
        """
        Makes AppState.get() read from one immutable snapshot in the current thread.
        Writes still update the live state and increment state_version.
        """
        prior_snapshot = self._active_snapshot()
        snapshot = self.snapshot()
        self._thread_context.snapshot = snapshot
        try:
            yield snapshot
        finally:
            if prior_snapshot is None:
                try:
                    del self._thread_context.snapshot
                except AttributeError:
                    pass
            else:
                self._thread_context.snapshot = prior_snapshot

    def set_db_handler(self, db_handler_instance):
        self.db_handler = db_handler_instance

    def load_persisted_settings(self):
        """Loads all persisted settings from the DB and updates AppState."""
        if not self.db_handler:
            logger.warning("AppState: db_handler not set. Cannot load persisted settings.")
            return

        logger.info("AppState: Loading persisted settings from database...")
        settings_from_db = self.db_handler.load_all_settings()

        loaded_count = 0
        for key, value in settings_from_db.items():
            with self._lock:
                self._ensure_state_version_locked()
                if key in self.current_values:  # Only update if key is known to AppState
                    # The value from DB is already deserialized to its Python type by load_all_settings
                    self.current_values[key] = self._copy_value(value)
                    self._bump_state_version_locked()
                    logger.debug(f"AppState: Loaded setting '{key}' = {value} (type: {type(value)}) from DB.")
                    loaded_count += 1
                else:
                    logger.warning(f"AppState: Setting '{key}' from DB is not a recognized AppState key. Ignoring.")
        if loaded_count > 0:
            logger.info(f"AppState: Successfully loaded {loaded_count} settings from database.")
        else:
            logger.info("AppState: No persisted settings found or loaded from database.")

    def _active_snapshot(self):
        return getattr(self._thread_context, "snapshot", None)

    def _ensure_state_version_locked(self):
        self.current_values.setdefault("state_version", 0)

    def _bump_state_version_locked(self):
        self.current_values["state_version"] = int(self.current_values.get("state_version", 0)) + 1

    @staticmethod
    def _copy_value(value):
        try:
            return copy.deepcopy(value)
        except Exception:
            logger.debug("AppState: deep copy failed for %s; using shallow snapshot value.", type(value), exc_info=True)
            return value


GLOBAL_APP_STATE = AppState()
