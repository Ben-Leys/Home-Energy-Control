# core/app_state.py
import logging
from typing import List, Optional

from hec.core import constants as c
from hec.database_ops import db_handler

logger = logging.getLogger(__name__)


class AppState:
    """
    Class to hold and manage the shared operational state and
    a structured way to access and update global data.
    """

    def __init__(self):
        self.current_values = {
            # General app values
            "app_state": c.AppStatus.STARTING,
            "app_operating_mode": c.OperatingMode.MODE_MANUAL,
            "app_mediator_goal": c.MediatorGoal.NO_CHARGING,
            "reboot_request": False,
            "summary_request": False,
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
            # Battery control
            "battery_data": None,
            "battery_records": [],
            "battery_manual_mode": None,
            "prediction_plan": None,
            "plan_generation_date": None,
            "empty_since": None
        }
        self.prediction_plan_df = None

        self.db_handler: Optional[db_handler] = None
        self.persisted_keys: List[str] = ["app_operating_mode", "app_mediator_goal", "inverter_manual_state",
                                          "inverter_manual_limit", "evcc_manual_state", "evcc_manual_limit",
                                          "battery_manual_mode", "empty_since"]

    def get(self, key, default=None):
        if key == "prediction_plan_df":
            return self.prediction_plan_df
        return self.current_values.get(key, default)

    def set(self, key, value):
        if key in self.current_values:
            self.current_values[key] = value
            truncated_value = str(value)[:500]
            logger.debug(f"App state updated: {key} = {truncated_value}")
        elif key == "prediction_plan_df":
            self.prediction_plan_df = value
        else:
            logger.warning(f"Attempted to update non-existent state key: {key}")

        should_persist = False
        if key in self.persisted_keys:
            should_persist = True

        if should_persist:
            if self.db_handler:
                self.db_handler.save_setting(key, value)  # Pass the original value
            else:
                logger.warning(f"AppState: db_handler not set. Cannot persist setting '{key}'.")

    def get_all(self):
        """Returns a copy of the entire current state."""
        return self.current_values.copy()

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
            if key in self.current_values:  # Only update if key is known to AppState
                # The value from DB is already deserialized to its Python type by load_all_settings
                self.current_values[key] = value
                logger.debug(f"AppState: Loaded setting '{key}' = {value} (type: {type(value)}) from DB.")
                loaded_count += 1
            else:
                logger.warning(f"AppState: Setting '{key}' from DB is not a recognized AppState key. Ignoring.")
        if loaded_count > 0:
            logger.info(f"AppState: Successfully loaded {loaded_count} settings from database.")
        else:
            logger.info("AppState: No persisted settings found or loaded from database.")


GLOBAL_APP_STATE = AppState()
