# core/app_state.py
import logging

from hec.core import constants as c

logger = logging.getLogger(__name__)


class AppState:
    """
    Class to hold and manage the shared operational state and
    a structured way to access and update global data.
    """

    def __init__(self):
        self.current_values = {
            "app_state": c.AppStatus.STARTING,                                            # DONE
            "app_operating_mode": c.OperatingMode.MODE_AUTO,
            "app_manual_state": None,
            "p1_meter_data": None,  # Latest raw P1 data dict                             # DONE
            "p1_meter_last_stored_boundary_slot_utc_iso": None,                           # DONE
            "inverter_data": {"operational_status": c.InverterStatus.UNKNOWN},            # DONE
            "inverter_operating_mode": c.OperatingMode.MODE_AUTO,  # auto, manual
            "inverter_manual_state": None,
            "ev_date": None,
            "ev_charge_status": c.EVChargeStatus.UNKNOWN,
            "electricity_prices_today": None,                                             # DONE
            "electricity_prices_tomorrow": None,                                          # DONE
            "forecasts": None,                                                            # DONE
            "evcc_data": None,
            "evcc_operating_mode": c.OperatingMode.MODE_AUTO,
            "evcc_manual_state": None,
        }

    def get(self, key, default=None):
        return self.current_values.get(key, default)

    def set(self, key, value):
        if key in self.current_values:
            self.current_values[key] = value
            logger.debug(f"App state updated: {key} = {value}")
        else:
            logger.warning(f"Attempted to update non-existent state key: {key}")

    def get_all(self):
        """Returns a copy of the entire current state."""
        return self.current_values.copy()


GLOBAL_APP_STATE = AppState()
