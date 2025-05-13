# core/app_state.py
import logging
from hec import constants


logger = logging.getLogger(__name__)


class AppState:
    """
    Class to hold and manage the shared operational state and
    a structured way to access and update global data.
    """

    def __init__(self):
        self.current_values = {
            "app_status": constants.AppStatus.STARTING,
            "p1_meter_data": None,  # Latest raw P1 data dict
            "inverter_data": None,  # Latest raw inverter data dict
            "inverter_status": constants.InverterStatus.OFFLINE,
            "inverter_mode": constants.MODE_AUTO,  # auto, manual
            "inverter_manual_state": None,
            "ev_charge_status": constants.EVChargeStatus.OFFLINE,
            "ev_soc_percent": None,  # EV State of Charge (%)
            "ev_charge_power_watts": 0,
            "energy_prices": None,
            "solar_forecast_watts": None,  # Forecasted PV for current period
            "wind_forecast_pct": None,
            "electricity_grid_load_forecast_pct": None,
            "evcc_mode": constants.MODE_AUTO,  # auto, manual
            "evcc_manual_state": None,
            "evcc_limit_amp": None,
            "evcc_smart_cost_limit": None,
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
