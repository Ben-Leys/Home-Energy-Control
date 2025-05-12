# core/app_state.py
import logging

logger = logging.getLogger(__name__)

MODE_AUTO = "auto"
MODE_MANUAL = "manual"

INV_CMD_LIMIT_STANDARD = "limit_standard"
INV_CMD_LIMIT_ZERO = "limit_zero"
INV_CMD_LIMIT_FIXED = {"limit_fixed_watts": INV_CMD_LIMIT_STANDARD}
INV_CMD_LIMIT_TO_USE = {"limit_to_use": INV_CMD_LIMIT_STANDARD}

EVCC_CMD_STATE_OFF = "off"
EVCC_CMD_STATE_PV = "pv"
EVCC_CMD_STATE_MINPV = "minpv"
EVCC_CMD_STATE_NOW = "now"


class AppState:
    """
    Class to hold and manage the shared operational state and
    a structured way to access and update global data.
    """

    def __init__(self):
        self.current_values = {
            "p1_meter_data": None,  # Latest raw P1 data dict
            "inverter_data": None,  # Latest raw inverter data dict
            "ev_soc_percent": None,  # EV State of Charge (%)
            "ev_is_connected": False,
            "ev_is_charging": False,
            "ev_charge_power_watts": 0,
            "current_energy_prices": None,  # Dict: {buy_price, sell_price} for current hour
            "solar_forecast_now": None,  # Forecasted PV for current period
            "inverter_mode": MODE_AUTO,  # auto, manual
            "inverter_manual_state": None,
            "evcc_mode": MODE_AUTO,  # auto, manual
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
