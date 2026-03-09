# tests/logic_engine/test_system_mediator.py
import unittest
import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from hec.controllers.api_evcc import EvccApiClient
from hec.controllers.modbus_sma_inverter import InverterSmaModbusClient
from hec.logic_engine.system_mediator import SystemMediator
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core import constants as c
from hec.core.models import NetElectricityPriceInterval, EVCCLoadpointState
from hec.data_sources import api_p1_meter_homewizard

# --- Configure a logger for the module being tested ---
logger_mediator = logging.getLogger('hec.logic_engine.system_mediator')

logger_mediator.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
logger_mediator.addHandler(stream_handler)


# --- Helper to create mock app_config ---
def get_mock_app_config():
    return {
        "mediator": {
            "max_peak_consumption_kw": 2.5,
            "buffer_before_pv_limit_change": 2,  # minutes
        },
        "inverter": {
            "standard_power_limit": 7000,  # Watts
            "location": {
                "city": "Putte",
                "latitude": 51.05483,
                "longitude": 4.62877,
                "timezone": 'Europe/Brussels',
                "region_name_for_astral_optional": 'Belgium'}
        },
        "evcc": {
            "max_current": 16,
        },
    }


# --- Helper to create sample price intervals for AppState ---
def create_sample_price_intervals(start_dt_local: datetime, num_intervals: int, resolution_min: int,
                                  buy_price: float, sell_price: float, contract_type: str = "dynamic"
                                  ) -> list:
    intervals = []
    for i in range(num_intervals):
        current_start = start_dt_local + timedelta(minutes=i * resolution_min)
        nep = NetElectricityPriceInterval(
            interval_start_local=current_start,
            resolution_minutes=resolution_min,
            active_contract_type=contract_type,
            net_prices_eur_per_kwh={
                "dynamic": {"buy": buy_price, "sell": sell_price},
                "fixed": {"buy": 0.27, "sell": 0.02}  # Dummy fixed prices
            }
        )
        intervals.append(nep)
    return intervals


class TestSystemMediatorFunctional(unittest.TestCase):

    def setUp(self):
        self.mock_app_config = get_mock_app_config()

        # Mock client instances
        self.mock_evcc_client = MagicMock(spec=EvccApiClient)
        self.mock_evcc_client.is_available = True
        self.mock_evcc_client.min_current = 6
        self.mock_evcc_client.max_current = 32

        self.mock_inverter_client = MagicMock(spec=InverterSmaModbusClient)
        self.mock_inverter_client.get_operational_status.return_value = c.InverterStatus.NORMAL
        self.mock_inverter_client.standard_power_limit = self.mock_app_config["inverter"]["standard_power_limit"]

        self.mock_p1_client = MagicMock(spec=api_p1_meter_homewizard)
        self.mock_p1_client.is_initialized = True
        self.mock_p1_client.set_battery_mode = MagicMock(return_value=True)

        # Reset GLOBAL_APP_STATE for each test or mock its get/set
        GLOBAL_APP_STATE.current_values = {"app_state": c.AppStatus.STARTING,
                                           "app_operating_mode": c.OperatingMode.MODE_MANUAL,
                                           "app_mediator_goal": None, "p1_meter_data": None,
                                           "average_grid_import_watts": None,
                                           "average_grid_export_watts": None,
                                           "inverter_data": {"operational_status": c.InverterStatus.UNKNOWN},
                                           "inverter_manual_state": None, "inverter_manual_limit": None,
                                           "average_solar_production_watts": None, "electricity_prices_today": None,
                                           "evcc_overall_state": None,  # DONE
                                           "evcc_loadpoint_state": None,
                                           "evcc_manual_state": None}
        GLOBAL_APP_STATE.set('app_state', c.AppStatus.NORMAL)
        GLOBAL_APP_STATE.set('app_operating_mode', c.OperatingMode.MODE_AUTO)
        GLOBAL_APP_STATE.set('p1_meter_data', {"active_power_w": 0, "monthly_power_peak_w": 2500})
        GLOBAL_APP_STATE.set('inverter_data', {"active_power_limit_watts": 7000, "pv_power_watts": 0})
        prices = create_sample_price_intervals(datetime.now(), 4, 15, buy_price=-0.10, sell_price=-0.26)
        GLOBAL_APP_STATE.set('electricity_prices_today', prices)
        GLOBAL_APP_STATE.set('evcc_loadpoint_state',
                             EVCCLoadpointState(is_connected=False, is_charging=False, charge_current=0, max_current=32,
                                                mode="off").to_dict())
        GLOBAL_APP_STATE.set('average_grid_import_watts', {'3m': 0, '5m': 0, '10m': 0})
        GLOBAL_APP_STATE.set('average_solar_production_watts', {'5m': 0})

        # Instantiate the mediator for each test
        self.mediator = SystemMediator(
            self.mock_app_config,
            self.mock_evcc_client,
            self.mock_inverter_client,
            self.mock_p1_client
        )
        # Reset mediator's internal state directly if needed
        self.mediator.next_price_interval_at = datetime.now().astimezone() - timedelta(hours=1)  # Force price fetch
        self.mediator.cur_buy_price = None
        self.mediator.cur_sell_price = None
        self.mediator.temp_charging_stopped_by_capacity = False
        self.mediator.car_was_connected = False
        self.mediator.force_charge_pushed = False
        self.mediator.last_pv_limit_change_time = None

    @patch('hec.logic_engine.system_mediator.datetime')  # Mock datetime.now() within the mediator module
    def test_scenario_negative_buy_price_force_charge_and_limit_pv(self, mock_datetime):
        """
        SCENARIO: Buy prices go negative.
        EXPECTED: EVCC set to "now", Inverter limit set to 0.
        """
        start_time = datetime(2025, 5, 24, 11, 0, 0)
        mock_datetime.now.return_value = start_time
        GLOBAL_APP_STATE.set('app_mediator_goal', c.MediatorGoal.CHARGE_WHEN_BUY_PRICE_NEGATIVE)

        # Setup AppState: prices, EV connected
        prices = create_sample_price_intervals(start_time, 4, 15, buy_price=-0.10, sell_price=-0.26)
        GLOBAL_APP_STATE.set('electricity_prices_today', prices)
        GLOBAL_APP_STATE.set('evcc_loadpoint_state',
                             EVCCLoadpointState(is_connected=True, is_charging=False, charge_current=0, max_current=32,
                                                mode="off").to_dict())
        GLOBAL_APP_STATE.set('p1_meter_data', {"active_power_w": -3400, "monthly_power_peak_w": 2500})
        GLOBAL_APP_STATE.set('inverter_data', {"active_power_limit_watts": 7000, "pv_power_watts": 3600})

        # --- Run mediator logic ---
        self.mediator.run_system_mediation_logic()
        self.assertEqual(self.mediator.app_mediator_goal, c.MediatorGoal.CHARGE_WHEN_BUY_PRICE_NEGATIVE)

        # --- Assertions for EVCC ---
        # Check if evcc_client.set_charge_mode was called with "now"
        self.mock_evcc_client.set_charge_mode.assert_any_call(c.EVCCManualState.EVCC_CMD_STATE_NOW)
        self.mock_evcc_client.set_max_current.assert_called_with(10)

        # --- Assertions for Inverter ---
        # Check if inverter_client.set_active_power_limit was called with 0
        self.mock_inverter_client.set_active_power_limit.assert_called_with(0)
        self.assertEqual(GLOBAL_APP_STATE.get('inverter_manual_state'), c.InverterManualState.INV_CMD_LIMIT_ZERO)

    @patch('hec.logic_engine.system_mediator.datetime')
    def test_scenario_negative_sell_price_no_limit_pv_and_ev_pv_charge(self, mock_datetime):
        """
        SCENARIO: Sell prices go negative, buy prices are normal. EV is connected.
        EXPECTED: Inverter was not limited so should not be called. EVCC set to "pv".
        """
        start_time = datetime(2025, 5, 24, 12, 0, 0)
        mock_datetime.now.return_value = start_time
        self.mediator.last_pv_limit_change_time = None
        GLOBAL_APP_STATE.set('app_mediator_goal', c.MediatorGoal.CHARGE_WHEN_SELL_PRICE_NEGATIVE)

        prices = create_sample_price_intervals(start_time, 4, 15, buy_price=0.15, sell_price=-0.05)
        GLOBAL_APP_STATE.set('electricity_prices_today', prices)
        GLOBAL_APP_STATE.set('evcc_loadpoint_state',
                             EVCCLoadpointState(is_connected=True, is_charging=False, charge_current=0, max_current=32,
                                                mode="off").to_dict())
        GLOBAL_APP_STATE.set('p1_meter_data', {"active_power_w": -2300, "monthly_power_peak_w": 2500}) # Exporting 2.3kW
        GLOBAL_APP_STATE.set('inverter_data', {"active_power_limit_watts": 7000, "pv_power_watts": 2500})

        # --- Run mediator logic ---
        self.mediator.run_system_mediation_logic()
        self.assertEqual(self.mediator.app_mediator_goal, c.MediatorGoal.CHARGE_WHEN_SELL_PRICE_NEGATIVE)

        # --- Assertions for EVCC ---
        self.mock_evcc_client.set_charge_mode.assert_any_call(c.EVCCManualState.EVCC_CMD_STATE_PV)
        self.mock_evcc_client.set_max_current.assert_called_with(10)

        # --- Assertions for Inverter ---
        self.assertEqual(GLOBAL_APP_STATE.get('inverter_manual_state'), c.InverterManualState.INV_CMD_LIMIT_STANDARD)
        self.mock_inverter_client.set_active_power_limit.assert_not_called()

    @patch('hec.logic_engine.system_mediator.datetime')
    def test_ev_charging_capacity_tariff_adjustment(self, mock_datetime):
        """
        SCENARIO: EV is charging, grid import is high, approaching capacity peak.
        EXPECTED: EV charging current (max_amps) is reduced.
        """
        start_time = datetime(2025, 5, 24, 13, 0, 0)
        mock_datetime.now.return_value = start_time
        self.mediator.current_max_peak_consumption_kw = 2.5  # Set capacity threshold
        GLOBAL_APP_STATE.set('app_mediator_goal', c.MediatorGoal.CHARGE_NOW_WITH_CAPACITY_RATE)

        prices = create_sample_price_intervals(start_time, 4, 15, buy_price=0.24, sell_price=0.08)
        GLOBAL_APP_STATE.set('electricity_prices_today', prices)
        # EV is connected AND charging, currently at 16A
        GLOBAL_APP_STATE.set('evcc_loadpoint_state',
                             EVCCLoadpointState(is_connected=True, is_charging=True, charge_current=16, max_current=32,
                                                mode="now").to_dict())
        # Grid import is high
        GLOBAL_APP_STATE.set('p1_meter_data', {"active_power_w": 2900, "monthly_power_peak_w": 2500})
        # Setup rolling average import to also indicate high load
        GLOBAL_APP_STATE.set('average_grid_import_watts', {'3m': 2300, '5m': 2200, '10m': 2100})
        GLOBAL_APP_STATE.set('inverter_data', {"active_power_limit_watts": 7000, "pv_power_watts": 100})  # Low PV

        # --- Run mediator logic ---
        self.mediator.run_system_mediation_logic()
        self.assertEqual(self.mediator.app_mediator_goal, c.MediatorGoal.CHARGE_NOW_WITH_CAPACITY_RATE)

        # --- Assertions for EVCC ---
        self.mock_evcc_client.set_max_current.assert_called()
        called_amps = self.mock_evcc_client.set_max_current.call_args[0][0]
        self.assertLess(called_amps, 16)
        self.assertGreaterEqual(called_amps, self.mock_evcc_client.min_current)

    @patch('hec.logic_engine.system_mediator.datetime')
    def test_manual_mode_override_evcc(self, mock_datetime):
        """SCENARIO: App is set to MANUAL mode by user, with goal no_charging."""
        GLOBAL_APP_STATE.set('app_operating_mode', c.OperatingMode.MODE_MANUAL)
        GLOBAL_APP_STATE.set('evcc_manual_state', c.EVCCManualState.EVCC_CMD_STATE_OFF)  # User wants EVCC off
        GLOBAL_APP_STATE.set('inverter_manual_state', c.InverterManualState.INV_CMD_LIMIT_STANDARD)

        # Irrelevant AppState data for this specific test, but good to have some defaults
        start_time = datetime(2025, 5, 24, 14, 0, 0)
        mock_datetime.now.return_value = start_time
        prices = create_sample_price_intervals(start_time, 4, 15, buy_price=0.24, sell_price=0.08)
        GLOBAL_APP_STATE.set('electricity_prices_today', prices)
        GLOBAL_APP_STATE.set('p1_meter_data', {"active_power_w": 200, "monthly_power_peak_w": 2500})
        GLOBAL_APP_STATE.set('evcc_loadpoint_state',
                             EVCCLoadpointState(is_connected=False, is_charging=False, charge_current=0, max_current=32,
                                                mode="pv").to_dict())
        self.mediator.run_system_mediation_logic()

        # Assert that EVCC client was told to go to "off"
        self.mock_evcc_client.set_charge_mode.assert_called_with(c.EVCCManualState.EVCC_CMD_STATE_OFF)
        # Assert that inverter control was NOT called for auto logic
        self.mock_inverter_client.set_active_power_limit.assert_not_called()

    @patch('hec.logic_engine.system_mediator.datetime')
    def test_inverter_manual_mode_respects_user_limit(self, mock_datetime):
        """
        SCENARIO: App in manual mode with a user-defined inverter limit.
        EXPECTED: _execute_inverter_state is called with that limit, regardless of prices.
        """
        # Arrange
        start_time = datetime(2025, 5, 24, 10, 0, 0)
        mock_datetime.now.return_value = start_time
        GLOBAL_APP_STATE.set('app_mediator_goal', c.MediatorGoal.CHARGE_WHEN_BUY_PRICE_NEGATIVE)
        GLOBAL_APP_STATE.set('app_operating_mode', c.OperatingMode.MODE_MANUAL)
        GLOBAL_APP_STATE.set('evcc_manual_state', c.EVCCManualState.EVCC_CMD_STATE_PV)
        # User manually selects 3000 W limit
        GLOBAL_APP_STATE.set('inverter_manual_state', c.InverterManualState.INV_CMD_LIMIT_MANUAL)
        GLOBAL_APP_STATE.set('inverter_manual_limit', 3000)
        prices = create_sample_price_intervals(start_time, 4, 15, buy_price=0.3, sell_price=0.1)
        GLOBAL_APP_STATE.set('electricity_prices_today', prices)

        # Act
        self.mediator.run_system_mediation_logic()

        # Assert
        self.mock_inverter_client.set_active_power_limit.assert_called_once_with(3000)
        self.mock_evcc_client.set_charge_mode.assert_called_with(c.EVCCManualState.EVCC_CMD_STATE_PV)

    @patch('hec.logic_engine.system_mediator.datetime')
    def test_car_connection_grace_period_and_expiry(self, mock_datetime):
        """
        SCENARIO: EV just connects—should get standard limit for 2 min, then revert to zero-import logic.
        """
        base_time = datetime(2025, 5, 24, 9, 0)
        # 1. initial connect
        mock_datetime.now.return_value = base_time
        GLOBAL_APP_STATE.set('app_mediator_goal', c.MediatorGoal.CHARGE_WHEN_SELL_PRICE_NEGATIVE)
        GLOBAL_APP_STATE.set('inverter_manual_state', c.InverterManualState.INV_CMD_LIMIT_TO_USE)
        GLOBAL_APP_STATE.set('evcc_manual_state', c.EVCCManualState.EVCC_CMD_STATE_PV)
        GLOBAL_APP_STATE.set('evcc_loadpoint_state',
                             EVCCLoadpointState(is_connected=False, is_charging=False,
                                                charge_current=0, max_current=32, mode="pv").to_dict())
        GLOBAL_APP_STATE.set('p1_meter_data', {"active_power_w": -95, "monthly_power_peak_w": 2500})
        GLOBAL_APP_STATE.set('inverter_data', {"active_power_limit_watts": 250, "pv_power_watts": 250})
        prices = create_sample_price_intervals(base_time, 4, 15, buy_price=0.1, sell_price=-0.05)
        GLOBAL_APP_STATE.set('electricity_prices_today', prices)

        self.mediator.run_system_mediation_logic()
        self.mock_inverter_client.set_active_power_limit.assert_called()  # Last limit change 10 min ago (because None)

        GLOBAL_APP_STATE.set('evcc_loadpoint_state',
                             EVCCLoadpointState(is_connected=True, is_charging=False,
                                                charge_current=0, max_current=32, mode="pv").to_dict())

        self.mediator.run_system_mediation_logic()
        # Should have pushed standard limit and force car charge start
        self.mock_inverter_client.set_active_power_limit.assert_called_with(
            self.mock_inverter_client.standard_power_limit
        )
        self.mock_evcc_client.sequence_force_pv_charging.assert_called()
        self.assertEqual(self.mediator.force_charge_pushed, True)
        GLOBAL_APP_STATE.set('inverter_data', {"active_power_limit_watts": 7000, "pv_power_watts": 7000})

        # 2. within 2-min window: still should not revert
        mock_datetime.now.return_value = base_time + timedelta(minutes=1)
        self.mock_inverter_client.set_active_power_limit.reset_mock()
        self.mediator.run_system_mediation_logic()
        self.mock_inverter_client.set_active_power_limit.assert_not_called()

        # 3. after 2 min: should now switch to INV_CMD_LIMIT_TO_USE logic (which for this goal yields standard)
        mock_datetime.now.return_value = base_time + timedelta(minutes=3)
        self.mock_inverter_client.set_active_power_limit.reset_mock()
        self.mediator.run_system_mediation_logic()
        self.assertEqual(self.mediator.force_charge_pushed, False)
        # Now it's evaluated again; since sell<0 it falls back to TO_USE, but with no PV yields zero-import buffer
        self.mock_inverter_client.set_active_power_limit.assert_called()
        self.assertEqual(GLOBAL_APP_STATE.get('inverter_manual_state'), c.InverterManualState.INV_CMD_LIMIT_TO_USE)

    def test_prepare_data_fails_with_no_price_intervals(self):
        """
        SCENARIO: electricity_prices_today is empty.
        EXPECTED: _prepare_data returns False and logs an error, mediator.run_system_mediation_logic() skips.
        """
        GLOBAL_APP_STATE.set('electricity_prices_today', [])
        success = self.mediator._prepare_data()
        self.assertFalse(success)

    @patch('hec.logic_engine.system_mediator.datetime')
    def test_transition_between_mediator_goals_updates_states(self, mock_datetime):
        """
        SCENARIO: Switch from CHARGE_WITH_ONLY_EXCESS to CHARGE_NOW_WITH_CAPACITY_RATE mid-run.
        EXPECTED: new_evcc_state and new_inv_state reflect the new goal on the second call.
        """
        t0 = datetime(2025, 5, 24, 8, 0)
        mock_datetime.now.return_value = t0
        # First: only excess solar
        GLOBAL_APP_STATE.set('app_mediator_goal', c.MediatorGoal.CHARGE_WITH_ONLY_EXCESS_SOLAR_POWER)
        GLOBAL_APP_STATE.set('evcc_loadpoint_state',
                             EVCCLoadpointState(is_connected=True, is_charging=True,
                                                charge_current=0, max_current=6, mode="pv").to_dict())
        GLOBAL_APP_STATE.set('p1_meter_data', {"active_power_w": -95, "monthly_power_peak_w": 2500})
        GLOBAL_APP_STATE.set('inverter_data', {"active_power_limit_watts": 7000, "pv_power_watts": 1600})
        prices = create_sample_price_intervals(t0, 4, 15, buy_price=0.2, sell_price=0.05)
        GLOBAL_APP_STATE.set('electricity_prices_today', prices)

        self.mediator.run_system_mediation_logic()
        first_state = self.mediator.new_evcc_state

        # Now switch to now-with-capacity
        mock_datetime.now.return_value = t0 + timedelta(minutes=30)
        GLOBAL_APP_STATE.set('app_mediator_goal', c.MediatorGoal.CHARGE_NOW_WITH_CAPACITY_RATE)
        self.mediator.run_system_mediation_logic()
        second_state = self.mediator.new_evcc_state

        self.assertNotEqual(first_state, second_state)
        self.assertEqual(second_state, c.EVCCManualState.EVCC_CMD_STATE_NOW)
        self.assertEqual(self.mediator.app_mediator_goal, c.MediatorGoal.CHARGE_NOW_WITH_CAPACITY_RATE)

        # --- Assertions for EVCC ---
        self.mock_evcc_client.set_charge_mode.assert_any_call(c.EVCCManualState.EVCC_CMD_STATE_NOW)
        self.mock_evcc_client.set_max_current.assert_called_with(10)

        # --- Assertions for Inverter ---
        self.assertEqual(GLOBAL_APP_STATE.get('inverter_manual_state'), c.InverterManualState.INV_CMD_LIMIT_STANDARD)
        self.mock_inverter_client.set_active_power_limit.assert_not_called()


if __name__ == '__main__':
    unittest.main()
