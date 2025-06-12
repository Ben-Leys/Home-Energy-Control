# hec/logic_engine/system_mediator.py
import logging
from datetime import datetime, timedelta
from typing import List, Optional

from hec.controllers.api_evcc import EvccApiClient
from hec.controllers.modbus_sma_inverter import InverterSmaModbusClient
from hec.core import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.models import EVCCLoadpointState, NetElectricityPriceInterval
from hec.utils.utils import convert_power, get_interval_from_list, is_daylight, send_email_with_attachments

logger = logging.getLogger(__name__)

_SHORTAGE_CONFIG = {
    '3m': (1.25, (-0.25, -0.10)),
    '5m': (1.50, (-0.25, -0.10)),
    '10m': (1.75, (-0.23, -0.07)),
}


class SystemMediator:

    def __init__(self, app_config, evcc_client: Optional[EvccApiClient],
                 inverter_client: Optional[InverterSmaModbusClient]):
        # Controllers
        self.evcc_client: Optional[EvccApiClient] = None
        self.inverter_client: Optional[InverterSmaModbusClient] = None
        # General
        self.app_config = app_config
        self.standard_max_peak_consumption_kw: float = 2.5
        self.current_max_peak_consumption_kw: float = 2.5
        self.app_mediator_goal: Optional[c.MediatorGoal] = None
        # Prices
        self.next_price_interval_at: datetime = datetime.now().astimezone()
        self.cur_buy_price: Optional[float] = None
        self.cur_sell_price: Optional[float] = None
        # Charging/evcc
        self.temp_charging_stopped_by_capacity: bool = False
        self.new_evcc_state: Optional[c.EVCCManualState] = None
        self.new_max_amps: Optional[int] = None
        self.car_was_connected: bool = False
        self.car_start_deadline: Optional[datetime] = datetime.now() - timedelta(days=999)
        self.force_charge_pushed: bool = False
        # Inverter
        self.buffer_before_pv_limit_change: int = 2
        self.last_pv_limit_change_time: Optional[datetime] = None
        self.new_inv_state: Optional[c.InverterManualState] = None
        self.new_inv_limit = None

        self._prepare_mediator_prerequisites(evcc_client, inverter_client)

    def _prepare_mediator_prerequisites(self, evcc_client, inverter_client):
        def degrade_app(reason):
            logger.warning(f'{reason}. Mediator functionality degraded.')
            GLOBAL_APP_STATE.set('app_state', c.AppStatus.DEGRADED)

        try:
            # Config variables
            mediator_config = self.app_config.get('mediator', {})
            self.standard_max_peak_consumption_kw = mediator_config.get('standard_max_peak_consumption_kw', 2.5)
            self.buffer_before_pv_limit_change = mediator_config.get('buffer_before_pv_limit_change', 3)

            if not mediator_config:
                degrade_app('No mediator config provided. Falling back to default values.')

            # Data sources/controllers
            if evcc_client and evcc_client.is_available:
                self.evcc_client = evcc_client
            else:
                degrade_app('No evcc_client provided.')

            inverter_status = inverter_client.get_operational_status() if inverter_client else None
            if inverter_client and inverter_status not in {c.InverterStatus.UNKNOWN, c.InverterStatus.OFFLINE}:
                self.inverter_client = inverter_client
            else:
                degrade_app('No valid inverter_client provided.')

            # App_state data
            if not GLOBAL_APP_STATE.get('app_state') == c.AppStatus.STARTING:
                if not GLOBAL_APP_STATE.get('p1_meter_data') or not GLOBAL_APP_STATE.get('electricity_prices_today'):
                    degrade_app('AppState missing p1_data and/or electricity_prices_today.')

            # Final validation
            if all([mediator_config, evcc_client, inverter_client, GLOBAL_APP_STATE.get('p1_meter_data')]):
                logger.info('All mediator prerequisites configured correctly.')

        except AttributeError as ae:
            logger.error(f'{ae}. Mediator prerequisites not configured correctly.')
        except KeyError as ke:
            logger.error(f'{ke}. Mediator prerequisites not configured correctly.')
        except TypeError as te:
            logger.error(f'{te}. Mediator prerequisites not configured correctly.')
        except Exception as ex:
            logger.error(f'{ex}. Mediator prerequisites not configured correctly.')

    def _prepare_data(self) -> bool:
        try:
            # Retrieve app state price data if last interval finished
            if (not self.next_price_interval_at or not self.cur_buy_price or not self.cur_sell_price or
                    self.next_price_interval_at < datetime.now().astimezone()):
                interval_list_today: List[NetElectricityPriceInterval] = GLOBAL_APP_STATE.get(
                    'electricity_prices_today')
                if not interval_list_today:
                    logger.error("Electricity prices for today are not available.")
                    return False

                # Determine the current price interval
                cur_interval = get_interval_from_list(datetime.now().astimezone(), interval_list_today)
                if not cur_interval:
                    logger.error("Unable to determine the current interval from the price list.")
                    return False

                # Check the active contract type
                cur_contract_type = cur_interval.active_contract_type
                if cur_contract_type == 'fixed':
                    logger.warning("Active contract type is set to fixed. Mediator cannot optimize.")
                    return False

                # Extract and set current buy/sell prices
                prices = cur_interval.net_prices_eur_per_kwh.get(cur_contract_type)
                if not prices or 'buy' not in prices or 'sell' not in prices:
                    logger.error(f"Prices are missing or incomplete.")
                    return False

                self.cur_buy_price = prices['buy']
                self.cur_sell_price = prices['sell']

                # Calculate and set the next price interval time
                next_price_interval_at = cur_interval.interval_start_local.astimezone() + timedelta(
                    minutes=cur_interval.resolution_minutes)
                self.next_price_interval_at = next_price_interval_at.replace(second=0)

            # Current peak calculation based on p1 meter or standard max
            cur_peak_kw = GLOBAL_APP_STATE.get('p1_meter_data', {}).get('monthly_power_peak_w', 0) / 1000
            self.current_max_peak_consumption_kw = max(cur_peak_kw, self.standard_max_peak_consumption_kw)

            return True

        except KeyError as ke:
            logger.error(f"KeyError while preparing data: {ke}")
        except AttributeError as ae:
            logger.error(f"AttributeError while preparing data: {ae}")
        except TypeError as te:
            logger.error(f"TypeError while preparing data: {te.with_traceback(te.__traceback__)}")
        except ValueError as ve:
            logger.error(f"ValueError while preparing data: {ve}")
        except IndexError as ie:
            logger.error(f"IndexError while preparing data: {ie}")
        except Exception as e:
            logger.error(f"Unexpected error while preparing data: {e}")
        return False

    def _handle_auto_mode(self):
        """Handles auto mode by determining controller states based on the mediator's goal."""
        self.app_mediator_goal = GLOBAL_APP_STATE.get('app_mediator_goal')

        # EVCC state logic
        self.new_evcc_state, self.new_max_amps = self._determine_evcc_state()
        logger.debug(f"Auto mode: evcc decided state {self.new_evcc_state}, max amps {self.new_max_amps}")

        # Inverter state logic
        self.new_inv_state = self._determine_inverter_state()
        logger.debug(f"Auto mode: inverter state {self.new_inv_state}")

    def _determine_evcc_state(self) -> (c.EVCCManualState, int):
        """Determines the new controller state based on the mediator goal."""
        goal = self.app_mediator_goal
        max_amps = self.evcc_client.max_current

        # EVCC state
        if goal == c.MediatorGoal.NO_CHARGING:
            return c.EVCCManualState.EVCC_CMD_STATE_OFF, max_amps
        elif goal == c.MediatorGoal.CHARGE_WITH_MINIMUM_SOLAR_POWER:
            return c.EVCCManualState.EVCC_CMD_STATE_MINPV, max_amps
        elif goal == c.MediatorGoal.CHARGE_WITH_ONLY_EXCESS_SOLAR_POWER:
            return c.EVCCManualState.EVCC_CMD_STATE_PV, max_amps
        elif goal == c.MediatorGoal.CHARGE_WHEN_SELL_PRICE_NEGATIVE:
            # State is PV when sell price negative (charge with excess), if not OFF
            return (c.EVCCManualState.EVCC_CMD_STATE_PV if self.cur_sell_price < 0 else
                    c.EVCCManualState.EVCC_CMD_STATE_OFF), max_amps
        elif goal == c.MediatorGoal.CHARGE_WHEN_BUY_PRICE_NEGATIVE:
            # State is OFF when buy price negative (charge with grid power), if not OFF
            return (c.EVCCManualState.EVCC_CMD_STATE_NOW if self.cur_buy_price < 0 else
                    c.EVCCManualState.EVCC_CMD_STATE_OFF), max_amps
        elif goal == c.MediatorGoal.CHARGE_NOW_WITH_CAPACITY_RATE:
            amps = convert_power(power_kw=self.current_max_peak_consumption_kw)
            max_amps = min(int(amps), self.evcc_client.max_current)
            return c.EVCCManualState.EVCC_CMD_STATE_NOW, max_amps
        elif goal == c.MediatorGoal.CHARGE_NOW_NO_CAPACITY_RATE:
            return c.EVCCManualState.EVCC_CMD_STATE_NOW, max_amps

    def _determine_inverter_state(self) -> c.InverterManualState:
        """Determines the new controller state based on the mediator goal."""
        if self.cur_buy_price < 0:
            return c.InverterManualState.INV_CMD_LIMIT_ZERO

        cur_state = GLOBAL_APP_STATE.get('evcc_manual_state', None)
        is_connected = GLOBAL_APP_STATE.get('evcc_loadpoint_state', {}).get('is_connected', True)
        if self.new_evcc_state == c.EVCCManualState.EVCC_CMD_STATE_PV and (
                (cur_state != self.new_evcc_state and is_connected) or
                (not self.car_was_connected and is_connected)):
            # Newly decided PV charge mode for EVCC and car is connected: start grace period
            self.car_start_deadline = datetime.now() + timedelta(minutes=2)
            logger.debug(f"Newly car connected or newly PV mode with car connected.")
            self.car_was_connected = is_connected
        is_charging = GLOBAL_APP_STATE.get('evcc_loadpoint_state').get('is_charging', False)
        car_charge_grace_period = (datetime.now() - self.car_start_deadline).total_seconds() < 0

        if self.cur_sell_price < 0 and not is_charging and not car_charge_grace_period:
            return c.InverterManualState.INV_CMD_LIMIT_TO_USE

        return c.InverterManualState.INV_CMD_LIMIT_STANDARD

    def _recalculate_charging_amperage(self) -> bool:
        """
        Recalculates state and charge amperage to avoid peak consumption.
        Returns True if there are changes, False otherwise.
        """
        try:
            # Load point state
            lp = EVCCLoadpointState.from_dict(GLOBAL_APP_STATE.get('evcc_loadpoint_state'))
            self.new_max_amps = lp.max_current if self.new_max_amps is None else self.new_max_amps

            # 2. Only when charging, about to charge or temporarily stopped AND not in CHARGE_NOW_NO_CAPACITY_RATE
            cur_state = GLOBAL_APP_STATE.get('evcc_manual_state', None)
            is_about_to_charge = self.new_evcc_state != cur_state
            if ((lp.is_charging or self.temp_charging_stopped_by_capacity or is_about_to_charge) and
                    self.app_mediator_goal != c.MediatorGoal.CHARGE_NOW_NO_CAPACITY_RATE):

                # 3. Base available kW
                grid_kw = GLOBAL_APP_STATE.get('p1_meter_data', {}).get('active_power_w', 0) / 1000
                threshold_kw = self.current_max_peak_consumption_kw - 0.125
                base_avail_kw = threshold_kw - grid_kw
                logger.debug(f"Grid power: {grid_kw:.2f} kW. Base available for charging: {base_avail_kw:.2f} kW")

                # 4. Adjust for recent shortages
                avail_kw = base_avail_kw
                average_import = GLOBAL_APP_STATE.get('average_grid_import_watts', 0)
                for window, (hi_mult, (low, high)) in _SHORTAGE_CONFIG.items():
                    avg_kw = average_import.get(window) / 1000
                    if avg_kw is None:  # Not enough readings yet
                        continue
                    shortage = threshold_kw - avg_kw
                    if low < shortage <= high:
                        logger.debug(f"Shortage over {window}: {shortage:.2f} kW → adjust by {shortage:.2f}")
                        avail_kw = min(avail_kw, base_avail_kw + shortage)
                    elif shortage <= low:
                        adjusted = base_avail_kw + shortage * hi_mult
                        logger.debug(f"High shortage over {window}: {shortage:.2f} kW → "
                                     f"adjust by {adjusted - base_avail_kw:.2f}")
                        avail_kw = min(avail_kw, adjusted)

                # 5. Compute new max_amp
                delta_amp = min(10, int(round(convert_power(power_kw=avail_kw))))  # Steps of 10 A
                max_amp = int(min(self.evcc_client.max_current, lp.charge_current + delta_amp))
                logger.debug(f"Computed avail: {avail_kw:.2f} kW → ΔA={delta_amp}, max_amp={max_amp}")

                # 6. Apply or stop
                if max_amp >= self.evcc_client.min_current:
                    # Only adjust if it’s a new value
                    if max_amp != lp.max_current:
                        self.new_max_amps = max_amp
                        logger.debug(f"Charging amp changed to {max_amp} kW")
                    if self.temp_charging_stopped_by_capacity:
                        self.temp_charging_stopped_by_capacity = False
                        self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_NOW
                        logger.debug(f"Charging resumed.")

                elif not self.temp_charging_stopped_by_capacity:
                    self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_OFF
                    self.temp_charging_stopped_by_capacity = True
                    self.new_max_amps = self.evcc_client.max_current
                    logger.debug(f"Charging stopped because {max_amp} below minimum of {self.evcc_client.min_current}")

            if self.new_evcc_state:
                enum_member = self.new_evcc_state
                if enum_member.value != lp.mode:
                    return True
            if self.new_max_amps != lp.max_current:
                return True
            return False

        except KeyError as ke:
            logger.error(f"KeyError during amperage calculation: {ke}")
        except AttributeError as ae:
            logger.error(f"AttributeError during amperage calculation: {ae} "
                         f"(self.new_evcc_state.value: {self.new_evcc_state})")
        except TypeError as te:
            logger.error(f"TypeError during amperage calculation: {te}")
        except ValueError as ve:
            logger.error(f"ValueError during amperage calculation: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error during amperage calculation: {e}")
        return False

    def _recalculate_inverter_limit(self) -> bool:
        """
        Recalculates inverter limit based on state.
        If in INV_CMD_LIMIT_TO_USE mode the target is zero grid flow +/- a dynamic buffer.
        A new target limit respects a quiet time before submitting and a grace time for
        a newly connected EV to start charging.
        Returns True if we should push a new limit to the inverter.
        """
        try:
            inv_data = GLOBAL_APP_STATE.get('inverter_data', {})
            cur_limit_w = inv_data.get('active_power_limit_watts', self.inverter_client.standard_power_limit)
            if self.new_inv_state == c.InverterManualState.INV_CMD_LIMIT_STANDARD:
                self.new_inv_limit = self.inverter_client.standard_power_limit
            elif self.new_inv_state == c.InverterManualState.INV_CMD_LIMIT_ZERO:
                self.new_inv_limit = 0
            elif self.new_inv_state == c.InverterManualState.INV_CMD_LIMIT_MANUAL:
                manual_limit = GLOBAL_APP_STATE.get('inverter_manual_limit', None)
                self.new_inv_limit = manual_limit if manual_limit is not None else None
            elif self.new_inv_state == c.InverterManualState.INV_CMD_LIMIT_TO_USE:
                now = datetime.now()
                # 1. If EV just plugged in, reset to standard and trigger charge start
                is_connected = GLOBAL_APP_STATE.get('evcc_loadpoint_state', {}).get('is_connected', False)
                if is_connected and not self.car_was_connected:
                    self.new_inv_state = c.InverterManualState.INV_CMD_LIMIT_STANDARD
                    self.new_inv_limit = self.inverter_client.standard_power_limit
                    self.car_start_deadline = now + timedelta(minutes=2)
                    self.car_was_connected = True
                    logger.debug(f"Car connected. Grace period for charge activated.")
                    return True
                self.car_was_connected = is_connected

                # 2. Current power data
                grid_w = GLOBAL_APP_STATE.get('p1_meter_data').get('active_power_w', 0)
                prod_w = GLOBAL_APP_STATE.get('inverter_data').get('pv_power_watts', 0)

                # 3. Compute dynamic buffer in Watt: positive -> favor export, negative -> favor import
                price_diff = self.cur_buy_price - abs(self.cur_sell_price)
                upper_limit_w = 90 if price_diff > 0 else -90
                lower_limit_w = 0

                # 4. Desired new inverter limit to achieve flow within limits
                home_use_w = grid_w + prod_w
                raw_limit_w = home_use_w + (upper_limit_w - lower_limit_w) / 2
                desired_limit_w = max(0, min(raw_limit_w, self.inverter_client.standard_power_limit))
                logger.debug(f"Raw limit: {desired_limit_w:.2f} W")

                # 5. Allowed to update the limit (quiet time or big change)
                can_update = False
                elapsed = (now - self.last_pv_limit_change_time).total_seconds() / 60 \
                    if self.last_pv_limit_change_time else 10
                # a. in case of big change -> override buffer
                if abs(raw_limit_w - cur_limit_w) >= 800:
                    can_update = True
                    logger.debug(f"Big change detected: {abs(raw_limit_w - cur_limit_w):.2f} W")
                # b. first ever or enough minutes elapsed
                elif not self.last_pv_limit_change_time:
                    can_update = True
                else:
                    if elapsed >= self.buffer_before_pv_limit_change:
                        can_update = True
                logger.debug(f"Can update: {can_update}")

                # 6. Long term import due to short term usage peaks
                avg_5m_import_w = GLOBAL_APP_STATE.get('average_grid_import_watts').get('5m', 0)
                avg_5m_prod_w = GLOBAL_APP_STATE.get('average_solar_production_watts').get('5m', 0)
                avg_5m_import_w = avg_5m_import_w if avg_5m_import_w is not None else 0
                avg_5m_prod_w = avg_5m_prod_w if avg_5m_prod_w is not None else 0
                prod_below_limit = avg_5m_prod_w < cur_limit_w - 200 and elapsed >= 5
                still_importing = avg_5m_import_w - desired_limit_w > 150
                if avg_5m_import_w is not None and still_importing and not prod_below_limit and elapsed >= 5:
                    desired_limit_w += avg_5m_import_w * 3
                    logger.debug(f"Still importing: {avg_5m_import_w:.2f} W. Desired limit: {desired_limit_w:.2f} W")

                # 7. Commit
                if can_update:
                    self.new_inv_limit = int(desired_limit_w)

            if ((self.new_inv_limit != cur_limit_w or
                GLOBAL_APP_STATE.get('inverter_manual_state') != self.new_inv_state)
                    and self.new_inv_limit is not None):
                return True
            return False

        except KeyError as ke:
            logger.error(f"KeyError during inverter limit calculation: {ke}")
        except AttributeError as ae:
            logger.error(f"AttributeError during inverter limit calculation: {ae}")
        except TypeError as te:
            logger.error(f"TypeError during inverter limit calculation: {te}")
        except ValueError as ve:
            logger.error(f"ValueError during inverter limit calculation: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error during inverter limit calculation: {e}")
        return False

    def _execute_inverter_state(self) -> None:
        if is_daylight(self.app_config):
            inv_data = GLOBAL_APP_STATE.get('inverter_data', {})
            cur_limit_w = inv_data.get('active_power_limit_watts', self.inverter_client.standard_power_limit)
            if self.new_inv_limit is None:
                return
            if self.new_inv_limit != cur_limit_w:
                logger.debug(f"Mediator pushed new inverter limit from {cur_limit_w} to {int(self.new_inv_limit)} W")
                self.inverter_client.set_active_power_limit(int(self.new_inv_limit))
                self.last_pv_limit_change_time = datetime.now()
            GLOBAL_APP_STATE.set('inverter_manual_state', self.new_inv_state)
        else:
            logger.info(f"Inverter changes not pushed because outside of daylight.")

    def _execute_evcc_state(self) -> None:
        if self.new_evcc_state is not None:
            self.evcc_client.set_charge_mode(self.new_evcc_state)
            GLOBAL_APP_STATE.set('evcc_manual_state', self.new_evcc_state)
        if self.new_max_amps is not None:
            self.evcc_client.set_max_current(self.new_max_amps)

    def _calculate_peak_consumption(self) -> bool:
        avg_import_watts = GLOBAL_APP_STATE.get('average_grid_import_watts', {})
        avg_5m = avg_import_watts.get('5m') / 1000 if avg_import_watts.get('5m') is not None else 0
        avg_10m = avg_import_watts.get('10m') / 1000 if avg_import_watts.get('10m') is not None else 0
        avg_15m = avg_import_watts.get('15m') / 1000 if avg_import_watts.get('15m') is not None else 0

        # Check if water heater is on
        now = datetime.now()
        water_heater_on = now.hour == 4 and now.minute <= 45

        # Notification conditions
        peak_exceeded = (
                (avg_5m > self.current_max_peak_consumption_kw * 1.1 or
                 avg_10m > self.current_max_peak_consumption_kw or
                 avg_15m > self.current_max_peak_consumption_kw)
                and not water_heater_on
        )

        if peak_exceeded:
            # Send notification email
            smtp_cfg = self.app_config.get('smtp', {})
            html_body = (f"\nCurrent month peak is {self.current_max_peak_consumption_kw:.2f} kWh"
                         f"\n{avg_5m:.2f} kWh over the last 5 minutes"
                         f"\n{avg_10m:.2f} kWh over the last 10 minutes"
                         f"\n{avg_15m:.2f} kWh over the last 15 minutes")
            send_email_with_attachments(
                smtp_config=smtp_cfg,
                sender_email=smtp_cfg.get('sender_email'),
                recipients=smtp_cfg.get('default_recipients'),
                subject=f"Peak consumption detected",
                html_body=html_body
            )
            GLOBAL_APP_STATE.set('app_state', c.AppStatus.ALARM)

            # Adjust EVCC and inverter states
            if (avg_10m > self.current_max_peak_consumption_kw * 1.05 or
                    avg_15m > self.current_max_peak_consumption_kw):
                self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_OFF
                self.new_inv_state = c.InverterManualState.INV_CMD_LIMIT_STANDARD
                self.new_inv_limit = self.inverter_client.standard_power_limit
                return True

        return False

    def run_system_mediation_logic(self):
        """
        Runs the system mediation logic: executes user-set manual overrides.
        If none, calculates optimal state of the controllers to achieve maximum cost saving or profit.
        """
        logger.debug(f"Running system mediation logic")
        # Prepare price data
        if not self._prepare_data():
            logger.error('Mediator encountered an error while preparing essential data and is skipping.')
            return

        # Check operating mode
        app_mode = GLOBAL_APP_STATE.get('app_operating_mode', c.OperatingMode.MODE_MANUAL)

        if app_mode == c.OperatingMode.MODE_MANUAL:
            # If mode manual, just set app states to be set to controllers
            self.new_evcc_state = GLOBAL_APP_STATE.get('evcc_manual_state')
            self.new_inv_state = GLOBAL_APP_STATE.get('inverter_manual_state')
        elif app_mode == c.OperatingMode.MODE_AUTO:
            # If mode auto: app decides controller states based on mediator goal
            self._handle_auto_mode()

        # Peak consumption avoidance
        peak_safety_override = self._calculate_peak_consumption()

        # Check for charging amperage adjustment to avoid peak consumption
        evcc_changes = False
        if not peak_safety_override:
            evcc_changes = self._recalculate_charging_amperage()
        if evcc_changes or peak_safety_override:
            logger.info(f"Evcc changes: {evcc_changes}. To push: {self.new_evcc_state} with {self.new_max_amps} A")
            self._execute_evcc_state()

        inverter_changes = False
        if not peak_safety_override:
            inverter_changes = self._recalculate_inverter_limit()
        if inverter_changes or peak_safety_override:
            logger.info(
                f"Inverter changes: {inverter_changes}. To push: {self.new_inv_state} with {self.new_inv_limit} W")
            self._execute_inverter_state()

        # Charge car grace period: force start
        car_charge_grace_period = (datetime.now() - self.car_start_deadline).total_seconds() < 0
        if car_charge_grace_period and not self.force_charge_pushed:
            logger.info(f"Car charge grace period activated.")
            self.evcc_client.sequence_force_pv_charging()
            self.force_charge_pushed = True
        self.force_charge_pushed = car_charge_grace_period

# if __name__ == "__main__":
#     mock_config = {'mediator': {'standard_max_peak_consumption_kw': 2.5, 'buffer_before_pv_limit_change': 3}}
#
#     mediator = SystemMediator(mock_config, None, None)
#     mediator.run_system_mediation_logic()
