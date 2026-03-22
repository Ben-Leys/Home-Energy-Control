# hec/logic_engine/system_mediator.py
import logging
import time
from datetime import datetime, timedelta, time
from typing import List, Optional

from hec.controllers.api_evcc import EvccApiClient
from hec.controllers.modbus_sma_inverter import InverterSmaModbusClient
from hec.core import constants as c, market_prices
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.models import EVCCLoadpointState, NetElectricityPriceInterval
from hec.core.tariff_manager import TariffManager
from hec.data_sources.api_p1_meter_homewizard import P1MeterHomewizardClient
from hec.database_ops.db_handler import DatabaseHandler
from hec.utils.utils import convert_power, get_interval_from_list, is_daylight, send_email_with_attachments

logger = logging.getLogger(__name__)

_SHORTAGE_CONFIG = {
    '3m': (1.25, (-0.25, -0.10)),
    '5m': (1.50, (-0.25, -0.10)),
    '10m': (1.75, (-0.23, -0.07)),
}


class SystemMediator:

    def __init__(self, app_config, evcc_client: Optional[EvccApiClient],
                 inverter_client: Optional[InverterSmaModbusClient], p1_client: Optional[P1MeterHomewizardClient]):
        # Controllers
        self.evcc_client: Optional[EvccApiClient] = None
        self.inverter_client: Optional[InverterSmaModbusClient] = None
        self.db_handler: Optional[DatabaseHandler] = None
        self.tariff_manager: Optional[TariffManager] = None
        self.p1_client: Optional[P1MeterHomewizardClient] = None
        # General
        self.app_config = app_config
        self.standard_max_peak_consumption_kw: float = 2.5
        self.current_max_peak_consumption_kw: float = 2.5
        self.app_mediator_goal: Optional[c.MediatorGoal] = None
        # Prices
        self.market: Optional[market_prices] = None
        # Charging/evcc
        self.temp_charging_stopped_by_capacity: bool = False
        self.state_before_charging_stopped: Optional[c.EVCCManualState] = None
        self.new_evcc_state: Optional[c.EVCCManualState] = None
        self.new_max_amps: Optional[int] = None
        self.last_max_amps: int = 0
        self.last_amps_push: int = int(time.time())
        self.car_was_connected: bool = False
        self.car_start_deadline: Optional[datetime] = datetime.now() - timedelta(days=999)
        self.force_charge_pushed: bool = False
        # Inverter
        self.buffer_before_pv_limit_change: int = 2
        self.last_pv_limit_change_time: Optional[datetime] = None
        self.new_inv_state: Optional[c.InverterManualState] = None
        self.new_inv_limit = None
        # Peak consumption
        self.ignore_start = time(4, 0)
        self.ignore_end = time(4, 45)
        self.last_email_sent_time: datetime | None = None
        self.is_peak_throttle_mode: bool = False
        self.inv_state_before_peak: Optional[c.InverterManualState] = None
        self.inv_limit_before_peak: Optional[int] = None
        self.evcc_state_before_peak: Optional[c.EVCCManualState] = None

        self._prepare_mediator_prerequisites(evcc_client, inverter_client, p1_client)

    @property
    def is_ignore_window_active(self) -> bool:
        """
        Returns True if the current time falls within the
        configured 'ignore' window (e.g., water heater window).
        """
        now_time = datetime.now().time()

        if self.ignore_start <= now_time <= self.ignore_end:
            return True

        return False

    def _prepare_mediator_prerequisites(self, evcc_client, inverter_client, p1_client):
        """
            Initializes hardware clients and validates configuration.
            Sets system state to DEGRADED if critical components are missing.
        """
        issues = []

        # Configuration
        conf = self.app_config.get('mediator', {})
        self.standard_max_peak_consumption_kw = conf.get('standard_max_peak_consumption_kw', 2.5)
        self.buffer_before_pv_limit_change = conf.get('buffer_before_pv_limit_change', 3)

        if not conf:
            issues.append("Missing 'mediator' config section")

        # Hardware Validation
        # EVCC
        if evcc_client and getattr(evcc_client, 'is_available', False):
            self.evcc_client = evcc_client
        else:
            issues.append("EVCC client unavailable")

        # Inverter
        inv_status = inverter_client.get_operational_status() if inverter_client else None
        if inverter_client and inv_status not in {c.InverterStatus.UNKNOWN, c.InverterStatus.OFFLINE}:
            self.inverter_client = inverter_client
        else:
            issues.append("Inverter client offline or invalid")

        # P1 Meter
        if p1_client and getattr(p1_client, 'is_initialized', False):
            self.p1_client = p1_client
        else:
            logger.warning("P1 client not initialized. Peak shaving will be disabled.")

        # Final Evaluation
        is_starting = GLOBAL_APP_STATE.get('app_state') == c.AppStatus.STARTING
        if issues and not is_starting:
            reason_str = ", ".join(issues)
            logger.warning(f"Mediator functionality degraded: {reason_str}")
            GLOBAL_APP_STATE.set('app_state', c.AppStatus.DEGRADED)
        elif not issues:
            logger.info("All mediator prerequisites configured correctly.")

    def _prepare_data(self) -> bool:
        try:
            if not GLOBAL_APP_STATE:
                return False

            if not self.market.refresh_if_needed(GLOBAL_APP_STATE):
                return False

            p1_data = GLOBAL_APP_STATE.get('p1_meter_data', {})
            cur_peak_kw = p1_data.get('monthly_power_peak_w', 0) / 1000
            self.current_max_peak_consumption_kw = max(cur_peak_kw, self.standard_max_peak_consumption_kw)

            return True

        except Exception as e:
            logger.error(f"Unexpected error while preparing data: {e}")
        return False

    def _handle_auto_mode(self):
        """Handles auto mode by determining controller states based on the mediator's goal."""
        self.app_mediator_goal = GLOBAL_APP_STATE.get('app_mediator_goal')

        # EVCC
        self.new_evcc_state, self.new_max_amps = self._determine_evcc_state()

        # Inverter
        self.new_inv_state = self._determine_inverter_state()

        # Battery
        # self.new_battery_mode = self._determine_battery_state()

        logger.debug(
            f"Auto Mode Logic: Goal={self.app_mediator_goal} | "
            f"EV={self.new_evcc_state}({self.new_max_amps}A) | "
            f"INV={self.new_inv_state} | "
            #f"BAT={self.new_bat_state} | "
        )

    def _determine_evcc_state(self) -> (c.EVCCManualState, int):
        """Determines the new controller state based on the mediator goal."""
        goal = self.app_mediator_goal
        max_amps = self.evcc_client.max_current
        lp = EVCCLoadpointState.from_dict(GLOBAL_APP_STATE.get('evcc_loadpoint_state'))

        # EVCC state
        if goal == c.MediatorGoal.NO_CHARGING:
            return c.EVCCManualState.EVCC_CMD_STATE_OFF, max_amps
        elif goal == c.MediatorGoal.CHARGE_WITH_MINIMUM_SOLAR_POWER:
            return c.EVCCManualState.EVCC_CMD_STATE_MINPV, max_amps
        elif goal == c.MediatorGoal.CHARGE_WITH_ONLY_EXCESS_SOLAR_POWER:
            if not is_daylight(self.app_config) or lp.smart_cost_active or lp.plan_active:
                max_amps = 11
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
        return c.EVCCManualState.EVCC_CMD_STATE_OFF, max_amps

    def _determine_inverter_state(self) -> c.InverterManualState:
        """Determines the new controller state based on the mediator goal."""
        if self.cur_buy_price < 0:
            return c.InverterManualState.INV_CMD_LIMIT_ZERO

        cur_state = GLOBAL_APP_STATE.get('evcc_manual_state', None)
        is_connected = GLOBAL_APP_STATE.get('evcc_loadpoint_state', {}).get('is_connected', True)
        if (self.new_evcc_state == c.EVCCManualState.EVCC_CMD_STATE_PV and
                self.car_start_deadline < datetime.now() and (
                (cur_state != self.new_evcc_state and is_connected) or
                (not self.car_was_connected and is_connected))):
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
            # self.new_max_amps = self.last_max_amps if self.new_max_amps is None else self.last_max_amps

            # 2. Only when charging, about to charge or temporarily stopped AND not in CHARGE_NOW_NO_CAPACITY_RATE
            cur_state = GLOBAL_APP_STATE.get('evcc_manual_state', None)
            is_about_to_charge = self.new_evcc_state != cur_state
            if (((lp.is_charging or self.temp_charging_stopped_by_capacity or is_about_to_charge) and
                    self.app_mediator_goal != c.MediatorGoal.CHARGE_NOW_NO_CAPACITY_RATE) and
                    (lp.smart_cost_active or lp.plan_active)):

                # 3. Base available kW
                grid_kw = GLOBAL_APP_STATE.get('p1_meter_data', {}).get('active_power_w', 0) / 1000
                threshold_kw = self.current_max_peak_consumption_kw - 0.175
                base_avail_kw = threshold_kw - grid_kw
                logger.debug(f"Grid power: {grid_kw:.2f} kW. Base available for charging: {base_avail_kw:.2f} kW")

                # 4. Adjust for recent shortages
                avail_kw = base_avail_kw
                average_import = GLOBAL_APP_STATE.get('average_grid_import_watts', 0)
                for window, (hi_mult, (low, high)) in _SHORTAGE_CONFIG.items():
                    avg_val = average_import.get(window)
                    if avg_val is None:  # Not enough readings yet
                        continue
                    avg_kw = avg_val / 1000
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
                max_amp = int(min(self.evcc_client.max_current, self.last_max_amps + delta_amp))
                logger.debug(f"Computed avail: {avail_kw:.2f} kW → ΔA={delta_amp}, max_amp={max_amp}")

                # 6. Apply or stop
                if max_amp >= self.evcc_client.min_current:
                    self.new_max_amps = max_amp
                    if max_amp != lp.max_current:
                        logger.debug(f"Charging amp changed to {max_amp} kW")
                    if self.temp_charging_stopped_by_capacity:
                        self.temp_charging_stopped_by_capacity = False
                        self.new_evcc_state = self.state_before_charging_stopped
                        logger.debug(f"Charging resumed.")

                elif not self.temp_charging_stopped_by_capacity:
                    self.state_before_charging_stopped = lp.mode
                    self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_OFF
                    self.temp_charging_stopped_by_capacity = True
                    self.new_max_amps = self.evcc_client.max_current
                    self.last_max_amps = 0
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
                # 0. If this state has just started, assume car just connected to start grace period
                # if GLOBAL_APP_STATE.get('inverter_manual_state') != self.new_inv_state: #  and now.minute % 15 == 0:
                #     logger.debug(f"Current inverter state: {GLOBAL_APP_STATE.get('inverter_manual_state')}. "
                #                  f"Reset car connection to False.")
                #     self.car_was_connected = False

                # 1. If EV just plugged in, reset to standard and trigger charge start
                is_connected = GLOBAL_APP_STATE.get('evcc_loadpoint_state', {}).get('is_connected', False)
                if is_connected and not self.car_was_connected:
                    self.new_inv_state = c.InverterManualState.INV_CMD_LIMIT_STANDARD
                    self.new_inv_limit = self.inverter_client.standard_power_limit
                    self.car_start_deadline = now + timedelta(minutes=1)
                    self.car_was_connected = True
                    logger.debug(f"Car connected. Grace period for charge activated.")
                    return True
                self.car_was_connected = is_connected

                # 2. Current power data
                grid_w = GLOBAL_APP_STATE.get('p1_meter_data').get('active_power_w', 0)
                prod_w = GLOBAL_APP_STATE.get('inverter_data').get('pv_power_watts', 0)

                # 3. Compute dynamic buffer in Watt: positive -> favor export, negative -> favor import
                price_diff = self.cur_buy_price - abs(self.cur_sell_price)
                if self.cur_sell_price < 0 and abs(self.cur_sell_price) < self.cur_buy_price / 6:
                    upper_limit_w = 180 if price_diff > 0 else -180
                elif self.cur_sell_price < 0 and abs(self.cur_sell_price) < self.cur_buy_price / 3:
                    upper_limit_w = 120 if price_diff > 0 else -120
                else:
                    upper_limit_w = 90 if price_diff > 0 else -90

                # Adjust upper limit based on recent limit adjustments
                if len(self.inverter_client.power_limit_timestamps) >= 4:
                    elapsed_time = now - self.inverter_client.power_limit_timestamps[0]
                    multiplier = 3 if elapsed_time < timedelta(minutes=20) else 2 if elapsed_time < timedelta(
                        minutes=60) else 1
                    upper_limit_w *= multiplier
                    logger.debug(f"Upper limit adjusted to {upper_limit_w} W (multiplier: {multiplier}).")

                # 4. Desired new inverter limit to achieve flow within limits
                home_use_w = grid_w + prod_w
                raw_limit_w = home_use_w + upper_limit_w / 3
                desired_limit_w = max(0, min(raw_limit_w, self.inverter_client.standard_power_limit))
                logger.debug(f"Raw limit: {desired_limit_w:.0f} W")

                # 5. Allowed to update the limit (quiet time or big change)
                can_update = False
                elapsed = (now - self.last_pv_limit_change_time).total_seconds() / 60 \
                    if self.last_pv_limit_change_time else 10
                # a. in case of big change -> override buffer
                if abs(raw_limit_w - cur_limit_w) >= 800:
                    can_update = True
                    logger.debug(f"Big change detected: {abs(raw_limit_w - cur_limit_w):.0f} W")
                # b. Otherwise, only if elapsed and home import is out of limits
                elif elapsed >= self.buffer_before_pv_limit_change:
                    if abs(desired_limit_w - cur_limit_w) > abs(upper_limit_w) / 2:
                        can_update = True
                        logger.debug(
                            f"Elapsed {elapsed:.1f} min and desired_limit_w {desired_limit_w:.0f} W vs cur_limit_w "
                            f"{cur_limit_w:.0f} W. Threshold: {upper_limit_w / 2:.0f} W."
                        )
                    else:
                        logger.debug(
                            f"Elapsed {elapsed:.1f} min but limit difference {abs(desired_limit_w - cur_limit_w):.0f} W"
                            f" is below threshold {upper_limit_w / 2:.0f} W. Skipping update."
                        )

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
        self.last_amps_push = int(time.time())
        if self.new_evcc_state is not None:
            self.evcc_client.set_charge_mode(self.new_evcc_state)
            GLOBAL_APP_STATE.set('evcc_manual_state', self.new_evcc_state)
        if self.new_max_amps is not None:
            self.evcc_client.set_max_current(self.new_max_amps)
            self.last_max_amps = self.new_max_amps

    def _handle_peak_consumption(self) -> bool:
        metrics = GLOBAL_APP_STATE.get('average_grid_import_watts', {})
        get_kw = lambda key: (metrics.get(key) or 0) / 1000
        avg = {k: get_kw(k) for k in ['5m', '10m', '15m']}

        # Detection logic
        limit = self.current_max_peak_consumption_kw
        peak_exceeded = (avg['5m'] > limit * 1.1 or avg['10m'] > limit or avg['15m'] > limit)
        should_throttle = (avg['5m'] > limit * 1.5 or avg['10m'] > limit * 1.05 or avg['15m'] > limit)

        # Notifications
        if peak_exceeded:
            def _handle_peak_notifications(avg_data):
                now = datetime.now()

                if self.is_ignore_window_active:
                    return

                if self.last_email_sent_time and (now - self.last_email_sent_time).total_seconds() < 300:
                    return

                def _send_peak_email(avg_data):
                    smtp_cfg = self.app_config.get('smtp', {})
                    limit = self.current_max_peak_consumption_kw

                    if avg_data['15m'] > limit:
                        status_msg = "peak exceeded!"
                    elif avg_data['10m'] > limit:
                        status_msg = "will exceed in 5 minutes"
                    else:
                        status_msg = "will exceed in 10 minutes"

                    html_content = [
                        f"<h3>{status_msg.capitalize()}</h3>",
                        f"Previous Month Peak: <b>{limit:.2f} kW</b><br><br>",
                        f"Current Averages:",
                        f"<ul>",
                        f"<li>5m: {avg_data['5m']:.2f} kW</li>",
                        f"<li>10m: {avg_data['10m']:.2f} kW</li>",
                        f"<li>15m: {avg_data['15m']:.2f} kW</li>",
                        f"</ul>"
                    ]

                    try:
                        send_email_with_attachments(
                            smtp_config=smtp_cfg,
                            sender_email=smtp_cfg.get('sender_email'),
                            recipients=smtp_cfg.get('default_recipients'),
                            subject=f"Peak consumption: {status_msg}",
                            html_body="".join(html_content)
                        )
                    except Exception as e:
                        logger.error(f"Failed to send peak alert email: {e}")

                _send_peak_email(avg_data)
                self.last_email_sent_time = now
                GLOBAL_APP_STATE.set('app_state', c.AppStatus.ALARM)

            _handle_peak_notifications(avg)

        # State Management
        if should_throttle:
            if not self.is_peak_throttle_mode:
                # Enter peak throttle mode
                self.is_peak_throttle_mode = True
                s = GLOBAL_APP_STATE
                self.inv_state_before_peak = s.get('inverter_manual_state', c.InverterManualState.INV_CMD_LIMIT_STANDARD)
                self.inv_limit_before_peak = s.get('active_power_limit_watts', self.inverter_client.standard_power_limit)
                self.evcc_state_before_peak = s.get('evcc_manual_state', c.EVCCManualState.EVCC_CMD_STATE_OFF)

                self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_OFF
                self.new_inv_state = c.InverterManualState.INV_CMD_LIMIT_STANDARD
                self.new_inv_limit = self.inverter_client.standard_power_limit

                logger.warning("Peak shaving ACTIVE: Throttling EV and Inverter.")

            return True
        elif self.is_peak_throttle_mode:
            # Exit peak throttle mode
            self.is_peak_throttle_mode = False
            self.new_inv_state = self.inv_state_before_peak
            self.new_inv_limit = self.inv_limit_before_peak
            self.new_evcc_state = self.evcc_state_before_peak
            logger.info("Peak shaving ENDED: Restoring previous states.")

        return False

    def run_system_mediation_logic(self):
        """
        Runs the system mediation logic: executes user-set manual overrides.
        If none, calculates optimal state of the controllers to achieve maximum cost saving or profit.
        """
        logger.debug(f"Running system mediation logic")

        # Prepare data
        if not self._prepare_data():
            logger.error('Mediator encountered an error while preparing essential data and is skipping.')
            return

        if self._handle_peak_consumption():
            pass
        else:
            # Check operating mode
            app_mode = GLOBAL_APP_STATE.get('app_operating_mode', c.OperatingMode.MODE_MANUAL)

            if app_mode == c.OperatingMode.MODE_MANUAL:
                # If mode manual, just set app states to be set to controllers
                self.new_evcc_state = GLOBAL_APP_STATE.get('evcc_manual_state')
                self.new_inv_state = GLOBAL_APP_STATE.get('inverter_manual_state')
                self.new_max_amps = GLOBAL_APP_STATE.get('evcc_manual_limit')

            elif app_mode == c.OperatingMode.MODE_AUTO:
                # If mode auto: app decides controller states based on mediator goal
                self._handle_auto_mode()

            # Peak consumption avoidance
            peak_safety_override = self._handle_peak_consumption()

            # Check for charging amperage adjustment to avoid peak consumption
            evcc_changes = False
            if not peak_safety_override:
                evcc_changes = self._recalculate_charging_amperage()
            if peak_safety_override or (evcc_changes and int(time.time()) - self.last_amps_push > 20):
                logger.info(f"Evcc changes: {evcc_changes}. To push: {self.new_evcc_state} with {self.new_max_amps} A")
                self._execute_evcc_state()

            inverter_changes = False
            if not peak_safety_override:
                inverter_changes = self._recalculate_inverter_limit()
            if inverter_changes or peak_safety_override:
                logger.info(
                    f"Inverter changes: {inverter_changes}. To push: {self.new_inv_state} with {self.new_inv_limit} W")
                self._execute_inverter_state()

            logger.debug(f"Battery mediator part.")
            manual = GLOBAL_APP_STATE.get("battery_manual_mode")
            battery_data = GLOBAL_APP_STATE.get("battery_data", {})
            actual = battery_data.get("mode", "UNKNOWN")

            is_charging = GLOBAL_APP_STATE.get('evcc_loadpoint_state').get('is_charging', False)
            new_battery_value = actual
            battery_stop_for_car_charge = False

            if is_charging and not is_daylight(self.app_config):
                # Charging without solar energy should never come from battery
                battery_stop_for_car_charge = True
                logger.debug(f"Battery stop for car charge without sunlight.")

            if battery_stop_for_car_charge:
                new_battery_value = c.BatteryState.BATTERY_OFF.value
            else:
                if manual is not None and manual is not c.BatteryState.BATTERY_AUTO:
                    logger.debug(f"Battery manual state: {manual.value}")
                    new_battery_value = manual.value
                elif manual is c.BatteryState.BATTERY_AUTO or manual is None:
                    new_battery_value = c.BatteryState.BATTERY_ON.value

            if new_battery_value != actual:
                logger.debug(f"Pushing new battery state: {new_battery_value}")
                self.p1_client.set_battery_mode(new_battery_value)

            # Charge car grace period: force start
            car_charge_grace_period = (datetime.now() - self.car_start_deadline).total_seconds() < 0
            if car_charge_grace_period and not self.force_charge_pushed:
                logger.info(f"Car charge grace period active. Force pv charging.")
                # Temporary disabled
                # self.evcc_client.sequence_force_pv_charging()
                self.force_charge_pushed = True
            self.force_charge_pushed = car_charge_grace_period

# if __name__ == "__main__":
#     mock_config = {'mediator': {'standard_max_peak_consumption_kw': 2.5, 'buffer_before_pv_limit_change': 3}}
#
#     mediator = SystemMediator(mock_config, None, None)
#     mediator.run_system_mediation_logic()
