# hec/logic_engine/system_mediator.py
import logging
import pandas as pd
import pytz
import time
from datetime import datetime, timedelta, time as dt_time
from typing import Optional

from hec.controllers.api_evcc import EvccApiClient
from hec.controllers.modbus_sma_inverter import InverterSmaModbusClient
from hec.core import constants as c, market_prices
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.models import EVCCLoadpointState
from hec.core.tariff_manager import TariffManager
from hec.data_sources.api_p1_meter_homewizard import P1MeterHomewizardClient
from hec.database_ops.db_handler import DatabaseHandler
from hec.utils.utils import convert_power, is_daylight, send_email_with_attachments

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
        self.app_mediator_goal: Optional[c.MediatorGoal] = None
        # Prices
        self.market = market_prices.MarketContext()
        # Charging/evcc
        self.new_evcc_state: Optional[c.EVCCManualState] = None
        self.new_max_amps: Optional[int] = None
        self.temp_charging_stopped_by_capacity: bool = False
        self.state_before_charging_stopped: Optional[c.EVCCManualState] = None
        self.last_max_amps: int = 0
        self.last_amps_push: int = int(time.time())
        # Inverter
        self.car_was_connected: bool = False
        self.last_evcc_state = None
        self.car_start_deadline = None
        self.last_solar_retry = None
        self.car_refused_to_charge = False
        self.buffer_before_pv_limit_change: int = 2
        self.last_pv_limit_change_time: Optional[datetime] = None
        self.new_inv_state: Optional[c.InverterManualState] = None
        self.new_inv_limit = None
        # Peak consumption
        self.standard_max_peak_consumption_kw: float = 2.5
        self.current_max_peak_kw: float = 2.5
        self.ignore_start = dt_time(4, 0)
        self.ignore_end = dt_time(4, 45)
        self.last_email_sent_time: datetime | None = None
        self.is_peak_throttle_mode: bool = False
        self.inv_state_before_peak: Optional[c.InverterManualState] = None
        self.inv_limit_before_peak: Optional[int] = None
        self.evcc_state_before_peak: Optional[c.EVCCManualState] = None
        self.bat_state_before_peak: Optional[c.BatteryState] = None
        # Battery
        self.new_bat_mode = c.BatteryState.BATTERY_ON
        self.battery_force_start_time = None
        self.last_processed_interval = None

        self._prepare_mediator_prerequisites(evcc_client, inverter_client, p1_client)

    @property
    def is_ignore_window_active(self) -> bool:
        """
        Returns True if the current time falls within the
        configured 'ignore' window (e.g., water heater window).
        """
        now_time = datetime.now().time()  # Local time

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

            if not self.market.refresh_if_needed():
                return False

            p1_data = GLOBAL_APP_STATE.get('p1_meter_data', {})
            cur_peak_kw = p1_data.get('monthly_power_peak_w', 0) / 1000
            self.current_max_peak_kw = max(cur_peak_kw, self.standard_max_peak_consumption_kw)

            return True

        except Exception as e:
            logger.error(f"Unexpected error while preparing data: {e}")
        return False

    def _handle_auto_mode(self):
        """Handles auto mode by determining controller states based on the mediator's goal."""
        self.app_mediator_goal = GLOBAL_APP_STATE.get('app_mediator_goal')

        # EVCC
        self._determine_evcc_state()
        self._recalculate_charging_amperage()

        # Inverter
        self._determine_inverter_state()
        self._recalculate_inverter_limit()

        # Battery
        self._determine_battery_state()

        logger.debug(
            f"Auto Mode Logic: Goal={self.app_mediator_goal} | "
            f"EV={self.new_evcc_state}({self.new_max_amps}A) | "
            f"INV={self.new_inv_state} | "
            f"BAT={self.new_bat_mode}"
        )

    def _handle_manual_mode(self):
        """Syncs manual UI settings to the mediator's target variables."""
        self.new_evcc_state = GLOBAL_APP_STATE.get('evcc_manual_state')
        self.new_inv_state = GLOBAL_APP_STATE.get('inverter_manual_state')
        self.new_max_amps = GLOBAL_APP_STATE.get('evcc_manual_limit')
        self.new_bat_mode = GLOBAL_APP_STATE.get("battery_manual_mode")
        self._recalculate_inverter_limit()

    def _determine_evcc_state(self):
        """Determines the new evcc controller state based on the mediator goal."""
        goal = self.app_mediator_goal

        if goal == c.MediatorGoal.NO_CHARGING:
            self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_OFF

        elif goal == c.MediatorGoal.CHARGE_WITH_MINIMUM_SOLAR_POWER:
            self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_MINPV

        elif goal == c.MediatorGoal.CHARGE_WITH_ONLY_EXCESS_SOLAR_POWER:
            self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_PV

        elif goal == c.MediatorGoal.CHARGE_WHEN_SELL_PRICE_NEGATIVE:
            self.new_evcc_state = (c.EVCCManualState.EVCC_CMD_STATE_PV
                                   if self.market.sell_price < 0 else c.EVCCManualState.EVCC_CMD_STATE_OFF)

        elif goal == c.MediatorGoal.CHARGE_WHEN_BUY_PRICE_NEGATIVE:
            self.new_evcc_state = (c.EVCCManualState.EVCC_CMD_STATE_NOW
                                   if self.market.buy_price < 0 else c.EVCCManualState.EVCC_CMD_STATE_OFF)

        elif goal in {c.MediatorGoal.CHARGE_NOW_WITH_CAPACITY_RATE, c.MediatorGoal.CHARGE_NOW_NO_CAPACITY_RATE}:
            self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_NOW

        else:
            self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_OFF

    def _recalculate_charging_amperage(self):
        """
        Recalculates state and charge amperage to avoid peak consumption.
        """
        # Load point state
        lp = EVCCLoadpointState.from_dict(GLOBAL_APP_STATE.get('evcc_loadpoint_state'))
        cur_state = GLOBAL_APP_STATE.get('evcc_manual_state', None)

        # Is amperage calculation needed?
        is_managed_charging = (lp.smart_cost_active or lp.plan_active or
                               self.app_mediator_goal == c.MediatorGoal.CHARGE_NOW_WITH_CAPACITY_RATE)
        if not is_managed_charging or self.app_mediator_goal == c.MediatorGoal.CHARGE_NOW_NO_CAPACITY_RATE:
            self.new_max_amps = self.evcc_client.max_current
            return

        # Is charging, starting or paused?
        is_starting = (self.new_evcc_state != cur_state and
                       self.new_evcc_state != c.EVCCManualState.EVCC_CMD_STATE_OFF)

        should_calculate = lp.is_charging or self.temp_charging_stopped_by_capacity or is_starting
        if not should_calculate:
            return

        # Starting charging, force 6A
        if is_starting and is_managed_charging and not lp.is_charging:
            self.new_max_amps = self.evcc_client.min_current
            logger.info(f"Initial charge command: Starting at {self.new_max_amps}A.")
            return

        # Calculate Available Power
        grid_kw = GLOBAL_APP_STATE.get('p1_meter_data', {}).get('active_power_w', 0) / 1000
        threshold_kw = self.current_max_peak_kw - 0.15
        avail_kw = threshold_kw - grid_kw
        logger.debug(f"Grid power: {grid_kw:.2f} kW. Base available for charging: {avail_kw:.2f} kW")

        # Shortage Adjustments
        average_import = GLOBAL_APP_STATE.get('average_grid_import_watts', {})
        for window, (hi_mult, (low, high)) in _SHORTAGE_CONFIG.items():
            avg_kw = (average_import.get(window, 0)) / 1000
            shortage = threshold_kw - avg_kw

            if shortage <= low:
                avail_kw = min(avail_kw, (threshold_kw - grid_kw) + (shortage * hi_mult))
                logger.debug(f"High shortage over {window}: {shortage:.2f} kW → adjust to {avail_kw:.2f}")
            elif low < shortage <= high:
                avail_kw = min(avail_kw, (threshold_kw - grid_kw) + shortage)
                logger.debug(f"High shortage over {window}: {shortage:.2f} kW → adjust to {avail_kw:.2f}")

        # Translate kW to Amps
        delta_amp = min(5, int(round(convert_power(power_kw=avail_kw))))
        target_amp = min(self.evcc_client.max_current, self.last_max_amps + delta_amp)
        logger.debug(f"Avail: {avail_kw:.2f} kW → ΔA={delta_amp}, target_amp={target_amp}")

        # Stop or start
        if target_amp >= self.evcc_client.min_current:
            self._resume_charging(target_amp)
        else:
            self._pause_charging(target_amp, lp)

    def _pause_charging(self, target_amp, lp):
        if not self.temp_charging_stopped_by_capacity:
            self.state_before_charging_stopped = lp.mode
            self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_OFF
            self.new_max_amps = self.evcc_client.max_current
            self.temp_charging_stopped_by_capacity = True
            self.last_max_amps = 0
            logger.warning(f"Charging paused because {target_amp} below minimum.")

    def _resume_charging(self, target_amp):
        self.new_max_amps = target_amp

        if self.temp_charging_stopped_by_capacity:
            self.temp_charging_stopped_by_capacity = False
            self.new_evcc_state = self.state_before_charging_stopped
            logger.info(f"Amperage calculator: charging resumed.")

    def _determine_inverter_state(self):
        """Determines the new controller state based on the mediator goal."""
        now = datetime.now(tz=pytz.UTC)
        lp = GLOBAL_APP_STATE.get('evcc_loadpoint_state', {})
        is_connected = lp.get('is_connected', False)
        is_charging = lp.get('is_charging', False)

        # 1. Grace period triggers
        # Reset refusal flag
        if not is_connected:
            self.car_refused_to_charge = False

        newly_connected = is_connected and not self.car_was_connected
        mode_changed_to_pv = (
                    self.new_evcc_state in [c.EVCCManualState.EVCC_CMD_STATE_PV, c.EVCCManualState.EVCC_CMD_STATE_MINPV]
                    and self.new_evcc_state != self.last_evcc_state)

        # Trigger A
        if newly_connected or mode_changed_to_pv:
            self.car_start_deadline = now + timedelta(minutes=2)
            self.car_refused_to_charge = False
            logger.info("Inverter grace period started: Car newly connected or mode changed.")

        # Trigger B
        is_willing = self.new_evcc_state in [c.EVCCManualState.EVCC_CMD_STATE_PV,
                                             c.EVCCManualState.EVCC_CMD_STATE_MINPV]

        if (self.market.sell_price < 0 and is_connected and is_willing and not is_charging
                and not self.car_refused_to_charge):

            # Initialize timer
            if self.last_solar_retry is None:
                self.last_solar_retry = now

            elif (now - self.last_solar_retry).total_seconds() > 2700:
                self.car_start_deadline = now + timedelta(minutes=2)
                self.last_solar_retry = now
                logger.info("Inverter grace period started: Periodic 30-min solar test.")
        else:
            # If the car is actively charging, reset the timer
            if is_charging:
                self.last_solar_retry = None
                self.car_refused_to_charge = False

        # 2. State evaluation
        # Currently in grace period
        in_grace_period = self.car_start_deadline is not None and now < self.car_start_deadline

        # Grace period ended and car not charging
        if self.car_start_deadline is not None and now >= self.car_start_deadline:
            if not is_charging and is_connected:
                self.car_refused_to_charge = True
                logger.debug("Car did not start charging during grace period. Suspending retries.")
            self.car_start_deadline = None

        # Track states for next loop
        self.car_was_connected = is_connected
        self.last_evcc_state = self.new_evcc_state

        # 3. Apply rules
        # A: We pay to use grid power. Turn off inverter immediately.
        if self.market.buy_price < 0:
            self.new_inv_state = c.InverterManualState.INV_CMD_LIMIT_ZERO
            return

        # B: If we are in a grace period or actively charging, we MUST have full production
        if in_grace_period or is_charging:
            self.new_inv_state = c.InverterManualState.INV_CMD_LIMIT_STANDARD
            return

        # C: We pay to export. Limit production to home usage only.
        if self.market.sell_price < 0:
            self.new_inv_state = c.InverterManualState.INV_CMD_LIMIT_TO_USE
            return

        # D: Prices are positive, no special limits. Standard production.
        self.new_inv_state = c.InverterManualState.INV_CMD_LIMIT_STANDARD

    def _recalculate_inverter_limit(self):
        """
        Sets the self.new_inv_limit based on the decided inverter state.
        For TO_USE mode, applies deadbands and hysteresis to protect inverter flash memory.
        """
        inv_data = GLOBAL_APP_STATE.get('inverter_data', {})
        cur_limit_w = inv_data.get('active_power_limit_watts', self.inverter_client.standard_power_limit)

        # 1. Simple states
        if self.new_inv_state == c.InverterManualState.INV_CMD_LIMIT_STANDARD:
            self.new_inv_limit = self.inverter_client.standard_power_limit
            return

        if self.new_inv_state == c.InverterManualState.INV_CMD_LIMIT_ZERO:
            self.new_inv_limit = 0
            return

        if self.new_inv_state == c.InverterManualState.INV_CMD_LIMIT_MANUAL:
            self.new_inv_limit = GLOBAL_APP_STATE.get('inverter_manual_limit', cur_limit_w)
            return

        # 2. Home usage state
        if self.new_inv_state == c.InverterManualState.INV_CMD_LIMIT_TO_USE:
            now = datetime.now(tz=pytz.UTC)

            # 1. Current power data
            grid_w = GLOBAL_APP_STATE.get('p1_meter_data', {}).get('active_power_w', 0)
            prod_w = GLOBAL_APP_STATE.get('inverter_data', {}).get('pv_power_watts', 0)
            bat_w = GLOBAL_APP_STATE.get('battery_data', {}).get('power_w', 0)
            bat_w = min(bat_w, 0)
            home_use_w = grid_w + prod_w - bat_w

            # 2. Dynamic buffer calculation based on market prices
            buy_price = self.market.buy_price or 1.0
            price_ratio = abs(self.market.sell_price) / buy_price
            price_diff = self.market.buy_price - abs(self.market.sell_price)

            if price_ratio < 0.166:  # 1/6th
                base_buffer = 180 if price_diff > 0 else -180
            elif price_ratio < 0.333:  # 1/3rd
                base_buffer = 120 if price_diff > 0 else -120
            else:
                base_buffer = 90 if price_diff > 0 else -90

            # Adjust buffer if changing the limit too frequently (Flash memory protection)
            multiplier = self._get_limit_frequency_multiplier(now)
            upper_limit_w = base_buffer * multiplier

            # 3. Target calculation
            raw_limit_w = home_use_w + (upper_limit_w / 3)
            desired_limit_w = max(0, min(raw_limit_w, self.inverter_client.standard_power_limit))

            # 4. Evaluate update condition
            elapsed_min = (now - self.last_pv_limit_change_time).total_seconds() / 60 \
                if self.last_pv_limit_change_time else 10

            is_big_change = abs(raw_limit_w - cur_limit_w) >= 800
            is_time_elapsed = elapsed_min >= self.buffer_before_pv_limit_change
            is_over_threshold = abs(desired_limit_w - cur_limit_w) > (abs(upper_limit_w) / 2)

            can_update = is_big_change or (is_time_elapsed and is_over_threshold)

            # 5. Long-term import correction
            if elapsed_min >= 5:
                avg_5m_import_w = GLOBAL_APP_STATE.get('average_grid_import_watts', {}).get('5m', 0)
                avg_5m_prod_w = GLOBAL_APP_STATE.get('average_solar_production_watts', {}).get('5m', 0)

                # Are we importing while the solar is artificially capped?
                prod_is_capped = avg_5m_prod_w >= (cur_limit_w - 200)
                still_importing = (avg_5m_import_w - desired_limit_w) > 150

                if still_importing and prod_is_capped:
                    desired_limit_w += (avg_5m_import_w * 3)
                    can_update = True
                    logger.debug(f"Sustained import detected. Boosting limit to {desired_limit_w:.0f} W")

            # 6 Minimum for battery charging
            # If battery needs energy (SOC < 95%) and isn't blocked, ensure at least 1800W
            battery_records = GLOBAL_APP_STATE.get("battery_records") or []
            bat_needing_charge = sum(1 for b in battery_records if b.get("state_of_charge_pct", 0) < 95)

            # Check if battery is in a state where it CAN charge
            is_charging_allowed = self.new_bat_mode not in [c.BatteryState.BATTERY_OFF,
                                                            c.BatteryState.BATTERY_BLOCK_CHARGE]

            if bat_needing_charge > 0 and is_charging_allowed:
                battery_data = GLOBAL_APP_STATE.get("battery_data", {})
                battery_count = max(1, battery_data.get('battery_count', 1))
                max_charge_w = battery_data.get("max_consumption_w", 0)
                max_charge_per_bat = max_charge_w / battery_count
                floor_w = max_charge_per_bat * bat_needing_charge
                if desired_limit_w < floor_w:
                    new_desired_limit_w = floor_w
                    logger.debug(f"{bat_needing_charge} batteries < 95%: Boosting inverter limit from "
                                 f"{desired_limit_w:.0f}W to {new_desired_limit_w}W for charging.")
                    desired_limit_w = new_desired_limit_w

                    # Force an update if the current limit is significantly below the floor
                    if cur_limit_w < floor_w - 200:
                        can_update = True

            # 7. Apply decision
            if can_update:
                # Final clamp to hardware limits
                self.new_inv_limit = int(max(0, min(desired_limit_w, self.inverter_client.standard_power_limit)))
            else:
                self.new_inv_limit = int(cur_limit_w)

    def _get_limit_frequency_multiplier(self, now: datetime) -> int:
        """Helper to widen the buffer if we've been sending too many commands."""
        timestamps = self.inverter_client.power_limit_timestamps
        if len(timestamps) < 4:
            return 1

        elapsed_time = now - timestamps[0]
        if elapsed_time < timedelta(minutes=20):
            return 3
        if elapsed_time < timedelta(minutes=60):
            return 2
        return 1

    def _determine_battery_state(self):
        """
        Determines target battery mode with advanced peak-shaving safety.
        Calculates remaining 15-min energy budget before allowing Force Charge.
        """
        now = datetime.now(tz=pytz.UTC)
        lp = GLOBAL_APP_STATE.get('evcc_loadpoint_state', {})
        bat_data = GLOBAL_APP_STATE.get("battery_data", {})
        bat_records = GLOBAL_APP_STATE.get("battery_records", [])
        lowest_soc = min([b.get("state_of_charge_pct", 5) for b in bat_records]) if bat_records else 5
        would_block_discharge = False

        # 1. Absolute rules:
        # A: Don't allow empty for too long
        empty_since = GLOBAL_APP_STATE.get("empty_since")
        if lowest_soc < 1:
            if empty_since is None:
                GLOBAL_APP_STATE.set("empty_since", now)
                empty_since = now
        else:
            GLOBAL_APP_STATE.set("empty_since", None)
            empty_since = None
        empty_too_long = (now - empty_since) > timedelta(hours=12) if empty_since else False

        # B: Car is charging
        if lp.get('is_charging', False):
            self.new_bat_mode = c.BatteryState.BATTERY_BLOCK_DISCHARGE
            # Clear any ongoing force-charge timers
            self.battery_force_start_time = None
            return

        # C: SOC < 2%
        if lowest_soc < 2 and not empty_too_long:
            would_block_discharge = True

        # 2. Prediction plan
        plan_df: pd.DataFrame = GLOBAL_APP_STATE.get("prediction_plan_df")
        if plan_df is None or plan_df.empty:
            self.new_bat_mode = c.BatteryState.BATTERY_ON
            return

        # A. Get the current instruction from the DataFrame
        try:
            current_row = plan_df[plan_df.index <= now].iloc[-1]
        except (IndexError, AttributeError, KeyError):
            logger.warning("Could not parse prediction_plan for current time. Defaulting to BATTERY_ON.")
            self.new_bat_mode = c.BatteryState.BATTERY_ON
            return

        # B. Handle Force Charge Timer
        is_force_plan = False
        plan_minutes_limit = 15.0
        current_interval_start = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)

        try:
            is_force_plan = bool(current_row.get("force_c", False))
            plan_minutes_limit = float(current_row.get("force_time", 15))
            current_interval_start = current_row.name
        except (IndexError, AttributeError):
            pass

        # Override if empty for too long
        if empty_too_long:
            if not is_force_plan:
                logger.info("Battery empty > 12h. Triggering 5min maintenance charge.")
                is_force_plan = True
                plan_minutes_limit = 5.0

        if is_force_plan:
            if self.last_processed_interval != current_interval_start:
                self.battery_force_start_time = now
                self.last_processed_interval = current_interval_start
                logger.info(f"New Force Charge interval started at {current_interval_start}.")

            elapsed_minutes = (now - self.battery_force_start_time).total_seconds() / 60

            # Recalculate budget every loop
            safe_minutes = self._calculate_safe_force_charge_minutes()

            # If the window is closing or budget is tight, safe_minutes will drop to 0
            if elapsed_minutes < plan_minutes_limit and safe_minutes >= 1:
                self.new_bat_mode = c.BatteryState.BATTERY_FORCE_CHARGE
                remaining_plan = round(plan_minutes_limit - elapsed_minutes, 1)
                logger.info(f"Force Charging: {remaining_plan}min left in plan. Safety buffer: {safe_minutes}min.")
                return
            else:
                if elapsed_minutes >= plan_minutes_limit:
                    logger.info(f"Force Charge complete: Reached plan limit of {plan_minutes_limit} min.")
                else:
                    logger.warning(f"Force Charge aborted: Peak budget exhausted (Safety: {safe_minutes}min).")

                self.new_bat_mode = c.BatteryState.BATTERY_BLOCK_DISCHARGE
                # We don't reset battery_force_start_time here yet,
                # so we don't restart immediately in the next loop of the same interval.
                return
        else:
            self.battery_force_start_time = None
            self.last_processed_interval = None

        # C. Handle Blocking (Charge / Discharge)
        is_block_c = bool(current_row.get("block_c", False))
        is_block_d = bool(current_row.get("block_d", False))

        if (is_block_d and is_block_c) or (is_block_c and would_block_discharge):
            self.new_bat_mode = c.BatteryState.BATTERY_OFF
            return

        if is_block_c:
            self.new_bat_mode = c.BatteryState.BATTERY_BLOCK_CHARGE
            return

        if is_block_d or would_block_discharge:
            avg_kw_2m = (GLOBAL_APP_STATE.get('average_grid_import_watts', {}).get("2m", 0)) / 1000
            may_cause_peak = avg_kw_2m > self.current_max_peak_kw
            if not may_cause_peak:
                self.new_bat_mode = c.BatteryState.BATTERY_BLOCK_DISCHARGE
                return

        self.new_bat_mode = c.BatteryState.BATTERY_ON

    def _calculate_safe_force_charge_minutes(self) -> int:
        """
        Calculates how many minutes we can charge at max power without
        exceeding the 15-minute average capacity limit.
        """
        now = datetime.now()
        bat_data = GLOBAL_APP_STATE.get("battery_data", {})
        # 1. Where are we in the current 15-minute block
        # (at 14:07, we are 7 minutes into the [14:00-14:15] window)
        minutes_passed = now.minute % 15
        seconds_passed = (minutes_passed * 60) + now.second
        seconds_remaining = 900 - seconds_passed

        # 2. Get current net import
        p1_data = GLOBAL_APP_STATE.get('p1_meter_data', {})
        current_grid_w = p1_data.get('active_power_w', 0)

        # 3. Get battery impact
        # If we force charge, the house consumption increases by the battery's max intake
        max_bat_w = bat_data.get("max_consumption_w", 1600)
        projected_total_w = current_grid_w + max_bat_w

        # 4. Math: How much 'Energy Debt' can we afford?
        # Total allowed Joules (Ws) in 15 mins = Limit_W * 900s
        allowed_ws = self.current_max_peak_kw * 1000 * 900

        # Estimated Joules already spent (using 5m average as a proxy for the current window)
        avg_5m_w = GLOBAL_APP_STATE.get('average_grid_import_watts', {}).get('5m', 0)
        spent_ws = avg_5m_w * seconds_passed

        remaining_ws = allowed_ws - spent_ws

        if remaining_ws <= 0:
            return 0

        # 5. How many seconds can we sustain 'projected_total_w'?
        # seconds = remaining_budget / projected_draw
        # We add a 10% safety buffer
        safe_seconds = (remaining_ws / projected_total_w) * 0.9

        # Clip to the end of the current 15-minute window
        actual_safe_seconds = min(safe_seconds, seconds_remaining)

        return int(actual_safe_seconds // 60)

    def _apply_inverter_state(self):
        """
        Executes the planned inverter state and limits against the hardware API.
        Maintains a 'Quiet Time' and daylight safety check.
        """
        # Don't talk to the inverter at night
        if not is_daylight(self.app_config):
            logger.info("Inverter: Skipping updates (outside of daylight hours).")
            return

        # Identify current hardware state
        inv_data = GLOBAL_APP_STATE.get('inverter_data', {})
        cur_limit_w = inv_data.get('active_power_limit_watts', self.inverter_client.standard_power_limit)
        cur_manual_state = GLOBAL_APP_STATE.get('inverter_manual_state')

        # Validation
        if self.new_inv_limit is None:
            logger.debug("Inverter: No new limit target defined. Skipping.")
            return

        # Physical update is needed
        limit_changed = int(self.new_inv_limit) != int(cur_limit_w)
        state_changed = self.new_inv_state != cur_manual_state

        if not limit_changed and not state_changed:
            return

        try:
            logger.info(
                f"Inverter: Pushing update. Mode: {self.new_inv_state.name} | "
                f"Limit: {cur_limit_w}W -> {int(self.new_inv_limit)}W"
            )

            success = self.inverter_client.set_active_power_limit(int(self.new_inv_limit))

            if success:
                self.last_pv_limit_change_time = datetime.now(tz=pytz.UTC)
                GLOBAL_APP_STATE.set('inverter_manual_state', self.new_inv_state)
                GLOBAL_APP_STATE.set('inverter_manual_limit', self.new_inv_limit)
            else:
                logger.error("Inverter: API rejected the limit update.")

        except Exception as e:
            logger.error(f"Inverter: Critical failure during execution: {e}")

    def _apply_evcc_state(self):
        """
        Executes the planned EVCC mode and amperage limits against the EVCC API.
        Includes a 20-second throttle to prevent API flooding.
        """
        now_ts = int(time.time())
        lp = EVCCLoadpointState.from_dict(GLOBAL_APP_STATE.get('evcc_loadpoint_state'))
        cur_manual_state = GLOBAL_APP_STATE.get('evcc_manual_state')

        # Validation
        state_changed, amps_changed = False, False
        if self.new_evcc_state is not None:
            state_changed = self.new_evcc_state != cur_manual_state
        if self.new_max_amps is not None:
            amps_changed = int(self.new_max_amps) != int(lp.max_current)

        # Changes are needed?
        if not state_changed and not amps_changed:
            return

        # Throttle Logic
        # We allow the push IF 20s have passed OR if it's a critical State change
        # (e.g. stopping because of a peak)
        time_since_last = now_ts - self.last_amps_push
        is_throttled = time_since_last < 20

        if is_throttled and not state_changed:
            return

        try:
            # Execute mode change
            if state_changed:
                logger.info(f"EVCC: Mode change {cur_manual_state} -> {self.new_evcc_state.name}")
                success_mode = self.evcc_client.set_charge_mode(self.new_evcc_state)
                if success_mode:
                    GLOBAL_APP_STATE.set('evcc_manual_state', self.new_evcc_state)

            # Execute Amperage change
            if amps_changed:
                logger.info(f"EVCC: Setting max current to {int(self.new_max_amps)}A")
                success_amps = self.evcc_client.set_max_current(int(self.new_max_amps))
                if success_amps:
                    self.last_max_amps = int(self.new_max_amps)
                    GLOBAL_APP_STATE.set('evcc_manual_limit', int(self.new_max_amps))

            self.last_amps_push = now_ts

        except Exception as e:
            logger.error(f"EVCC: Critical failure during execution: {e}")

    def _apply_battery_state(self):
        """
        Executes the planned battery state against the P1 Client API.
        """
        if not self.new_bat_mode:
            return
        # Get current mode
        bat_data = GLOBAL_APP_STATE.get("battery_data", {})
        current_state = c.BatteryState.BATTERY_OFF
        if not bat_data:
            logger.error("No battery data available. Will apply new state anyway.")
        else:
            mode = bat_data.get("mode")
            perms = bat_data.get("permissions", [])

            if mode == "standby":
                current_state = c.BatteryState.BATTERY_OFF
            elif mode == "to_full":
                current_state = c.BatteryState.BATTERY_FORCE_CHARGE
            elif perms:
                charge = "charge_allowed" in perms
                discharge = "discharge_allowed" in perms
                if charge and discharge:
                    current_state = c.BatteryState.BATTERY_ON
                elif discharge and not charge:
                    current_state = c.BatteryState.BATTERY_BLOCK_CHARGE
                elif charge and not discharge:
                    current_state = c.BatteryState.BATTERY_BLOCK_DISCHARGE

        if current_state == self.new_bat_mode:
            logger.debug(f"Battery is already in state: {current_state.name}. No action needed.")
            return

        try:
            logger.info(f"Transitioning battery: {current_state.name} -> {self.new_bat_mode.name}")
            success = self.p1_client.set_battery_mode(self.new_bat_mode)
            GLOBAL_APP_STATE.set('battery_manual_mode', self.new_bat_mode)
            if success:
                logger.debug(f"Battery successfully set to {self.new_bat_mode.name}")
            else:
                logger.error(f"P1 Client failed to apply state {self.new_bat_mode.name}")

        except Exception as e:
            logger.error(f"Failed to update battery state: {e}")

    def _handle_peak_consumption(self) -> bool:
        metrics = GLOBAL_APP_STATE.get('average_grid_import_watts', {})
        get_kw = lambda key: (metrics.get(key) or 0) / 1000
        avg = {k: get_kw(k) for k in ['5m', '10m', '15m']}

        # Detection logic
        limit = self.current_max_peak_kw
        peak_exceeded = (avg['5m'] > limit * 1.1 or avg['10m'] > limit or avg['15m'] > limit)
        should_throttle = (avg['5m'] > limit * 1.25 or avg['10m'] > limit * 1.05 or avg['15m'] > limit)

        # Notifications
        if peak_exceeded:
            def _handle_peak_notifications(avg_data):
                now = datetime.now(tz=pytz.UTC)

                if self.is_ignore_window_active:
                    return

                if self.last_email_sent_time and (now - self.last_email_sent_time).total_seconds() < 300:
                    return

                def _send_peak_email(avg_data_mail):
                    smtp_cfg = self.app_config.get('smtp', {})
                    limit = self.current_max_peak_kw

                    if avg_data_mail['15m'] > limit:
                        status_msg = "peak exceeded!"
                    elif avg_data_mail['10m'] > limit:
                        status_msg = "will exceed in 5 minutes"
                    else:
                        status_msg = "will exceed in 10 minutes"

                    html_content = [
                        f"<h3>{status_msg.capitalize()}</h3>",
                        f"Previous Month Peak: <b>{limit:.2f} kW</b><br><br>",
                        f"Current Averages:",
                        f"<ul>",
                        f"<li>5m: {avg_data_mail['5m']:.2f} kW</li>",
                        f"<li>10m: {avg_data_mail['10m']:.2f} kW</li>",
                        f"<li>15m: {avg_data_mail['15m']:.2f} kW</li>",
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
                self.bat_state_before_peak = s.get("battery_data").get("mode", c.BatteryState.BATTERY_ON)

                self.new_evcc_state = c.EVCCManualState.EVCC_CMD_STATE_OFF
                self.new_inv_state = c.InverterManualState.INV_CMD_LIMIT_STANDARD
                self.new_inv_limit = self.inverter_client.standard_power_limit
                self.new_bat_mode = c.BatteryState.BATTERY_ON

                logger.warning("Peak shaving ACTIVE: Throttling EV and Inverter.")

            return True
        elif self.is_peak_throttle_mode:
            # Exit peak throttle mode
            self.is_peak_throttle_mode = False
            self.new_inv_state = self.inv_state_before_peak
            self.new_inv_limit = self.inv_limit_before_peak
            self.new_evcc_state = self.evcc_state_before_peak
            self.new_bat_mode = self.bat_state_before_peak
            logger.info("Peak shaving ENDED: Restoring previous states.")

        return False

    def run_system_mediation_logic(self):
        """
        Runs the system mediation logic: executes user-set manual overrides.
        If none, calculates optimal state of the controllers to achieve maximum cost saving or profit.
        """
        logger.debug(f"Running system mediation logic")
        try:

            # Prepare data
            if not self._prepare_data():
                logger.error('Mediator encountered an error while preparing essential data and is skipping.')
                return

            if self._handle_peak_consumption():
                logger.warning('Mediator received peak consumption and disregards other instructions.')
            else:
                # Check operating mode
                app_mode = GLOBAL_APP_STATE.get('app_operating_mode', c.OperatingMode.MODE_MANUAL)

                if app_mode == c.OperatingMode.MODE_MANUAL:
                    # If mode manual, just set app states to be set to controllers
                    self._handle_manual_mode()

                elif app_mode == c.OperatingMode.MODE_AUTO:
                    # If mode auto: app decides controller states based on mediator goal
                    self._handle_auto_mode()

            self._apply_evcc_state()
            self._apply_inverter_state()
            self._apply_battery_state()

        except Exception as e:
            logger.error(f"Unexpected error during mediator run: {e}", exc_info=True)

# if __name__ == "__main__":
#     mock_config = {'mediator': {'standard_max_peak_consumption_kw': 2.5, 'buffer_before_pv_limit_change': 3}}
#
#     mediator = SystemMediator(mock_config, None, None)
#     mediator.run_system_mediation_logic()
