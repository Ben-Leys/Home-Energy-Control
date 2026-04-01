# hec/logic_engine/scheduled_tasks.py
import logging
import os
import signal
import sys
from time import sleep
from datetime import datetime, timedelta, timezone, time, date
from typing import Optional, List, Dict, Any

import pandas as pd
import pytz
from apscheduler.schedulers.base import BaseScheduler

from hec.controllers.api_evcc import EvccApiClient
from hec.controllers.modbus_sma_inverter import InverterSmaModbusClient
from hec.core import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.models import EVCCOverallState, EVCCLoadpointState
from hec.core.tariff_manager import TariffManager
from hec.data_sources.api_elia import fetch_and_process_forecast
from hec.data_sources.api_entsoe import fetch_entsoe_prices
from hec.data_sources.api_p1_meter_homewizard import P1MeterHomewizardClient
from hec.data_sources.api_battery_homewizard import BatteryHomeWizard
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine import battery_predictor
from hec.logic_engine.battery_predictor import BatteryPredictor
from hec.logic_engine.consumption_predictor import ConsumptionPredictor
from hec.logic_engine.data_processors import populate_appstate_with_price_data, populate_appstate_with_forecast_data, \
    update_rolling_averages
from hec.logic_engine.system_mediator import SystemMediator
from hec.reporting.daily_summary import DailySummaryGenerator
from hec.utils.utils import process_price_points_to_app_state, is_daylight

logger = logging.getLogger(__name__)

# --- Scheduled Tasks ---
FETCH_PRICES_JOB_ID = "fetch_day_ahead_prices"
FETCH_PRICES_HISTORICAL_JOB_ID = "fetch_day_ahead_historical_prices"
fetch_prices_attempt_count = 0  # First 5 retries on the house
MIDNIGHT_ROLLOVER_JOB_ID = "midnight_rollover"
P1_METER_JOB_ID = "p1_meter_update"
FETCH_ELIA_FORECAST_JOB_ID = "fetch_elia_forecast"
FETCH_ELIA_HISTORICAL_DATA_JOB_ID = "fetch_elia_historical_data"
INVERTER_FOR_DB_JOB_ID = "inverter_update_for_db"
INVERTER_FOR_MEDIATOR_JOB_ID = "inverter_update_for_mediator"
POLL_EVCC_JOB_ID = "evcc_update"
DAILY_SUMMARY_EMAIL_JOB_ID = "daily_summary_email"
MEDIATOR_LOGIC_JOB_ID = "mediator_logic"
BATTERY_UPDATE_FOR_DB_JOB_ID = "battery_update_for_db"
BATTERY_UPDATE_JOB_ID = "battery_update"
BATTERY_PREDICTOR_JOB_ID = "battery_prediction"


def task_fetch_and_store_day_ahead_prices(scheduler: BaseScheduler, db_handler: DatabaseHandler, app_config: dict,
                                          tariff_manager: TariffManager):
    """
    Scheduled task to fetch day-ahead prices, store them in database, and update AppState.
    Handles retries by rescheduling itself if data is not yet available.
    """
    global fetch_prices_attempt_count

    day_ahead_schedule = app_config.get("tasks_schedule", {}).get(FETCH_PRICES_JOB_ID, {})
    max_retries = day_ahead_schedule.get("max_retries", 36)
    retry_after = 2 if fetch_prices_attempt_count < 5 else day_ahead_schedule.get("retry_after", 15)
    daily_summary_mail = day_ahead_schedule.get('summary_email', False)

    logger.info(f"Running task: Fetch and Store Day-Ahead Prices (Attempt: {fetch_prices_attempt_count + 1})")

    # Determine target date: ENTSO-E auction is for D+1
    target_day = (datetime.now().astimezone() + timedelta(days=1))

    price_points = fetch_entsoe_prices(target_day, app_config)

    if price_points:
        if process_price_points_to_app_state(price_points, target_day, "electricity_prices_tomorrow", app_config,
                                             db_handler):
            fetch_prices_attempt_count = -5
            if daily_summary_mail:
                register_job(scheduler, DAILY_SUMMARY_EMAIL_JOB_ID, task_send_daily_energy_summary_email, "date", {},
                             [app_config, db_handler, tariff_manager], "Daily summary e-mail", 3600)
            task_run_battery_predictor(app_config, db_handler)
            return

    # --- Handle Retries if no price points ---
    fetch_prices_attempt_count += 1
    if fetch_prices_attempt_count < max_retries + 5:
        next_run_time = datetime.now(timezone.utc) + timedelta(minutes=retry_after)
        try:
            scheduler.modify_job(FETCH_PRICES_JOB_ID, next_run_time=next_run_time)
            logger.info(f"Price fetch job rescheduled to run at {next_run_time.astimezone().isoformat()} "
                        f"(in {retry_after} min).")
        except Exception as e:  # Catch JobLookupError if job was removed
            logger.warning(f"Could not reschedule price fetch job: {e}", exc_info=True)
    else:
        logger.error(f"Max retry attempts ({max_retries}) reached for fetching prices for "
                     f"{target_day.strftime('%Y-%m-%d')}. Giving up.")
        fetch_prices_attempt_count = -5  # Reset for the next day's attempt


def task_midnight_rollover(db_handler: DatabaseHandler, app_config: dict):
    logger.info("Running task: Midnight Rollover")

    if GLOBAL_APP_STATE.get("electricity_prices_tomorrow", []):
        # Shift tomorrow's prices to today
        prices_tomorrow = GLOBAL_APP_STATE.get("electricity_prices_tomorrow", [])
        GLOBAL_APP_STATE.set("electricity_prices_today", prices_tomorrow)
        GLOBAL_APP_STATE.set("electricity_prices_tomorrow", [])  # Clear tomorrow
        logger.info("Shifted 'tomorrow' prices to 'today'.")
    else:  # No prices for tomorrow, try fetch from API
        populate_appstate_with_price_data(db_handler, app_config, True)


def task_poll_p1_meter(db_handler: DatabaseHandler, p1_client: Optional[P1MeterHomewizardClient], boundary: int = 5):
    """
    Polls the P1 meter, updates AppState, and conditionally stores into the DB
    once per 'boundary'-minute slot.
    """

    if not p1_client or not p1_client.is_initialized:
        logger.warning("P1 Meter task: client unavailable. Skipping.")
        return

    logger.debug("P1 Meter polling task: Fetching data...")
    p1_data = p1_client.refresh_meter_data()
    battery_data = p1_client.refresh_batteries_data()

    if not p1_data or "timestamp_utc_iso" not in p1_data:
        logger.warning("P1 Meter task: no valid data fetched.")
        GLOBAL_APP_STATE.set("p1_meter_data", None)
        return
    if not battery_data:
        logger.info("P1 Meter task: no battery data.")
        GLOBAL_APP_STATE.set("battery_data", None)
    else:
        GLOBAL_APP_STATE.set("battery_data", battery_data)

    ts = datetime.fromisoformat(p1_data["timestamp_utc_iso"])

    # 1. Update live data in AppState
    live = {
        "timestamp_utc_iso": p1_data["timestamp_utc_iso"],
        "active_power_w": p1_data.get("active_power_w"),
        "active_power_average_w": p1_data.get("active_power_average_w"),
        "total_power_import_kwh": p1_data.get("total_power_import_kwh"),
        "total_power_export_kwh": p1_data.get("total_power_export_kwh"),
        "monthly_power_peak_w": p1_data.get("montly_power_peak_w"),  # P1_meter spelling error
        "monthly_power_peak_timestamp": p1_data.get("montly_power_peak_timestamp"),  # P1_meter spelling error
    }
    if datetime.now().day == 1 and datetime.now().hour <= 1:
        # Monthly power peak doesn't reset immediately in smart meter possibly allowing charging to create a new peak
        live["monthly_power_peak_w"] = 0
    GLOBAL_APP_STATE.set("p1_meter_data", live)
    logger.debug(f"P1 Meter live data set: {live['timestamp_utc_iso']}")
    update_rolling_averages()

    # 2. Boundary‐slot determination
    minute = ts.minute
    if minute % boundary != 0:
        logger.debug(f"P1 Meter: {minute=} not on {boundary}-min boundary; skipping DB store.")
        return

    slot = ts.replace(minute=(minute // boundary * boundary), second=0, microsecond=0)
    slot_iso = slot.isoformat()

    last_slot = GLOBAL_APP_STATE.get("p1_meter_last_stored_boundary_slot_utc_iso")
    if slot_iso == last_slot:
        logger.debug(f"P1 Meter: boundary slot {slot_iso} already stored; skipping.")
        return

    # 3. Store to DB & update AppState
    if db_handler.store_p1_meter_data(p1_data):
        GLOBAL_APP_STATE.set("p1_meter_last_stored_boundary_slot_utc_iso", slot_iso)
        logger.info(f"P1 Meter: data stored for slot {slot_iso}.")
    else:
        logger.error(f"P1 Meter: failed to store data for slot {slot_iso}.")


def task_poll_inverter(db_handler: DatabaseHandler, inv_client: InverterSmaModbusClient, app_config: dict,
                       log_to_db: bool) -> Optional[Dict[str, Any]]:
    """Polling inverter, updating AppState, and optionally logging to DB."""
    if not inv_client:
        logger.debug("Inverter poll: Client not initialized.")
        GLOBAL_APP_STATE.set("inverter_data", None)
        return None

    status = inv_client.get_operational_status()
    if status == c.InverterStatus.UNKNOWN or status == c.InverterStatus.OFFLINE:
        logger.debug("Inverter poll: Client not available.")
        GLOBAL_APP_STATE.set("inverter_data", None)
        return None

    if not is_daylight(app_config):
        logger.debug("Inverter poll: Not daylight. Setting PV to 0 and status to STANDBY.")
        current_inverter_data = GLOBAL_APP_STATE.get("inverter_data")

        current_inverter_data.update({
            "pv_power_watts": 0,
            "operational_status": c.InverterStatus.STANDBY,
            "timestamp_utc_iso": datetime.now(timezone.utc).isoformat()
        })
        GLOBAL_APP_STATE.set("inverter_data", current_inverter_data)
        return current_inverter_data

    logger.debug(f"Inverter poll: Fetching data (log_to_db={log_to_db})...")
    live_data = inv_client.get_live_data()

    if live_data:
        GLOBAL_APP_STATE.set("inverter_data", live_data)

        if log_to_db:
            logger.info(f"Inverter DB log: Storing data for {live_data.get('timestamp_utc_iso')}")
            db_handler.store_inverter_data(live_data)

        return live_data
    else:
        logger.warning("Inverter poll: Failed to fetch data from inverter.")
        GLOBAL_APP_STATE.set("inverter_data", None)
        return None


def task_poll_inverter_for_db_logging(db_handler: DatabaseHandler, inv_client: InverterSmaModbusClient, app_config):
    """Straightforward: poll for db storage and update AppState too"""
    logger.debug("Running task: Poll inverter for DB logging")
    task_poll_inverter(db_handler, inv_client, app_config, log_to_db=True)


def task_poll_inverter_for_live_update(db_handler: DatabaseHandler, inv_client: InverterSmaModbusClient, app_config):
    """Make an on-request update. Usually API request from dashboard."""
    logger.debug("Running task: Poll inverter for Live Update (Example: dashboard)")
    task_poll_inverter(db_handler, inv_client, app_config, log_to_db=False)


def task_poll_inverter_for_mediator_update(db_handler: DatabaseHandler, inv_client: InverterSmaModbusClient,
                                           app_config):
    """Poll to update average values for controller calculations."""
    # Avoid running together with standard db_logging task every 15 minutes ?
    logger.debug("Running task: Poll inverter for Mediator Update")
    task_poll_inverter(db_handler, inv_client, app_config, log_to_db=False)


def task_poll_evcc_state(evcc_client: Optional[EvccApiClient]):
    """Poll evcc for state dict and store in AppState"""
    if not evcc_client:
        logger.debug("EVCC polling task: Client not available. Skipping.")
        GLOBAL_APP_STATE.set("evcc_overall_state", None)
        return

    logger.debug("EVCC polling task: Fetching state...")
    evcc_state = evcc_client.get_current_state_data()

    if evcc_state:
        cur_state = EVCCOverallState(timestamp_utc_iso=evcc_state.get("timestamp_utc_iso"),
                                     residual_power=evcc_state.get('residualPower'))
        lp_data = evcc_state.get('loadpoints')[0]
        currents = lp_data.get("chargeCurrents", [])
        charge_current = sum(currents) if currents else None
        cur_lp = EVCCLoadpointState(loadpoint_id=1,
                                    is_connected=lp_data.get('connected'),
                                    is_charging=lp_data.get('charging'),
                                    charge_current=charge_current,
                                    min_current=lp_data.get('minCurrent'),
                                    max_current=lp_data.get('maxCurrent'),
                                    enable_threshold=lp_data.get('enableThreshold'),
                                    disable_threshold=lp_data.get('disableThreshold'),
                                    limit_energy=lp_data.get('limitEnergy'),
                                    mode=lp_data.get('mode'),
                                    session_energy=lp_data.get('sessionEnergy'),
                                    smart_cost_active=lp_data.get('smartCostActive'),
                                    plan_active=lp_data.get('planActive'))
        GLOBAL_APP_STATE.set("evcc_overall_state", cur_state.to_dict())
        GLOBAL_APP_STATE.set("evcc_loadpoint_state", cur_lp.to_dict())
        logger.debug(f"EVCC: AppState updated. Mode: {cur_lp.mode}, Charging: {cur_lp.is_charging}")

    else:
        logger.warning("EVCC polling task: Failed to fetch state from EVCC.")
        GLOBAL_APP_STATE.set("evcc_overall_state", None)


def task_poll_battery_for_db_logging(db_handler: DatabaseHandler,
                                     battery_clients: Optional[Dict[str, BatteryHomeWizard]], store_to_db=False):
    all_battery_records = []

    # --- Get overall battery mode from P1 meter ---
    battery_mode = (GLOBAL_APP_STATE.get("battery_data") or {}).get("mode", "UNKNOWN")

    # --- Poll each configured battery ---
    if not battery_clients:
        logger.info("No battery clients available to poll.")
        GLOBAL_APP_STATE.set("battery_records", all_battery_records)
        return

    for name, client in battery_clients.items():
        data = client.refresh_data()
        if not data:
            logger.warning(f"Skipping battery '{name}': no data received.")
            continue

        # Merge the mode from P1 meter with the battery-specific data
        battery_record = {
            "battery_name": data.get("battery_name", name),
            "timestamp_utc_iso": data.get("timestamp_utc_iso"),
            "energy_import_kwh": data.get("energy_import_kwh"),
            "energy_export_kwh": data.get("energy_export_kwh"),
            "state_of_charge_pct": data.get("state_of_charge_pct"),
            "battery_mode": battery_mode,
            "cycles": data.get("cycles")
        }

        # Store in database
        if store_to_db:
            success = db_handler.store_battery_data(battery_record)
            if success:
                logger.info(f"Battery data for '{name}' stored successfully.")
            else:
                logger.error(f"Failed to store battery data for '{name}'.")

        all_battery_records.append(battery_record)
    GLOBAL_APP_STATE.set("battery_records", all_battery_records)


def task_fetch_elia_forecasts(db_handler: DatabaseHandler, app_config: dict):
    """
    Scheduled task to fetch various forecasts from Elia Open Data,
    and store them in the database. Fetches for the next 5 days (D+1 to D+5).
    Grid load forecast only available until D+4.
    """
    logger.info("Running task: Fetch Elia Forecasts.")

    # Define the range of days to fetch (e.g., today + 1 to today + 5)
    # Forecasts are for D+1, D+2, ..., D+5
    days_to_fetch = 5

    all_fetched_records: List[Dict[str, Any]] = []

    for i in range(1, days_to_fetch + 1):  # From D+1 to D+5
        target_day_utc = (datetime.now(timezone.utc) + timedelta(days=i)).replace(hour=0, minute=0, second=0,
                                                                                  microsecond=0)
        target_day_utc_str = target_day_utc.strftime("%Y-%m-%d")
        logger.info(f"Fetching Elia forecasts for day: {target_day_utc_str}")

        for forecast_type in ["solar", "wind", "grid_load"]:
            if i > 4 and forecast_type == "grid_load":
                logger.debug(f"Skipping grid load forecast for {target_day_utc_str} beyond Elia's 4-day limit.")
                continue  # Grid Load Forecast (Elia API provides up to 4 days)
            logger.debug(f"Fetching {forecast_type.capitalize()} forecast for {target_day_utc_str}.")
            data = fetch_and_process_forecast(target_day_utc, app_config, forecast_type)
            if data:
                all_fetched_records.extend(data)
            else:
                logger.warning(f"Failed to fetch {forecast_type} forecast for {target_day_utc_str}.")
            sleep(1)  # Too many requests leads to empty response

    if all_fetched_records:
        logger.info(f"Finished fetching Elia forecasts. Total days: {days_to_fetch} rec: {len(all_fetched_records)}.")
        db_handler.store_elia_forecasts(all_fetched_records)
    else:
        logger.warning(f"No Elia forecast data fetched for {days_to_fetch} days. Check API.")


def task_send_daily_energy_summary_email(app_config, db_handler, tariff_manager, renew_prices=False):
    """
    Scheduled task to generate and send the daily energy summary email.
    """
    forecasts = populate_appstate_with_forecast_data(db_handler)

    summary_generator = DailySummaryGenerator(app_config, db_handler, tariff_manager, forecasts)
    logger.info("Running task: Send Daily Energy Summary Email.")

    t_date_prices = GLOBAL_APP_STATE.get("electricity_prices_tomorrow")
    if not t_date_prices or renew_prices:
        populate_appstate_with_price_data(db_handler, app_config, True)

    try:
        success = summary_generator.generate_and_send_summary(app_config)
        if success:
            logger.info("Daily energy summary email generated and sent successfully.")
        else:
            logger.error("Failed to generate or send daily energy summary email.")
    except Exception as e:
        logger.error(f"Error in task_send_daily_energy_summary_email: {e}", exc_info=True)


def fetch_historic_da_data(db_handler: DatabaseHandler, app_config: dict, hist_start_date):
    """Fetch historic data from ENTSO-E for predictions"""
    days = (datetime.now().date() - hist_start_date.date()).days
    total_lines_added = 0
    for day_offset in range(days):
        day = hist_start_date + timedelta(days=day_offset)
        price_points = fetch_entsoe_prices(day, app_config)

        if price_points:
            lines_added = db_handler.store_da_prices(price_points)
            total_lines_added += lines_added
        else:
            logger.info(f"No historic data available for {day} day-ahead prices.")

    logger.info(f"Fetched and stored {total_lines_added} day-ahead price points.")


def fetch_historic_elia_data(db_handler: DatabaseHandler, app_config: dict, hist_start_date):
    """Fetch historic data from Elia for predictions"""
    days = (datetime.now().date() - hist_start_date.date()).days
    total_lines_added = 0
    for day_offset in range(days):
        test_target_day = hist_start_date + timedelta(days=day_offset)

        for f_type in ["solar", "wind", "grid_load"]:
            result = fetch_and_process_forecast(test_target_day, app_config, f_type)
            if result:
                lines_added = db_handler.store_elia_forecasts(result)
                total_lines_added += lines_added
            else:
                logger.info(f"No historic data available for Elia forecasts for {test_target_day}.")

    logger.info(f"Fetched and stored {total_lines_added} Elia forecast records.")


def task_run_battery_predictor(app_config, db_handler: DatabaseHandler):
    """
    Scheduled task to run the battery predictor.
    Generates the base 48h plan once per day, and optimizes it every 15 minutes.
    """
    logger.info("Running battery predictor task...")
    bp = BatteryPredictor(app_config)
    cd = ConsumptionPredictor(db_handler)

    local_tz = pytz.timezone('Europe/Brussels')
    now_utc = datetime.now(pytz.UTC)
    now_local = now_utc.astimezone(local_tz)

    # 1. Get Actual SOC from App State (fallback to 0 if not found)
    battery_records = GLOBAL_APP_STATE.get("battery_records") or []
    actual_soc = -0.001
    if battery_records and len(battery_records) > 0:
        for battery in battery_records:
            actual_soc += battery.get("state_of_charge_pct", 0.0)
        actual_soc /= len(battery_records)
    if actual_soc == -0.001:
        logger.warning("Predictor: No battery SOC found in state, skipping predictor.")
        return

    # 2. Get Max Peak (Convert W to kW)
    p1_data = GLOBAL_APP_STATE.get("p1_meter_data") or {}
    max_peak_w = p1_data.get("monthly_power_peak_w", 2500)
    max_peak_kw = max_peak_w / 1000.0

    # 3. Check if we need to generate the base plan
    today_date_str = now_local.strftime('%Y-%m-%d')
    last_gen_date = GLOBAL_APP_STATE.get("plan_generation_date")
    base_plan_df = GLOBAL_APP_STATE.get("prediction_plan_df", None)

    if last_gen_date != today_date_str:
        logger.info(f"Generating new 48h base prediction plan for {today_date_str}...")

        # Determine strict local midnight boundaries (DST safe!)
        today_local_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_date = today_local_midnight.date() + timedelta(days=1)
        tomorrow_local_midnight = local_tz.localize(datetime.combine(tomorrow_date, datetime.min.time()))
        day_after_date = today_local_midnight.date() + timedelta(days=2)
        day_after_local_midnight = local_tz.localize(datetime.combine(day_after_date, datetime.min.time()))

        # Convert back to UTC for the predictor limits
        first_day_start = today_local_midnight.astimezone(pytz.UTC)
        first_day_end = (tomorrow_local_midnight - timedelta(minutes=15)).astimezone(pytz.UTC)

        second_day_start = tomorrow_local_midnight.astimezone(pytz.UTC)
        second_day_end = (day_after_local_midnight - timedelta(minutes=15)).astimezone(pytz.UTC)

        try:
            # --- DAY 1 ---
            ff1 = cd.generate_consumption_forecast(first_day_start, first_day_end)
            first_plan_df = bp.generate_plan(
                first_day_start, first_day_end, ff1, db_handler,
                max_peak_kw, initial_soc_pct=actual_soc
            )

            # --- DAY 2 ---
            # Carry over the last SOC from Day 1 to the start of Day 2
            if not first_plan_df.empty:
                last_soc_day1 = first_plan_df['soc_pct'].iloc[-1]
            else:
                last_soc_day1 = actual_soc

            ff2 = cd.generate_consumption_forecast(second_day_start, second_day_end)
            second_plan_df = bp.generate_plan(
                second_day_start, second_day_end, ff2, db_handler,
                max_peak_kw, initial_soc_pct=last_soc_day1
            )

            # --- Combine and Store ---
            base_plan_df = pd.concat([first_plan_df, second_plan_df])
            GLOBAL_APP_STATE.set("plan_generation_date", today_date_str)
            logger.info("Base prediction plan generated successfully.")

        except Exception as e:
            logger.error(f"Failed to generate base prediction plan: {e}", exc_info=True)
            return  # Stop execution if base plan fails

    # 4. Run the 15-minute Optimization
    try:
        if base_plan_df is not None:
            logger.debug("Optimizing prediction plan for current state...")
            opt_plan_df = bp.optimize_plan(base_plan_df, now_utc, actual_soc, GLOBAL_APP_STATE)

            # Store the finalized plan in app_state so the Vue dashboard can pick it up
            plan_list = opt_plan_df.reset_index().to_dict(orient='records')
            GLOBAL_APP_STATE.set("prediction_plan", plan_list)
            GLOBAL_APP_STATE.set("prediction_plan_df", opt_plan_df)
            logger.debug("Prediction plan optimized and saved to state.")

    except Exception as e:
        logger.error(f"Failed to optimize prediction plan: {e}", exc_info=True)


def task_system_mediator(system_mediator: SystemMediator, app_config,
                         db_handler: DatabaseHandler, tariff_manager: TariffManager):
    # System reboot asked
    if GLOBAL_APP_STATE.get('reboot_request', False):
        logger.warning('Reboot requested')
        pid = os.getpid()
        os.kill(pid, signal.SIGINT)
    # Daily summary e-mail requested
    if GLOBAL_APP_STATE.get('summary_request', False):
        logger.info('Summary e-mail requested')
        GLOBAL_APP_STATE.set('summary_request', False)
        task_send_daily_energy_summary_email(app_config, db_handler, tariff_manager, True)
    if GLOBAL_APP_STATE.get('prediction_plan', None) is None:
        task_run_battery_predictor(app_config, db_handler)

    system_mediator.run_system_mediation_logic()


def register_all_jobs(scheduler: BaseScheduler, db_handler: DatabaseHandler, app_config: dict,
                      p1_client: Optional[P1MeterHomewizardClient],
                      inv_client: Optional[InverterSmaModbusClient],
                      evcc_client: Optional[EvccApiClient],
                      tariff_manager: Optional[TariffManager],
                      system_mediator: SystemMediator,
                      battery_clients: Optional[Dict[str, BatteryHomeWizard]],
                      fetch_entsoe=False, fetch_elia=False):
    """Registers all defined scheduled jobs with the provided scheduler instance."""

    logger.info("Registering scheduled jobs...")
    try:
        job_definitions = [
            {
                "job_id": FETCH_PRICES_JOB_ID,
                "task_function": task_fetch_and_store_day_ahead_prices,
                "trigger": "cron",
                "trigger_args": "",
                "job_args": [scheduler, db_handler, app_config, tariff_manager],
                "name": "Fetch Day-Ahead ENTSO-E Prices",
                "misfire_grace_time": 32400,  # 9 hours
            },
            {
                "job_id": MIDNIGHT_ROLLOVER_JOB_ID,
                "task_function": task_midnight_rollover,
                "trigger": "cron",
                "trigger_args": "",
                "job_args": [db_handler, app_config],
                "name": "Midnight Rollover",
                "misfire_grace_time": 50400,  # 13 hours
            },
            {
                "job_id": FETCH_ELIA_FORECAST_JOB_ID,
                "task_function": task_fetch_elia_forecasts,
                "trigger": "cron",
                "trigger_args": "",
                "job_args": [db_handler, app_config],
                "name": "Fetch Elia Forecasts",
                "misfire_grace_time": 3600,  # 1 hour
            },
            {
                "job_id": MEDIATOR_LOGIC_JOB_ID,
                "task_function": task_system_mediator,
                "trigger": "cron",
                "trigger_args": "",
                "job_args": [system_mediator, app_config, db_handler, tariff_manager],
                "name": "System Mediator",
                "misfire_grace_time": 10,
            },
            {
                "job_id": BATTERY_PREDICTOR_JOB_ID,
                "task_function": task_run_battery_predictor,
                "trigger": "cron",
                "trigger_args": "",
                "job_args": [app_config, db_handler],
                "name": "Battery predictor",
                "misfire_grace_time": 600,
            }
        ]

        # Register CRON jobs
        tasks_config = app_config.get("tasks_schedule", {})
        for job in job_definitions:
            task_config = tasks_config.get(job["job_id"], {})
            trigger_args = task_config.get('trigger_args', job.get('trigger_args', ""))

            if trigger_args:
                register_job(
                    scheduler,
                    job_id=job["job_id"],
                    func=job["task_function"],
                    trigger=job["trigger"],
                    trigger_args=trigger_args,
                    job_args=job["job_args"],
                    name=job["name"],
                    grace_time=job["misfire_grace_time"],
                )
            else:
                logger.warning(f"No trigger for job '{job['job_id']}'. Skipping.")

        # Register P1 Meter job if available
        if p1_client:
            p1_meter_schedule = tasks_config.get(P1_METER_JOB_ID, {})
            second = p1_meter_schedule.get('second', '*/15')
            register_job(
                scheduler,
                job_id=P1_METER_JOB_ID,
                func=task_poll_p1_meter,
                trigger="cron",
                trigger_args={"second": second},
                job_args=[db_handler, p1_client],
                name="Poll P1 Smart Meter",
                grace_time=10,
            )
        else:
            logger.warning(f"P1 Meter client not initialized. '{P1_METER_JOB_ID}' job not scheduled.")

        # Register inverter job if available
        if inv_client:
            inverter_schedule = tasks_config.get(INVERTER_FOR_DB_JOB_ID, {})
            minute = inverter_schedule.get('minute', '*/15')
            register_job(
                scheduler,
                job_id=INVERTER_FOR_DB_JOB_ID,
                func=task_poll_inverter_for_db_logging,
                trigger="cron",
                trigger_args={"minute": minute},
                job_args=[db_handler, inv_client, app_config],
                name="Poll inverter for DB Logging",
                grace_time=600,
            )
            inverter_schedule = tasks_config.get(INVERTER_FOR_MEDIATOR_JOB_ID, {})
            second = inverter_schedule.get('second', '*/15')
            register_job(
                scheduler,
                job_id=INVERTER_FOR_MEDIATOR_JOB_ID,
                func=task_poll_inverter_for_mediator_update,
                trigger="cron",
                trigger_args={"second": second},
                job_args=[db_handler, inv_client, app_config],
                name="Poll inverter for mediator (if required)",
                grace_time=10,
            )
        else:
            logger.warning(f"Inverter client not initialized. '{INVERTER_FOR_MEDIATOR_JOB_ID}' job not scheduled.")

        # Register evcc job if available
        if evcc_client:
            evcc_schedule = tasks_config.get(POLL_EVCC_JOB_ID, {})
            second = evcc_schedule.get('second', '*/15')
            register_job(
                scheduler,
                job_id=POLL_EVCC_JOB_ID,
                func=task_poll_evcc_state,
                trigger="cron",
                trigger_args={"second": second},
                job_args=[evcc_client],
                name="Poll EVCC",
                grace_time=10,
            )
        else:
            logger.warning(f"P1 Meter client not initialized. '{POLL_EVCC_JOB_ID}' job not scheduled.")

        if fetch_entsoe:
            hist_start_date = datetime.combine(date.fromisoformat(app_config.get('historic_data').get('start_date')),
                                               time.min)
            register_job(
                scheduler,
                job_id=FETCH_PRICES_HISTORICAL_JOB_ID,
                func=fetch_historic_da_data,
                trigger="date",
                trigger_args={"run_date": datetime.now() + timedelta(seconds=20)},
                job_args=[db_handler, app_config, hist_start_date],
                name="Fetch historical entsoe data",
                grace_time=10,
            )

        if fetch_elia:
            hist_start_date = datetime.combine(date.fromisoformat(app_config.get('historic_data').get('start_date')),
                                               time.min)
            register_job(
                scheduler,
                job_id=FETCH_ELIA_HISTORICAL_DATA_JOB_ID,
                func=fetch_historic_elia_data,
                trigger="date",
                trigger_args={"run_date": datetime.now() + timedelta(seconds=140)},
                job_args=[db_handler, app_config, hist_start_date],
                name="Fetch historical elia data",
                grace_time=10,
            )

        # Register battery job if available
        if battery_clients and p1_client:
            inverter_schedule = tasks_config.get(BATTERY_UPDATE_FOR_DB_JOB_ID, {})
            minute = inverter_schedule.get('minute', '*/15')
            register_job(
                scheduler,
                job_id=BATTERY_UPDATE_FOR_DB_JOB_ID,
                func=task_poll_battery_for_db_logging,
                trigger="cron",
                trigger_args={"minute": minute},
                job_args=[db_handler, battery_clients, True],
                name="Poll battery for DB logging",
                grace_time=600,
            )
            inverter_schedule = tasks_config.get(BATTERY_UPDATE_JOB_ID, {})
            second = inverter_schedule.get('second', '*/15')
            register_job(
                scheduler,
                job_id=BATTERY_UPDATE_JOB_ID,
                func=task_poll_battery_for_db_logging,
                trigger="cron",
                trigger_args={"second": second},
                job_args=[db_handler, battery_clients, False],
                name="Poll battery for display",
                grace_time=10,
            )

    except Exception as e:
        logger.critical(f"Failed to register scheduled jobs: {e}", exc_info=True)
        db_handler.close_connection()  # Clean up
        return


def register_job(scheduler, job_id, func, trigger, trigger_args, job_args, name, grace_time, replace_existing=True):
    """Helper to register a job with the scheduler."""
    try:
        scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            args=job_args,
            name=name,
            misfire_grace_time=grace_time,
            replace_existing=replace_existing,
            **trigger_args
        )
        logger.info(f"Job '{job_id}' scheduled: {trigger.upper()} with args {trigger_args}.")
    except Exception as e:
        logger.warning(f"Failed to schedule job '{job_id}': {e}", exc_info=True)
