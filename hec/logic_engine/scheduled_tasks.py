# hec/logic_engine/scheduled_tasks.py
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from apscheduler.schedulers.base import BaseScheduler

from hec.core import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.data_sources import api_entsoe
from hec.data_sources.api_elia import fetch_and_process_forecast
from hec.data_sources.api_p1_meter_homewizard import P1MeterHomewizardClient
from hec.data_sources.modbus_sma_inverter import InverterSmaModbusClient
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.data_processors import populate_appstate_with_price_data, populate_appstate_with_forecast_data
from hec.logic_engine.utils import parse_hh_mm_time_string, process_price_points_to_app_state, is_daylight

logger = logging.getLogger(__name__)

# --- Scheduled Tasks ---
FETCH_PRICES_JOB_ID = "fetch_day_ahead_prices"
fetch_prices_attempt_count = 0
MIDNIGHT_ROLLOVER_JOB_ID = "midnight_rollover"
P1_METER_JOB_ID = "p1_meter_update"
FETCH_ELIA_FORECAST_JOB_ID = "fetch_elia_forecast"
INVERTER_JOB_ID = "inverter_update"


def task_fetch_and_store_day_ahead_prices(scheduler: BaseScheduler, db_handler: DatabaseHandler, app_config: dict):
    """
    Scheduled task to fetch day-ahead prices, store them in database, and update AppState.
    Handles retries by rescheduling itself if data is not yet available.
    """
    global fetch_prices_attempt_count

    day_ahead_schedule = app_config.get("scheduler", {}).get(FETCH_PRICES_JOB_ID, {})
    max_retries = day_ahead_schedule.get("max_retries", 36)
    retry_after = day_ahead_schedule.get("retry_after", 0)
    retry_interval_minutes = app_config.get('scheduler', {}).get('price_fetch_retry_interval_min', 15)

    logger.info(f"Running task: Fetch and Store Day-Ahead Prices (Attempt: {fetch_prices_attempt_count + 1})")

    # Determine target date: ENTSO-E auction is for D+1
    target_day = (datetime.now().astimezone() + timedelta(days=1))

    price_points = api_entsoe.fetch_entsoe_prices(target_day, app_config)

    if process_price_points_to_app_state(price_points, target_day, "electricity_prices_tomorrow", db_handler):
        fetch_prices_attempt_count = 0
        return

    # --- Handle Retries if no price points ---
    fetch_prices_attempt_count += 1
    if fetch_prices_attempt_count < max_retries:
        next_run_time = datetime.now(timezone.utc) + timedelta(minutes=retry_after)
        try:
            scheduler.modify_job(FETCH_PRICES_JOB_ID, next_run_time=next_run_time)
            logger.info(f"Price fetch job rescheduled to run at {next_run_time.astimezone().isoformat()} "
                        f"(in {retry_interval_minutes} min).")
        except Exception as e:  # Catch JobLookupError if job was removed
            logger.warning(f"Could not reschedule price fetch job: {e}", exc_info=True)
    else:
        logger.warning(f"Max retry attempts ({max_retries}) reached for fetching prices for "
                       f"{target_day.strftime('%Y-%m-%d')}. Giving up.")
        fetch_prices_attempt_count = 0  # Reset for the next day's attempt


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

    populate_appstate_with_forecast_data(db_handler)


def task_poll_p1_meter(db_handler: DatabaseHandler, p1_client: Optional[P1MeterHomewizardClient], boundary: int = 5):
    """
    Polls the P1 meter, updates AppState, and conditionally stores into the DB
    once per 'boundary'-minute slot.
    """

    if not p1_client or not p1_client.is_initialized:
        logger.warning("P1 Meter task: client unavailable. Skipping.")
        return

    logger.debug("P1 Meter polling task: Fetching data...")
    p1_data = p1_client.refresh_data()

    if not p1_data or "timestamp_utc_iso" not in p1_data:
        logger.warning("P1 Meter task: no valid data fetched.")
        GLOBAL_APP_STATE.set("p1_meter_data", None)
        return

    ts = datetime.fromisoformat(p1_data["timestamp_utc_iso"])
    # 1. Update live data in AppState
    live = {
        "timestamp_utc_iso": p1_data["timestamp_utc_iso"],
        "active_power_w": p1_data.get("active_power_w"),
        "active_power_average_w": p1_data.get("active_power_average_w"),
        "total_power_import_kwh": p1_data.get("total_power_import_kwh"),
        "total_power_export_kwh": p1_data.get("total_power_export_kwh"),
        "monthly_power_peak_w": p1_data.get("monthly_power_peak_w"),
        "monthly_power_peak_timestamp": p1_data.get("monthly_power_peak_timestamp"),
    }
    GLOBAL_APP_STATE.set("p1_meter_data", live)
    logger.debug(f"P1 Meter live data set: {live['timestamp_utc_iso']}")

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
    """Common logic for polling inverter, updating AppState, and optionally logging to DB."""
    if not inv_client:
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
            # TODO: Implement db_handler.store_inverter_data(live_data)
            logger.info(f"Inverter DB log: Storing data for {live_data.get('timestamp_utc_iso')}")
            # db_handler.store_inverter_data(live_data) # Call your new DB store function

        return live_data
    else:
        logger.warning("Inverter poll: Failed to fetch data from inverter.")
        GLOBAL_APP_STATE.set("inverter_data", None)
        return None


def task_poll_inverter_for_db_logging(db_handler: DatabaseHandler, inv_client: InverterSmaModbusClient, app_config):
    logger.debug("Running task: Poll inverter for DB logging")
    task_poll_inverter(db_handler, inv_client, app_config, log_to_db=True)


def task_poll_inverter_for_live_update(db_handler: DatabaseHandler, inv_client: InverterSmaModbusClient, app_config):
    logger.debug("Running task: Poll inverter for AppState Update")
    task_poll_inverter(db_handler, inv_client, app_config, log_to_db=False)


def task_fetch_elia_forecasts(db_handler: DatabaseHandler, app_config: dict):
    """
    Scheduled task to fetch various forecasts from Elia Open Data,
    and store them in the database. Fetches for the next 5 days (D+1 to D+5).
    Grid load forecast only available until D+4.
    """
    logger.info("Running task: Fetch Elia Renewables Forecasts.")

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
                logger.error(f"Failed to fetch {forecast_type} forecast for {target_day_utc_str}.")

    if all_fetched_records:
        logger.info(f"Finished fetching Elia forecasts. Total days: {days_to_fetch} rec: {len(all_fetched_records)}.")
        db_handler.store_elia_forecasts(all_fetched_records)
    else:
        logger.warning(f"No Elia forecast data fetched for {days_to_fetch} days. Check API.")


def register_all_jobs(scheduler: BaseScheduler, db_handler: DatabaseHandler,
                      app_config: dict, p1_client: Optional[P1MeterHomewizardClient], inv_client: InverterSmaModbusClient):
    """Registers all defined scheduled jobs with the provided scheduler instance."""

    logger.info("Registering scheduled jobs...")
    try:
        tasks_config = app_config.get("tasks_schedule", {})

        job_definitions = [
            {
                "job_id": FETCH_PRICES_JOB_ID,
                "task_function": task_fetch_and_store_day_ahead_prices,
                "trigger": "cron",
                "trigger_args": lambda cfg: {"hour": cfg[0], "minute": cfg[1]},
                "job_args": [scheduler, db_handler, app_config],
                "name": "Fetch Day-Ahead ENTSO-E Prices",
                "misfire_grace_time": 32400,  # 9 hours
            },
            {
                "job_id": MIDNIGHT_ROLLOVER_JOB_ID,
                "task_function": task_midnight_rollover,
                "trigger": "cron",
                "trigger_args": lambda cfg: {"hour": cfg[0], "minute": cfg[1]},
                "job_args": [db_handler, app_config],
                "name": "Midnight Rollover",
                "misfire_grace_time": 50400,  # 13 hours
            },
            {
                "job_id": FETCH_ELIA_FORECAST_JOB_ID,
                "task_function": task_fetch_elia_forecasts,
                "trigger": "cron",
                "trigger_args": lambda cfg: {"hour": cfg[0], "minute": cfg[1]},
                "job_args": [db_handler, app_config],
                "name": "Fetch Elia Forecasts",
                "misfire_grace_time": 3600,  # 1 hour
            },
        ]

        # Register CRON jobs
        for job in job_definitions:
            task_config = tasks_config.get(job["job_id"], {})
            scheduled_time = task_config.get('time')
            parsed_time = parse_hh_mm_time_string(scheduled_time)

            if parsed_time:
                trigger_args = job["trigger_args"](parsed_time)
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
                logger.warning(f"Could not parse time for job '{job['job_id']}'. Skipping.")

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
            inverter_schedule = tasks_config.get(INVERTER_JOB_ID, {})
            minute = inverter_schedule.get('minute', '*/15')
            register_job(
                scheduler,
                job_id=INVERTER_JOB_ID,
                func=task_poll_inverter_for_db_logging,
                trigger="cron",
                trigger_args={"minute": minute},
                job_args=[db_handler, inv_client, app_config],
                name="Poll inverter for DB Logging",
                grace_time=600,
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
