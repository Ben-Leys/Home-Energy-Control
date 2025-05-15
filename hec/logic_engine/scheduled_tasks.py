# hec/logic_engine/scheduled_tasks.py
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from apscheduler.schedulers.base import BaseScheduler

from hec.core import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.data_sources import day_ahead_price_api, elia_forecast_api
from hec.data_sources.p1_meter_homewizard import P1MeterHomeWizard
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.utils import convert_utc_price_points_to_local, parse_hh_mm_time_string, process_price_points

logger = logging.getLogger(__name__)


# --- Scheduled Tasks ---
FETCH_PRICES_JOB_ID = "fetch_day_ahead_prices"
fetch_prices_attempt_count = 0
MIDNIGHT_ROLLOVER_JOB_ID = "midnight_rollover"
P1_METER_JOB_ID = "p1_meter_update"
FETCH_ELIA_FORECAST_JOB_ID = "fetch_elia_forecast"


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

    price_points = day_ahead_price_api.fetch_entsoe_prices(target_day, app_config)

    if process_price_points(price_points, db_handler, target_day, "electricity_prices_tomorrow"):
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
            logger.error(f"Could not reschedule price fetch job: {e}", exc_info=True)
    else:
        logger.error(f"Max retry attempts ({max_retries}) reached for fetching prices for "
                     f"{target_day.strftime('%Y-%m-%d')}. Giving up.")
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.ALARM)
        fetch_prices_attempt_count = 0  # Reset for the next day's attempt


def task_midnight_rollover(db_handler: DatabaseHandler, app_config: dict):
    logger.info("Running task: Midnight Rollover")

    if GLOBAL_APP_STATE.get("electricity_prices_tomorrow", []):
        # Shift tomorrow's prices to today
        prices_tomorrow = GLOBAL_APP_STATE.get("electricity_prices_tomorrow", [])
        GLOBAL_APP_STATE.set("electricity_prices_today", prices_tomorrow)
        GLOBAL_APP_STATE.set("electricity_prices_tomorrow", [])  # Clear tomorrow
        logger.info("Shifted 'tomorrow' prices to 'today'. 'Tomorrow' prices are now empty in AppState.")
    else:  # No prices for tomorrow, try fetch from API
        local_now = datetime.now().astimezone()
        populate_price_data_in_appstate(db_handler, local_now, app_config, "electricity_prices_today",
                                        force_api_fetch_if_missing=True)

    # TODO: Similar logic for forecasts etc.


def task_poll_p1_meter(db_handler: DatabaseHandler, p1_client: Optional[P1MeterHomeWizard]):
    """Scheduled task to poll the P1 meter, update AppState, and log to DB."""

    if not p1_client or not p1_client.is_initialized:
        logger.warning("P1 Meter polling task: Client not available or not initialized. Skipping.")
        return

    logger.debug("P1 Meter polling task: Fetching data...")
    p1_data = p1_client.refresh_data()

    if p1_data:
        # 1. Update AppState
        live_p1_for_appstate = {
            "timestamp_utc_iso": p1_data.get("timestamp_utc_iso"),
            "active_power_w": p1_data.get("active_power_w"),
            "active_power_average_w": p1_data.get("active_power_average_w"),
            "total_power_import_kwh": p1_data.get("total_power_import_kwh"),
            "total_power_export_kwh": p1_data.get("total_power_export_kwh"),
            "monthly_power_peak_w": p1_data.get("montly_power_peak_w"),
            "monthly_power_peak_timestamp": p1_data.get("montly_power_peak_timestamp"),
        }
        GLOBAL_APP_STATE.set("p1_meter_data", live_p1_for_appstate)
        logger.debug(f"AppState updated with P1 meter live data: {live_p1_for_appstate.get("timestamp_utc_iso)")}")

        # 2. Store full data to Database
        db_handler.store_p1_meter_data(p1_data, GLOBAL_APP_STATE)
    else:
        logger.warning("P1 Meter polling task: Failed to fetch data from P1 meter.")
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)
        GLOBAL_APP_STATE.set("p1_meter_data", None)  # Clear stale data


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
        logger.info(f"Fetching Elia forecasts for day: {target_day_utc.strftime('%Y-%m-%d')}")

        # Solar Forecast
        solar_data = elia_forecast_api.fetch_solar_production_forecast(target_day_utc, app_config)
        if solar_data is not None:  # None means critical error, [] means no data for day
            all_fetched_records.extend(solar_data)
        else:
            logger.error(f"Failed to fetch solar forecast for {target_day_utc.date()}. Critical error.")

        # Wind Forecast
        wind_data = elia_forecast_api.fetch_wind_production_forecast(target_day_utc, app_config)
        if wind_data is not None:
            all_fetched_records.extend(wind_data)
        else:
            logger.error(f"Failed to fetch wind forecast for {target_day_utc.date()}. Critical error.")

        # Grid Load Forecast (Elia API provides up to 4 days)
        if i <= 4:  # Check if within 4-day limit for load forecast
            load_data = elia_forecast_api.fetch_grid_load_forecast(target_day_utc, app_config)
            if load_data is not None:
                all_fetched_records.extend(load_data)
            else:
                logger.error(f"Failed to fetch grid load forecast for {target_day_utc.date()}. Critical error.")
        else:
            logger.debug(
                f"Skipping grid load forecast for {target_day_utc.date()} as it's beyond Elia's 4-day limit.")

    if all_fetched_records:
        logger.info(f"Finished fetching Elia forecasts. Total days: {days_to_fetch} rec: {len(all_fetched_records)}.")
        db_handler.store_elia_forecasts(all_fetched_records)
    else:
        logger.warning(f"No Elia forecast data fetched for {days_to_fetch} days. Check API.")
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)


def register_all_jobs(scheduler: BaseScheduler, db_handler: DatabaseHandler,
                      app_config: dict, p1_client: Optional[P1MeterHomeWizard]):
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

        # Register P1 Meter job if applicable
        if p1_client:
            p1_meter_schedule = tasks_config.get(P1_METER_JOB_ID, {})
            poll_interval_sec = p1_meter_schedule.get('poll_interval_seconds', 60)
            register_job(
                scheduler,
                job_id=P1_METER_JOB_ID,
                func=task_poll_p1_meter,
                trigger="interval",
                trigger_args={"seconds": poll_interval_sec},
                job_args=[db_handler, p1_client],
                name="Poll P1 Smart Meter",
                grace_time=max(1, poll_interval_sec // 2),
            )
        else:
            logger.warning(f"P1 Meter client not initialized. '{P1_METER_JOB_ID}' job not scheduled.")

    except Exception as e:
        logger.critical(f"Failed to register scheduled jobs: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.ALARM)
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
        logger.error(f"Failed to schedule job '{job_id}': {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)


def populate_price_data_in_appstate(db_handler: DatabaseHandler, target_day_local: datetime, app_config: dict,
                                    app_state_key: str, force_api_fetch_if_missing: bool = False):
    """
    Ensures price data for target_day_local is in AppState.
    Tries DB first. If missing and force_api_fetch_if_missing is True, tries API.
    """
    logger.info(f"Populating price data for AppState key '{app_state_key}' (date: {target_day_local.date()})")

    # Target_day_local is timezone-aware
    local_tz = target_day_local.tzinfo or datetime.now().astimezone().tzinfo
    target_day_local = target_day_local.replace(tzinfo=local_tz)

    # Try to get from database
    price_points = db_handler.get_da_prices(target_day_local)

    if not price_points and force_api_fetch_if_missing:
        logger.info(f"No DB data for '{app_state_key}' on {target_day_local.date()}, attempting API fetch.")
        price_points = day_ahead_price_api.fetch_entsoe_prices(target_day_local, app_config)

    # Process the price points if available
    return process_price_points(price_points, db_handler, target_day_local, app_state_key)
