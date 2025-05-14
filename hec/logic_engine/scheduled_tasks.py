# hec/logic_engine/scheduled_tasks.py
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from apscheduler.schedulers.base import BaseScheduler

from hec import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.app_initializer import populate_price_data_in_appstate
from hec.data_sources import day_ahead_price_api
from hec.data_sources.p1_meter_homewizard import P1MeterHomeWizard
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.utils import convert_utc_price_points_to_local, parse_hh_mm_time_string


logger = logging.getLogger(__name__)


# --- Scheduled Tasks ---
FETCH_PRICES_JOB_ID = "fetch_day_ahead_prices"
fetch_prices_attempt_count = 0
MIDNIGHT_ROLLOVER_JOB_ID = "midnight_rollover"
P1_METER_JOB_ID = "p1_meter_update"


def task_fetch_and_store_day_ahead_prices(scheduler: BaseScheduler, db_handler: DatabaseHandler, app_config: dict):
    """
    Scheduled task to fetch day-ahead prices, store them in database, and update AppState.
    Handles retries by rescheduling itself if data is not yet available.
    """
    global fetch_prices_attempt_count
    day_ahead_schedule = app_config.get("scheduler", {}).get(FETCH_PRICES_JOB_ID, {})
    max_retries = day_ahead_schedule.get("max_retries", 36)
    retry_after = day_ahead_schedule.get("retry_after", 0)

    logger.info(f"Running task: Fetch and Store Day-Ahead Prices (Attempt: {fetch_prices_attempt_count + 1})")

    # Determine target date: ENTSO-E auction is for D+1 (tomorrow)
    local_now = datetime.now().astimezone()  # Get current local time with timezone
    target_day_for_prices = local_now + timedelta(days=1)

    price_points = day_ahead_price_api.fetch_entsoe_prices(target_day_for_prices)

    if price_points is None:
        logger.error(
            f"Error fetching prices for {target_day_for_prices.strftime('%Y-%m-%d')}. Check API key or ENTSO-E status.")
        fetch_prices_attempt_count += 1
    elif not price_points:  # Empty list, data likely not published yet
        if fetch_prices_attempt_count < max_retries:
            logger.info(f"Prices for {target_day_for_prices.strftime('%Y-%m-%d')} not yet available from ENTSO-E.")
        fetch_prices_attempt_count += 1
    else:
        logger.info(
            f"Successfully fetched {len(price_points)} price points for {target_day_for_prices.strftime('%Y-%m-%d')}.")
        db_handler.store_da_prices(price_points)  # Store raw PricePoint data

        # Process for AppState (tomorrow's prices)
        local_tz = target_day_for_prices.tzinfo if target_day_for_prices.tzinfo else datetime.now().astimezone().tzinfo

        processed_prices_for_appstate = convert_utc_price_points_to_local(price_points, local_tz)
        GLOBAL_APP_STATE.set("electricity_prices_tomorrow", processed_prices_for_appstate)
        logger.info(f"Updated AppState with {len(processed_prices_for_appstate)} price points for tomorrow.")

        fetch_prices_attempt_count = 0
        return

    # --- Handle Retries ---
    if fetch_prices_attempt_count < max_retries:
        retry_interval_minutes = app_config.get('scheduler', {}).get('price_fetch_retry_interval_min', 15)
        next_run_time = datetime.now(timezone.utc) + timedelta(minutes=retry_after)

        try:
            scheduler.modify_job(FETCH_PRICES_JOB_ID, next_run_time=next_run_time)
            logger.info(f"Price fetch job rescheduled to run at {next_run_time.astimezone().isoformat()} "
                        f"(in {retry_interval_minutes} min).")
        except Exception as e:  # Catch JobLookupError if job was removed
            logger.error(f"Could not reschedule price fetch job: {e}", exc_info=True)
    else:
        logger.error(f"Max retry attempts ({max_retries}) reached for fetching prices for "
                     f"{target_day_for_prices.strftime('%Y-%m-%d')}. Giving up.")
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
        populate_price_data_in_appstate(db_handler, local_now, "electricity_prices_today",
                                        force_api_fetch_if_missing=True)

    # TODO: Similar logic for renew forecasts etc.


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


def register_all_jobs(scheduler: BaseScheduler, db_handler: DatabaseHandler,
                      app_config: dict, p1_client: Optional[P1MeterHomeWizard]):
    """Registers all defined scheduled jobs with the provided scheduler instance."""

    logger.info("Registering scheduled jobs...")
    try:
        tasks_config = app_config.get("tasks_schedule", {})

        # Job to fetch Day-Ahead Prices
        day_ahead_schedule = tasks_config.get(FETCH_PRICES_JOB_ID, {})
        scheduled_time = day_ahead_schedule.get('time')
        parsed_time = parse_hh_mm_time_string(scheduled_time)
        if parsed_time:
            hour, minute = parsed_time
            scheduler.add_job(
                task_fetch_and_store_day_ahead_prices,
                trigger='cron',
                hour=hour,
                minute=minute,
                id=FETCH_PRICES_JOB_ID,
                args=[scheduler, db_handler, app_config],  # Pass scheduler for self-rescheduling
                name="Fetch Day-Ahead ENTSO-E Prices",
                misfire_grace_time=32400,  # 9 hours later still within the same day
                replace_existing=True  # If re-registering jobs on app restart
            )
            logger.info(f"Job '{FETCH_PRICES_JOB_ID}' scheduled: CRON Daily at {scheduled_time}.")
        else:
            logger.warning(f"Could not parse time {scheduled_time}. Skipping job {FETCH_PRICES_JOB_ID}.")

        # Job to make midnight rollover
        rollover_schedule = tasks_config.get(MIDNIGHT_ROLLOVER_JOB_ID, {})
        scheduled_time = rollover_schedule.get('time')
        parsed_time = parse_hh_mm_time_string(scheduled_time)
        if parsed_time:
            hour, minute = parsed_time
            scheduler.add_job(
                task_midnight_rollover,
                trigger='cron',
                hour=hour,
                minute=minute,
                id=MIDNIGHT_ROLLOVER_JOB_ID,
                args=[db_handler, app_config],
                name="Midnight rollover",
                misfire_grace_time=50400,  # 13 hours later tomorrow's prices will be overwritten by day ahead fetch
                replace_existing=True  # If re-registering jobs on app restart
            )
            logger.info(f"Job '{MIDNIGHT_ROLLOVER_JOB_ID}' scheduled: CRON Daily at {scheduled_time}.")
        else:
            logger.warning(f"Could not parse time {scheduled_time}. Skipping job {MIDNIGHT_ROLLOVER_JOB_ID}.")

        # Update P1 meter data
        if p1_client:
            p1_meter_schedule = tasks_config.get(P1_METER_JOB_ID, {})
            p1_poll_interval_sec = p1_meter_schedule.get('poll_interval_seconds', 60)
            scheduler.add_job(
                task_poll_p1_meter,
                trigger='interval',
                seconds=p1_poll_interval_sec,
                id=P1_METER_JOB_ID,
                args=[db_handler, p1_client],
                name="Poll P1 Smart Meter",
                misfire_grace_time=max(1, int(p1_poll_interval_sec / 2)),
                replace_existing=True  # If re-registering jobs on app restart
            )
            logger.info(f"Job '{P1_METER_JOB_ID}' scheduled: interval {p1_poll_interval_sec} seconds.")
        else:
            logger.warning("P1 Meter client not initialized. P1 polling job not scheduled.")

    except Exception as e:
        logger.critical(f"Failed to register scheduled jobs: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.ALARM)
        db_handler.close_connection()  # Clean up
        return
