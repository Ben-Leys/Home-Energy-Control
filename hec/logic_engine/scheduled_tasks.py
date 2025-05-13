# hec/logic_engine/scheduled_tasks.py
import logging
import os
from datetime import datetime, timedelta, timezone

from hec.core.app_state import GLOBAL_APP_STATE
from hec.data_sources import day_ahead_price_api  # Assuming PricePoint is here
from hec.database_ops.db_handler import DatabaseHandler
from hec import constants
from apscheduler.schedulers.base import BaseScheduler
from hec.logic_engine.utils import convert_utc_price_points_to_local, parse_hh_mm_time_string

logger = logging.getLogger(__name__)


# --- Scheduled Tasks ---
FETCH_PRICES_JOB_ID = "fetch_day_ahead_prices"
price_fetch_attempt_count = 0  # Global or better, managed via job metadata if possible


def task_fetch_and_store_day_ahead_prices(scheduler: BaseScheduler, db_handler: DatabaseHandler, app_config: dict):
    """
    Scheduled task to fetch day-ahead prices, store them, and update AppState.
    Handles retries by rescheduling itself if data is not yet available.
    """
    global price_fetch_attempt_count

    logger.info(f"Running task: Fetch and Store Day-Ahead Prices (Attempt: {price_fetch_attempt_count + 1})")

    # Determine target date: ENTSO-E auction is for D+1 (tomorrow)
    local_now = datetime.now().astimezone()  # Get current local time with timezone
    target_day_for_prices = local_now + timedelta(days=1)

    price_points = day_ahead_price_api.fetch_entsoe_prices(target_day_for_prices)

    if price_points is None:
        logger.error(
            f"Error fetching prices for {target_day_for_prices.strftime('%Y-%m-%d')}. Check API key or ENTSO-E status.")
        price_fetch_attempt_count += 1
    elif not price_points:  # Empty list, data likely not published yet
        logger.warning(f"Prices for {target_day_for_prices.strftime('%Y-%m-%d')} not yet available from ENTSO-E.")
        price_fetch_attempt_count += 1
    else:
        logger.info(
            f"Successfully fetched {len(price_points)} price points for {target_day_for_prices.strftime('%Y-%m-%d')}.")
        db_handler.store_price_forecasts(price_points)  # Store raw PricePoint data

        # Process for AppState (tomorrow's prices)
        local_tz = target_day_for_prices.tzinfo if target_day_for_prices.tzinfo else datetime.now().astimezone().tzinfo

        processed_prices_for_appstate = convert_utc_price_points_to_local(price_points, local_tz)
        GLOBAL_APP_STATE.set("electricity_prices_tomorrow", processed_prices_for_appstate)
        logger.info(f"Updated AppState with {len(processed_prices_for_appstate)} price points for tomorrow.")

        price_fetch_attempt_count = 0
        return

    # --- Handle Retries ---
    day_ahead_schedule = app_config.get("scheduler", {}).get(FETCH_PRICES_JOB_ID, {})
    max_retries = day_ahead_schedule.get("max_retries", 36)
    retry_after = day_ahead_schedule.get("retry_after", 0)
    if price_fetch_attempt_count < max_retries:
        retry_interval_minutes = app_config.get('scheduler', {}).get('price_fetch_retry_interval_min', 15)
        next_run_time = datetime.now(timezone.utc) + timedelta(minutes=retry_after)

        try:
            scheduler.modify_job(FETCH_PRICES_JOB_ID, next_run_time=next_run_time)
            logger.info(f"Price fetch job rescheduled to run at {next_run_time.astimezone().isoformat()} "
                        f"(in {retry_interval_minutes} min).")
        except Exception as e:  # Catch JobLookupError if job was removed, or other errors
            logger.error(f"Could not reschedule price fetch job: {e}", exc_info=True)
    else:
        logger.error(f"Max retry attempts ({max_retries}) reached for fetching prices for "
                     f"{target_day_for_prices.strftime('%Y-%m-%d')}. Giving up for this day.")
        GLOBAL_APP_STATE.set("app_state", constants.AppStatus.DEGRADED)
        price_fetch_attempt_count = 0  # Reset for the next day's attempt


def populate_price_data_in_appstate(db_handler: DatabaseHandler, target_day_local: datetime,
                                    app_state_key: str, force_api_fetch_if_missing: bool = False):
    """
    Ensures price data for target_day_local is in AppState.
    Tries DB first. If missing and force_api_fetch_if_missing is True, tries API.
    """
    logger.info(f"Populating price data for AppState key '{app_state_key}' (date: {target_day_local.strftime('%Y-%m-%d')})")

    # Target_day_local is timezone-aware
    local_tz = target_day_local.tzinfo if target_day_local.tzinfo else datetime.now().astimezone().tzinfo
    target_day_local_aware = target_day_local.replace(tzinfo=local_tz)

    # Try to get from Database
    price_points_db = db_handler.get_price_forecasts_for_day(target_day_local_aware)

    if price_points_db:
        processed_prices = convert_utc_price_points_to_local(price_points_db, local_tz)
        GLOBAL_APP_STATE.set(app_state_key, processed_prices)
        logger.info(f"Loaded {len(processed_prices)} price intervals for '{app_state_key}' from DB into AppState.")
        return True  # Data loaded from DB

    logger.info(f"No prices for '{app_state_key}' ({target_day_local_aware.date()}) found in DB.")

    # If missing in DB and force_api_fetch_if_missing is True, try API
    if force_api_fetch_if_missing:
        logger.info(f"Attempting API fetch for {target_day_local_aware.date()} for AppState key '{app_state_key}'.")

        price_points = day_ahead_price_api.fetch_entsoe_prices(target_day_local_aware)

        if price_points:  # API returned some data (could be empty list if not published)
            logger.debug(f"API fetch returned {len(price_points)} price points for {target_day_local_aware.date()}.")
            if len(price_points):  # Actually got price data
                db_handler.store_price_forecasts(price_points)  # Store it in DB
                processed_prices = convert_utc_price_points_to_local(price_points, local_tz)
                GLOBAL_APP_STATE.set(app_state_key, processed_prices)
                logger.debug(f"Loaded {len(processed_prices)} price points for '{app_state_key}' into AppState.")
                return True # Data loaded from API
            else:  # API returned empty list (data not published yet for that day)
                logger.debug(f"API fetch for {target_day_local_aware.date()} returned no data (not published yet?)")
                GLOBAL_APP_STATE.set(app_state_key, [])
                return False
        else:  # API call failed critically
            logger.error(f"Critical API fetch error for {target_day_local_aware.date()}.")
            GLOBAL_APP_STATE.set(app_state_key, [])
            return False
    else:
        # Not forcing API fetch, and not found in DB
        GLOBAL_APP_STATE.set(app_state_key, [])
        return False


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

    # TODO: Similar logic for solar forecasts etc.


def register_all_jobs(scheduler: BaseScheduler, db_handler: DatabaseHandler, app_config: dict):
    """Registers all defined scheduled jobs with the provided scheduler instance."""

    scheduler_config = app_config.get("scheduler", {})
    day_ahead_schedule = scheduler_config.get(FETCH_PRICES_JOB_ID, {})

    # Job to fetch Day-Ahead Prices
    scheduled_time = day_ahead_schedule.get('time')
    hour, minute = parse_hh_mm_time_string(scheduled_time)
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
