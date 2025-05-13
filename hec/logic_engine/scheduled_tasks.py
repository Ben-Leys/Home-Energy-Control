# hec/logic_engine/scheduled_tasks.py
import logging
import os
from datetime import datetime, timedelta, timezone

from hec.core.app_state import GLOBAL_APP_STATE
from hec.data_sources import day_ahead_price_api  # Assuming PricePoint is here
from hec.database_ops.db_handler import DatabaseHandler
from hec import constants
from apscheduler.schedulers.base import BaseScheduler
from hec.logic_engine.utils import convert_utc_price_points_to_local_intervals


logger = logging.getLogger(__name__)


# --- Scheduled Tasks ---
FETCH_PRICES_JOB_ID = "fetch_daily_day_ahead_prices"
MAX_PRICE_FETCH_ATTEMPTS = 12  # e.g., 12 attempts * 15 min = 3 hours
price_fetch_attempt_count = 0  # Global or better, managed via job metadata if possible


def task_fetch_and_store_day_ahead_prices(scheduler: BaseScheduler, db_handler: DatabaseHandler, app_config: dict):
    """
    Scheduled task to fetch day-ahead prices, store them, and update AppState.
    Handles retries by rescheduling itself if data is not yet available.
    """
    global price_fetch_attempt_count  # Simple way to track attempts for this example

    logger.info(f"Running task: Fetch and Store Day-Ahead Prices (Attempt: {price_fetch_attempt_count + 1})")

    # Determine target date: ENTSO-E auction is for D+1 (tomorrow)
    # If run at 13:00 on Monday, we fetch prices for Tuesday.
    local_now = datetime.now().astimezone()  # Get current local time with timezone
    target_day_for_prices = (local_now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    entsoe_api_key = os.getenv("ENTSOE_API_KEY")
    if not entsoe_api_key:
        logger.error("ENTSOE_API_KEY not found in environment. Cannot fetch prices.")
        GLOBAL_APP_STATE.set("app_state", constants.AppStatus.ALARM)
        if scheduler.get_job(FETCH_PRICES_JOB_ID):
            logger.info(f"Permanently removing job {FETCH_PRICES_JOB_ID} due to missing API key.")
            scheduler.remove_job(FETCH_PRICES_JOB_ID)
        return

    price_points = day_ahead_price_api.fetch_entsoe_prices(target_day_for_prices, entsoe_api_key)

    if price_points is None:
        logger.error(
            f"Critical error fetching prices for {target_day_for_prices.strftime('%Y-%m-%d')}. Check API key or ENTSO-E status.")
        price_fetch_attempt_count += 1
    elif not price_points:  # Empty list, data likely not published yet
        logger.warning(f"Prices for {target_day_for_prices.strftime('%Y-%m-%d')} not yet available from ENTSO-E.")
        price_fetch_attempt_count += 1
    else:
        logger.info(
            f"Successfully fetched {len(price_points)} price points for {target_day_for_prices.strftime('%Y-%m-%d')}.")
        db_handler.store_price_forecasts(price_points)  # Store raw PricePoint data

        # Process for AppState (tomorrow's prices)
        # Get the local timezone
        local_timezone = target_day_for_prices.tzinfo if target_day_for_prices.tzinfo else datetime.now().astimezone().tzinfo

        processed_prices_for_appstate = convert_utc_price_points_to_local_intervals(price_points, local_timezone)
        GLOBAL_APP_STATE.set("electricity_prices_tomorrow", processed_prices_for_appstate)
        logger.info(f"Updated AppState with {len(processed_prices_for_appstate)} price intervals for 'tomorrow'.")

        price_fetch_attempt_count = 0
        return

    # --- Handle Retries ---
    if price_fetch_attempt_count < MAX_PRICE_FETCH_ATTEMPTS:
        retry_interval_minutes = app_config.get('scheduler', {}).get('price_fetch_retry_interval_min', 15)
        next_run_time = datetime.now(timezone.utc) + timedelta(minutes=retry_interval_minutes)

        try:
            scheduler.modify_job(FETCH_PRICES_JOB_ID, next_run_time=next_run_time)
            logger.info(
                f"Price fetch job rescheduled to run at {next_run_time.astimezone().isoformat()} "
                f"(in {retry_interval_minutes} min).")
        except Exception as e:  # Catch JobLookupError if job was removed, or other errors
            logger.error(f"Could not reschedule price fetch job: {e}", exc_info=True)
    else:
        logger.error(f"Max retry attempts ({MAX_PRICE_FETCH_ATTEMPTS}) reached for fetching prices for "
                     f"{target_day_for_prices.strftime('%Y-%m-%d')}. Giving up for this day.")
        GLOBAL_APP_STATE.set("app_state", constants.AppStatus.DEGRADED)
        price_fetch_attempt_count = 0  # Reset for the next day's attempt


def populate_price_data_in_appstate(db_handler: DatabaseHandler, target_day_local: datetime,
                                    app_state_key: str, force_api_fetch_if_missing: bool = False):
    """
    Ensures price data for target_day_local is in AppState.
    Tries DB first. If missing and force_api_fetch_if_missing is True, tries API.
    """
    logger.info(f"Ensuring price data for AppState key '{app_state_key}' (date: {target_day_local.strftime('%Y-%m-%d')})")

    # Target_day_local is timezone-aware
    local_tz = target_day_local.tzinfo if target_day_local.tzinfo else datetime.now().astimezone().tzinfo
    target_day_local_aware = target_day_local.replace(tzinfo=local_tz)

    # Try to get from Database
    price_points_db = db_handler.get_price_forecasts_for_day(target_day_local_aware)

    if price_points_db:
        logger.info(f"Found {len(price_points_db)} price points in DB for {target_day_local_aware.date()}.")
        processed_prices = convert_utc_price_points_to_local_intervals(price_points_db, local_tz)
        GLOBAL_APP_STATE.set(app_state_key, processed_prices)
        logger.info(f"Loaded {len(processed_prices)} price intervals for '{app_state_key}' from DB into AppState.")
        return True  # Data loaded from DB

    logger.info(f"No prices for '{app_state_key}' ({target_day_local_aware.date()}) found in DB.")

    # If missing in DB and force_api_fetch_if_missing is True, try API
    if force_api_fetch_if_missing:
        logger.info(f"Attempting immediate API fetch for {target_day_local_aware.date()} for AppState key '{app_state_key}'.")

        entsoe_api_key = os.getenv("ENTSOE_API_KEY")
        if not entsoe_api_key:
            logger.error("ENTSOE_API_KEY not configured. Cannot perform API fetch for initial data.")
            GLOBAL_APP_STATE.set(app_state_key, [])
            return False

        api_fetch_target_day = target_day_local_aware

        price_points_api = day_ahead_price_api.fetch_entsoe_prices(api_fetch_target_day, entsoe_api_key)

        if price_points_api:  # API returned some data (could be empty list if not published)
            logger.debug(f"API fetch returned {len(price_points_api)} price points for {api_fetch_target_day.date()}.")
            if price_points_api:  # Actually got price data
                db_handler.store_price_forecasts(price_points_api)  # Store it in DB
                processed_prices = convert_utc_price_points_to_local_intervals(price_points_api, local_tz)
                GLOBAL_APP_STATE.set(app_state_key, processed_prices)
                logger.debug(f"Loaded {len(processed_prices)} price intervals for '{app_state_key}' from API into AppState.")
                return True # Data loaded from API
            else:  # API returned empty list (data not published yet for that day)
                logger.debug(f"API fetch for {api_fetch_target_day.date()} returned no data (likely not published yet).")
                GLOBAL_APP_STATE.set(app_state_key, []) # Set to empty
                return False # Data not available via API yet
        else:  # API call failed critically
            logger.error(f"Critical API fetch error for {api_fetch_target_day.date()}.")
            GLOBAL_APP_STATE.set(app_state_key, [])  # Set to empty
            return False
    else:
        # Not forcing API fetch, and not found in DB
        GLOBAL_APP_STATE.set(app_state_key, [])  # Ensure it's an empty list if no data
        return False


def task_midnight_rollover(db_handler: DatabaseHandler, app_config: dict):
    logger.info("Running task: Midnight Rollover")
    now_local_aware = datetime.now().astimezone()

    # Shift tomorrow's prices to today's
    prices_tomorrow = GLOBAL_APP_STATE.get("electricity_prices_tomorrow", [])  # Default to empty list
    GLOBAL_APP_STATE.set("electricity_prices_today", prices_tomorrow)
    GLOBAL_APP_STATE.set("electricity_prices_tomorrow", [])  # Clear tomorrow
    logger.info("Shifted 'tomorrow' prices to 'today'. 'Tomorrow' prices are now empty in AppState.")

    # TODO: Similar logic for solar forecasts etc.

    # After rollover, "tomorrow" (which is now D+1 from the current new day) might be empty.
    # We can trigger an immediate check/fetch for the new "tomorrow".
    new_tomorrow_local_start = (now_local_aware + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    ensure_daily_price_data(
        db_handler,
        app_config,
        new_tomorrow_local_start,
        "electricity_prices_tomorrow",
        force_api_fetch_if_missing=True  # Try to get the new tomorrow's data
    )


def register_all_jobs(scheduler: BaseScheduler, db_handler: DatabaseHandler, app_config: dict):
    """Registers all defined scheduled jobs with the provided scheduler instance."""

    schedule_times = app_config.get('tasks_schedule', {})

    # Job to fetch Day-Ahead Prices
    scheduler.add_job(
        task_fetch_and_store_day_ahead_prices,
        trigger='cron',
        hour=schedule_times.get('fetch_prices_hour', 13),
        minute=schedule_times.get('fetch_prices_minute', 5),
        id=FETCH_PRICES_JOB_ID,
        args=[scheduler, db_handler, app_config],  # Pass scheduler for self-rescheduling
        name="Fetch Day-Ahead ENTSO-E Prices",
        replace_existing=True  # If re-registering jobs on app restart
    )
    logger.info(f"Job '{FETCH_PRICES_JOB_ID}' scheduled: CRON Daily at 13:00.")
