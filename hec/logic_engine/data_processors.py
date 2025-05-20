import logging
from collections import deque
from datetime import datetime, timedelta, time, timezone
from typing import Deque, Dict, Tuple, Optional

from hec.core import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.data_sources.api_entsoe import fetch_entsoe_prices
from hec.database_ops.db_handler import DatabaseHandler
from hec.utils.utils import process_price_points_to_app_state

logger = logging.getLogger(__name__)

AVERAGE_WINDOWS_SECONDS: Dict[str, int] = {
    "30s": 30,
    "60s": 60,
    "2m": 120,
    "3m": 180,
    "5m": 300,
    "10m": 600,
    "15m": 900,
}
MAX_HISTORY_SECONDS = max(AVERAGE_WINDOWS_SECONDS.values())


def populate_appstate_with_price_data(db_handler: DatabaseHandler, app_config: dict,
                                      force_api_fetch_if_missing: bool = False):
    """
    Ensures price data for today and tomorrow is in AppState.
    Tries DB first. If missing and force_api_fetch_if_missing is True, tries API.
    """
    logger.info(f"Populating price data for AppState")

    # Target day is timezone-aware
    local_now = datetime.now().astimezone()
    local_tomorrow = local_now + timedelta(days=1)

    for day, key in [(local_now, "electricity_prices_today"),
                     (local_tomorrow, "electricity_prices_tomorrow")]:
        # Try to get from database
        price_points = db_handler.get_da_prices(day)

        # If DB is empty and API fetching is allowed, fetch from API
        store_to_db = False
        if not price_points and force_api_fetch_if_missing:
            logger.info(f"No DB data for '{key}' on {day.date()}, attempting API fetch.")
            price_points = fetch_entsoe_prices(day, app_config)
            store_to_db = True if price_points else False
        if not price_points:
            continue

        # Process the price points (if any)
        process_price_points_to_app_state(price_points, day, key, app_config,
                                          db_handler if store_to_db else None)

    if not GLOBAL_APP_STATE.get("electricity_prices_today"):
        logger.warning("No 'electricity_prices_today' found in AppState. Price-based decisions will fail.")


def populate_appstate_with_forecast_data(db_handler: DatabaseHandler):
    """Loads forecast data from DB into appstate for now + 4 or 5 days depending on type."""

    local_now = datetime.combine(datetime.now().astimezone(), time.min)

    logger.info("Populating AppState with forecast data...")
    forecast_days = {"wind": 5, "solar": 5, "grid_load": 4}
    forecasts = {
        f_type: db_handler.get_elia_forecasts(f_type, local_now, local_now + timedelta(days=days))
        for f_type, days in forecast_days.items()
    }
    GLOBAL_APP_STATE.set("forecasts", forecasts)


def _initialize_rolling_average_structures_if_needed():
    """Initializes the deque structures in AppState if they don't exist."""
    if GLOBAL_APP_STATE.get("recent_p1_import_kwh_samples") is None:
        GLOBAL_APP_STATE.set("recent_p1_import_kwh_samples", deque(maxlen=int(MAX_HISTORY_SECONDS / 10) + 5))
    if GLOBAL_APP_STATE.get("recent_p1_export_kwh_samples") is None:
        GLOBAL_APP_STATE.set("recent_p1_export_kwh_samples", deque(maxlen=int(MAX_HISTORY_SECONDS / 10) + 5))
    if GLOBAL_APP_STATE.get("recent_solar_production_wh_samples") is None:
        GLOBAL_APP_STATE.set("recent_solar_production_wh_samples", deque(maxlen=int(MAX_HISTORY_SECONDS / 10) + 5))


def _update_samples_deque(
        samples_deque: Deque[Tuple[datetime, float]],
        new_timestamp_utc: datetime,
        new_cumulative_value: float
):
    """
    Adds a new sample to the deque and removes old samples.
    Assumes new_cumulative_value is always increasing or same.
    """
    if new_cumulative_value is None:
        return

    # Remove samples older than MAX_HISTORY_SECONDS from the new_timestamp_utc
    while samples_deque and (
            new_timestamp_utc - samples_deque[0][0]).total_seconds() > MAX_HISTORY_SECONDS + 60:  # With buffer
        samples_deque.popleft()

    # Add the new sample if it's newer than the last one
    if not samples_deque or new_timestamp_utc > samples_deque[-1][0]:
        samples_deque.append((new_timestamp_utc, new_cumulative_value))
    elif new_timestamp_utc == samples_deque[-1][0]:
        samples_deque[-1] = (new_timestamp_utc, new_cumulative_value)


def _calculate_average_power_from_samples(
        samples_deque: Deque[Tuple[datetime, float]],
        window_seconds: int,
        unit_conversion_factor: float = 1.0
) -> Optional[float]:
    """
    Calculates the average power (e.g., Watts) over a given window from cumulative energy samples.
    unit_conversion_factor: Multiply delta_energy by this to get desired power unit numerator (e.g., Wh)
                            The result is then divided by actual_duration_hours.
    """
    if not samples_deque or len(samples_deque) < 2:
        return None

    now_utc = datetime.now(timezone.utc)

    first_relevant_sample = None
    last_relevant_sample = samples_deque[-1]

    # Iterate backwards to find the sample that is at least `window_seconds` old,
    # or the oldest sample if the deque doesn't span the full window.
    for i in range(len(samples_deque) - 1, -1, -1):
        ts, val = samples_deque[i]
        if (now_utc - ts).total_seconds() <= window_seconds:
            first_relevant_sample = (ts, val)
        else:
            if i + 1 < len(samples_deque):  # If there's a next sample, that's closer to window start
                first_relevant_sample = samples_deque[i + 1]
            else:
                first_relevant_sample = samples_deque[i]  # Use the oldest available as start
            break

    if first_relevant_sample is None:  # Should only happen if deque just got its first entry
        if len(samples_deque) >= 1:
            first_relevant_sample = samples_deque[0]
        else:
            return None

    ts_start, val_start = first_relevant_sample
    ts_end, val_end = last_relevant_sample

    if ts_end <= ts_start:
        return None

    delta_energy = val_end - val_start
    actual_duration_seconds = (ts_end - ts_start).total_seconds()

    if actual_duration_seconds <= 0:  # Avoid division by zero
        return None

    # Average power = (change in energy * conversion_factor) / (change in time)
    # Example: if delta_energy is in kWh, and we want Watts:
    # (delta_energy_kwh * 1000 Wh/kWh) / (actual_duration_seconds / 3600 s/h)
    # = delta_energy_kwh * 1000 * 3600 / actual_duration_seconds
    # Unit_conversion_factor is for energy (1000 if input is kWh, and we want Wh for power calc)
    # Power (Watts) = (delta_energy_in_Wh) / (duration_in_hours)
    power = (delta_energy * unit_conversion_factor) / (actual_duration_seconds / 3600)

    # Sanity check (if meter resets, delta_energy could be negative)
    if delta_energy < 0:
        logger.warning(f"Negative energy delta ({delta_energy}) detected for window {window_seconds}s. "
                       f"Start: {val_start} @ {ts_start}, End: {val_end} @ {ts_end}. This could be a meter error.")
        return None

    return round(power, 3)


def update_rolling_averages():
    """
    Called periodically by scheduled task.
    Updates recent data dequeue and calculates rolling averages for AppState.
    """
    _initialize_rolling_average_structures_if_needed()
    now_utc = datetime.now(timezone.utc)

    # --- 1. P1 Meter Data ---
    p1_data = GLOBAL_APP_STATE.get("p1_meter_data")
    if p1_data and isinstance(p1_data, dict):
        try:
            p1_timestamp_str = p1_data.get("timestamp_utc_iso")
            p1_timestamp_utc = datetime.fromisoformat(p1_timestamp_str) if p1_timestamp_str else now_utc

            current_total_import_kwh = p1_data.get("total_power_import_kwh")
            current_total_export_kwh = p1_data.get("total_power_export_kwh")

            if current_total_import_kwh is not None:
                _update_samples_deque(
                    GLOBAL_APP_STATE.get("recent_p1_import_kwh_samples"),
                    p1_timestamp_utc,
                    float(current_total_import_kwh)
                )
            if current_total_export_kwh is not None:
                _update_samples_deque(
                    GLOBAL_APP_STATE.get("recent_p1_export_kwh_samples"),
                    p1_timestamp_utc,
                    float(current_total_export_kwh)
                )
        except Exception as e:
            logger.error(f"Error processing P1 live data for rolling averages: {e}", exc_info=True)

    # Calculate P1 averages
    avg_import_watts = {}
    avg_export_watts = {}
    for name, seconds in AVERAGE_WINDOWS_SECONDS.items():
        avg_import_watts[name] = _calculate_average_power_from_samples(
            GLOBAL_APP_STATE.get("recent_p1_import_kwh_samples"), seconds, unit_conversion_factor=1000  # kWh to Wh
        )
        avg_export_watts[name] = _calculate_average_power_from_samples(
            GLOBAL_APP_STATE.get("recent_p1_export_kwh_samples"), seconds, unit_conversion_factor=1000
        )
    GLOBAL_APP_STATE.set("average_grid_import_watts", avg_import_watts)
    GLOBAL_APP_STATE.set("average_grid_export_watts", avg_export_watts)

    logger.debug(f"Updated P1 rolling averages. 5min Import: {avg_import_watts.get('5m')} W, "
                 f"5min Export: {avg_export_watts.get('5m')} W")

    # --- 2. Solar Production Data ---
    avg_solar_watts = {}  # Initialize to empty because values not always stored (at night or when not needed)

    inverter_data = GLOBAL_APP_STATE.get("inverter_data")
    if inverter_data and isinstance(inverter_data, dict) and \
            inverter_data.get("operational_status") != c.InverterStatus.OFFLINE.name and \
            inverter_data.get("operational_status") != c.InverterStatus.UNKNOWN.name:  # Only if inverter is responsive

        try:
            inv_timestamp_str = inverter_data.get("timestamp_utc_iso")
            inv_timestamp_utc = datetime.fromisoformat(inv_timestamp_str) if inv_timestamp_str else now_utc

            current_daily_yield_wh = inverter_data.get("daily_yield_wh")

            if current_daily_yield_wh is not None:
                _update_samples_deque(
                    GLOBAL_APP_STATE.get("recent_solar_production_wh_samples"),
                    inv_timestamp_utc,
                    float(current_daily_yield_wh)
                )
            # Will daily_yield_wh give exact enough results for short average times (15-30 seconds)?
            # If not: switch to modified logic below where averages are calculated with current
            # pv_power. This however could possibly give readings that are off for longer times
            # because it's not really an average production but an average of momentary production readings

            # --- MODIFIED SOLAR LOGIC: Using inverter_data["pv_power_watts"] ---
            # current_solar_watts = inverter_data.get("pv_power_watts")
            # if current_solar_watts is not None:
            #     solar_samples_deque: Deque[Tuple[datetime, float]] = GLOBAL_APP_STATE.get(
            #         "recent_solar_production_wh_samples")
            #     # Remove old samples
            #     while solar_samples_deque and (
            #             now_utc - solar_samples_deque[0][0]).total_seconds() > MAX_HISTORY_SECONDS + 60:
            #         solar_samples_deque.popleft()
            #     # Add new sample
            #     if not solar_samples_deque or inv_timestamp_utc > solar_samples_deque[-1][0]:
            #         solar_samples_deque.append((inv_timestamp_utc, float(current_solar_watts)))
            #     elif inv_timestamp_utc == solar_samples_deque[-1][0]:
            #         solar_samples_deque[-1] = (inv_timestamp_utc, float(current_solar_watts))
            #
            #     # Calculate averages for solar (simple average of instantaneous readings in window)
            #     for name, seconds in AVERAGE_WINDOWS_SECONDS.items():
            #         relevant_samples = [val for ts, val in solar_samples_deque if
            #                             (now_utc - ts).total_seconds() <= seconds]
            #         avg_solar_watts[name] = round(sum(relevant_samples) / len(relevant_samples),
            #                                       1) if relevant_samples else None
            # --- END OF MODIFIED SOLAR LOGIC ---

        except Exception as e:
            logger.error(f"Error processing inverter live data for rolling averages: {e}", exc_info=True)

    for name, seconds in AVERAGE_WINDOWS_SECONDS.items():
        avg_solar_watts[name] = _calculate_average_power_from_samples(
            GLOBAL_APP_STATE.get("recent_solar_production_wh_samples"), seconds, unit_conversion_factor=1)  # In Wh

    GLOBAL_APP_STATE.set("average_solar_production_watts", avg_solar_watts)
    if avg_solar_watts:  # Log only if updated
        logger.debug(f"Updated Solar rolling averages. 5min Solar: {avg_solar_watts.get('5m')} W")
