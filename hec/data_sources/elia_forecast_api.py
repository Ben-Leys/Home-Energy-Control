# hec/data_sources/elia_forecast_api.py
import logging
import requests
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

from hec.core import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.config_loader import load_app_config

logger = logging.getLogger(__name__)

APP_CONFIG = load_app_config()
elia_config = APP_CONFIG.get('elia')

elia_api_base_url = elia_config.get('api_base_url')
elia_timezone = elia_config.get('timezone')

# --- Dataset IDs for Elia Open Data ---
elia_dataset_solar = elia_config.get('dataset_solar')
elia_dataset_wind = elia_config.get('dataset_wind')
elia_dataset_grid_load = elia_config.get('dataset_grid_load')


# --- Common Helper for API Requests ---
def _fetch_elia_data(dataset_id: str, url_params: str) -> Optional[List[Dict[str, Any]]]:
    """
    Generic function to fetch data from Elia Open Data API.

    Args:
        dataset_id (str): The Elia dataset ID
        url_params (str): params to pass to Elia Open Data API

    Returns:
        Optional[List[Dict[str, Any]]]: List of result dictionaries, or None on critical error.
    """
    url = f"{elia_api_base_url}/{dataset_id}/records{url_params}"
    logger.debug(f"Elia API: Fetching from {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        results = data.get('results', [])
        if not results:
            logger.warning(f"Elia API ({dataset_id}): No results found for date {date_str_for_api}.")
            return []  # Return empty list if no data, not None

        # Check for API specific errors in the results structure
        if 'message' in data and isinstance(data['message'], dict) and data['message'].get('type') == 'error':
            logger.error(
                f"Elia API ({dataset_id}): API returned error: {data['message']['text']} for date {date_str_for_api}.")
            GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)
            return None  # Critical API error

        return results

    except requests.exceptions.RequestException as e:
        logger.error(f"Elia API ({dataset_id}): Request failed for date {date_str_for_api}: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Elia API ({dataset_id}): Failed to decode JSON for {date_str_for_api}: {e}", exc_info=True)
        logger.debug(f"Elia API ({dataset_id}): Raw response: {response.text if 'response' in locals() else 'N/A'}")
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)
        return None
    except Exception as e:
        logger.error(f"Elia API ({dataset_id}): Error fetching data for {date_str_for_api}: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)
        return None


# --- Specific Forecast Fetchers ---
def fetch_solar_production_forecast(target_day_local: datetime) -> Optional[List[Dict[str, Any]]]:
    """Fetches solar production forecast data for a single day from Elia Open Data."""
    date_str = target_day_local.strftime('%Y/%m/%d')
    select_params = "datetime,mostrecentforecast,monitoredcapacity"
    url_params = (
        f"?select={select_params}"
        f"&order_by=datetime"
        f"&limit=100"  # Max limit for a single call
        f"&timezone={elia_timezone}"
        f"&refine=region%3A%22Flemish-Brabant%22"
        f"&refine=datetime%3A{date_str}"
    )
    results = _fetch_elia_data(elia_dataset_solar, url_params)
    if results is None:
        return None

    processed_records = []
    for item in results:
        try:
            processed_records.append({
                "timestamp_utc": item.get('datetime'),
                "forecast_type": "solar",
                "resolution_minutes": 15,
                "most_recent_forecast_mwh": round(item.get('mostrecentforecast'), 3),
                "monitored_capacity_mw": round(item.get('monitoredcapacity'), 3),
            })
        except Exception as e:
            logger.warning(f"Elia Solar: Error processing record {item}: {e}")
            GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)
    return processed_records


def fetch_wind_production_forecast(target_day_local: datetime) -> Optional[List[Dict[str, Any]]]:
    """Fetches wind production forecast data for a single day from Elia Open Data."""
    date_str = target_day_local.strftime('%Y/%m/%d')
    select_params = "datetime,sum(mostrecentforecast),sum(monitoredcapacity)"
    url_params = (
        f"?select={select_params}"
        f"&group_by=datetime"
        f"&order_by=datetime"
        f"&limit=100"
        f"&timezone={elia_timezone}"
        f"&refine=datetime%3A{date_str}"
    )
    results = _fetch_elia_data(elia_dataset_wind, url_params)
    if results is None:
        return None

    processed_records = []
    for item in results:
        try:
            processed_records.append({
                "timestamp_utc": item.get('datetime'),
                "forecast_type": "wind",
                "resolution_minutes": 15,
                "most_recent_forecast_mwh": round(item.get('sum(mostrecentforecast)'), 2),
                "monitored_capacity_mw": round(item.get('sum(monitoredcapacity)'), 2)
            })
        except Exception as e:
            logger.warning(f"Elia Wind: Error processing record {item}: {e}")
            GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)
    return processed_records


def fetch_grid_load_forecast(target_day_local: datetime) -> Optional[List[Dict[str, Any]]]:
    """Fetches grid load forecast data for a single day from Elia Open Data."""
    date_str = target_day_local.strftime('%Y/%m/%d')
    select_params = "datetime,mostrecentforecast"
    url_params = (
        f"?select={select_params}"
        f"&order_by=datetime"
        f"&limit=100"
        f"&timezone={elia_timezone}"
        f"&refine=datetime%3A{date_str}"
    )
    results = _fetch_elia_data(elia_dataset_grid_load, url_params)
    if results is None:
        return None

    processed_records = []
    for item in results:
        try:
            processed_records.append({
                "timestamp_utc": item.get('datetime'),
                "forecast_type": "grid_load",
                "resolution_minutes": 15,  # Assuming 15 min resolution
                "most_recent_forecast_mwh": round(item.get('mostrecentforecast'), 2),
                "monitored_capacity_mw": None
            })
        except Exception as e:
            logger.warning(f"Elia Load: Error processing record {item}: {e}")
            GLOBAL_APP_STATE.set("app_state", c.AppStatus.WARNING)
    return processed_records


# --- For testing this module directly ---
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    print("--- Testing Elia Forecast API ---")

    # Test for tomorrow's data
    test_target_day = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    print(f"\nFetching Solar Forecast for {test_target_day.strftime('%Y-%m-%d')}:")
    solar_data = fetch_solar_production_forecast(test_target_day)
    if solar_data:
        print(f"  Fetched {len(solar_data)} records. Example: {solar_data[0]}")
    else:
        print("  Failed or no solar data.")

    print(f"\nFetching Wind Forecast for {test_target_day.strftime('%Y-%m-%d')}:")
    wind_data = fetch_wind_production_forecast(test_target_day)
    if wind_data:
        print(f"  Fetched {len(wind_data)} records. Example: {wind_data[0]}")
    else:
        print("  Failed or no wind data.")

    print(f"\nFetching Grid Load Forecast for {test_target_day.strftime('%Y-%m-%d')}:")
    load_data = fetch_grid_load_forecast(test_target_day)
    if load_data:
        print(f"  Fetched {len(load_data)} records. Example: {load_data[0]}")
    else:
        print("  Failed or no load data.")

    print("\n--- Elia Forecast API Testing Complete ---")
