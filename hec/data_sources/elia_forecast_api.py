# hec/data_sources/elia_forecast_api.py
import logging
import requests
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


# --- Common Helper for API Requests ---
def _fetch_elia_data(base_url: str, dataset_id: str, date_str: str, url_params: str) -> Optional[List[Dict[str, Any]]]:
    """
    Generic function to fetch data from Elia Open Data API.

    Args:
        base_url (str): Elia Open Data API base URL.
        dataset_id (str): The Elia dataset ID
        date_str (str): The Elia date string
        url_params (str): params to pass to Elia Open Data API

    Returns:
        Optional[List[Dict[str, Any]]]: List of result dictionaries, or None on critical error.
    """

    url = f"{base_url}/{dataset_id}/records{url_params}"
    logger.debug(f"Elia API: Fetching from {url}")

    response = ''
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        results = data.get('results', [])
        if not results:
            logger.warning(f"Elia API ({dataset_id}): No results found for date {date_str}.")
            return []  # Return empty list if no data, not None

        # Check for API specific errors in the results structure
        if 'message' in data and isinstance(data['message'], dict) and data['message'].get('type') == 'error':
            logger.warning(f"Elia API ({dataset_id}): returned error: {data['message']['text']} for date {date_str}.")
            return None  # Critical API error

        return results

    except requests.exceptions.RequestException as e:
        logger.warning(f"Elia API ({dataset_id}): Request failed for date {date_str}: {e}", exc_info=True)
        return None
    except json.JSONDecodeError as e:
        logger.warning(f"Elia API ({dataset_id}): Failed to decode JSON for {date_str}: {e}", exc_info=True)
        logger.debug(f"Elia API ({dataset_id}): Raw response: {response.text if response else 'N/A'}")
        return None
    except Exception as e:
        logger.warning(f"Elia API ({dataset_id}): Error fetching data for {date_str}: {e}", exc_info=True)
        return None


# --- Specific Forecast Fetchers ---
def fetch_forecast(target_day_local: datetime, app_config: dict, forecast_type: str) -> Optional[List[Dict[str, Any]]]:
    """Fetches forecast data for a specific type (solar, wind, grid_load)."""
    config = _load_config(app_config)

    dataset_map = {
        "solar": config['dataset_solar'],
        "wind": config['dataset_wind'],
        "grid_load": config['dataset_grid_load'],
    }

    dataset_id = dataset_map.get(forecast_type)
    if not dataset_id:
        logger.warning(f"Invalid forecast type: {forecast_type}")
        return None

    date_str = target_day_local.strftime('%Y/%m/%d')
    select_params_map = {
        "solar": "datetime,mostrecentforecast,monitoredcapacity",
        "wind": "datetime,sum(mostrecentforecast),sum(monitoredcapacity)",
        "grid_load": "datetime,mostrecentforecast"
    }
    url_params = (
        f"?select={select_params_map[forecast_type]}"
        f"{'&group_by=datetime' if forecast_type == 'wind' else ''}"
        f"&order_by=datetime"
        f"&limit=100"
        f"&timezone={config['timezone']}"
        f"{'&refine=region%3A%22Flemish-Brabant%22' if forecast_type == 'solar' else ''}"
        f"&refine=datetime%3A{date_str}"
    )

    results = _fetch_elia_data(config['api_base_url'], dataset_id, date_str, url_params)
    if results is None:
        return None

    processed_records = []
    for item in results:
        try:
            record = {
                "timestamp_utc": item.get('datetime'),
                "forecast_type": forecast_type,
                "resolution_minutes": 15,
                "most_recent_forecast_mwh": round(item.get('mostrecentforecast', 0), 3),
                "monitored_capacity_mw": round(item.get('monitoredcapacity', 0),
                                               3) if 'monitoredcapacity' in item else None,
            }
            if forecast_type == "wind":
                record["most_recent_forecast_mwh"] = round(item.get('sum(mostrecentforecast)', 0), 3)
                record["monitored_capacity_mw"] = round(item.get('sum(monitoredcapacity)', 0), 3)
            processed_records.append(record)
        except Exception as e:
            logger.warning(f"Elia {forecast_type.capitalize()}: Error processing record {item}: {e}")
    return processed_records


def _load_config(app_config: dict) -> dict:
    """Loads and returns the Elia API configuration."""
    elia_config = app_config.get('elia', {})
    return {
        "api_base_url": elia_config.get('api_base_url', ''),
        "timezone": elia_config.get('timezone', ''),
        "dataset_solar": elia_config.get('dataset_solar', ''),
        "dataset_wind": elia_config.get('dataset_wind', ''),
        "dataset_grid_load": elia_config.get('dataset_grid_load', ''),
    }


# --- For testing this module directly ---
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    test_config = {"elia": {"api_base_url": "https://opendata.elia.be/api/explore/v2.1/catalog/datasets",
                            "timezone": "UTC", "dataset_solar": "ods087", "dataset_wind": "ods086",
                            "dataset_grid_load": "ods002"}}

    test_target_day = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0,
                                                                               microsecond=0)

    for f_type in ["solar", "wind", "grid_load"]:
        print(f"\nFetching {f_type.capitalize()} Forecast for {test_target_day.strftime('%Y-%m-%d')}:")
        result = fetch_forecast(test_target_day, test_config, f_type)
        if result:
            print(f"  Fetched {len(result)} records. Example: {result[0]}")
        else:
            print(f"  Failed or no {f_type} data.")
