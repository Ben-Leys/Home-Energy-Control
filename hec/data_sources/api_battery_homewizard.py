# hec/data_sources/api_battery_homewizard.py
import json
import logging
import requests
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class BatteryHomeWizard:
    def __init__(self, name: str, host: str, token: str, request_timeout: int = 10):
        """
        Initializes the HomeWizard Battery.

        Args:
            name (str): Friendly name for the battery (e.g., "Garage", "Boven").
            host (str): The IP address or hostname of the battery.
            token (str): The bearer token used for API authentication.
            request_timeout (int): Timeout in seconds for HTTP requests.
        """
        self.name: str = name
        self.host: str = host
        self.token: str = token
        self.request_timeout: int = request_timeout
        self.api_url: str = f"https://{self.host}/api"
        self.api_measurement_url: str = f"{self.api_url}/measurement"
        self.is_initialized: bool = False

        self._initialize_connection()

    def _initialize_connection(self):
        """Attempts to verify connection to the battery API."""
        logger.info(f"Battery [{self.name}]: Initializing connection at {self.api_url}")
        try:
            response = requests.get(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "X-Api-Version": "2",
                },
                verify=False,  # local HTTPS with self-signed cert
                timeout=self.request_timeout,
            )
            if response.status_code == 200:
                logger.info(f"Battery [{self.name}]: Successfully initialized.")
                self.is_initialized = True
            else:
                logger.warning(
                    f"Battery [{self.name}]: Initialization failed "
                    f"(HTTP {response.status_code}: {response.reason})"
                )
        except requests.RequestException as e:
            logger.error(f"Battery [{self.name}]: Connection attempt failed: {e}")
            self.is_initialized = False

    def refresh_data(self) -> Optional[Dict[str, Any]]:
        """
        Fetches the latest data from the battery API.

        Returns:
            A dictionary containing:
            - total_import_kwh
            - total_export_kwh
            - state_of_charge (percentage)
            - cycles
            - timestamp_utc_iso
        """
        if not self.is_initialized:
            logger.warning(f"Battery [{self.name}]: Not initialized, skipping refresh.")
            return None

        try:
            response = requests.get(
                self.api_measurement_url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "X-Api-Version": "2",
                },
                verify=False,
                timeout=self.request_timeout,
            )
            response.raise_for_status()
            data = response.json()

            battery_data = {
                "battery_name": self.name,
                "energy_import_kwh": data.get("energy_import_kwh"),
                "energy_export_kwh": data.get("energy_export_kwh"),
                "state_of_charge_pct": data.get("state_of_charge_pct"),
                "cycles": data.get("cycles"),
                "timestamp_utc_iso": datetime.now(timezone.utc).isoformat(),
            }

            logger.debug(
                f"Battery [{self.name}]: Data fetched - "
                f"SoC: {battery_data['state_of_charge_pct']}%, "
                f"Import: {battery_data['energy_import_kwh']} kWh, "
                f"Export: {battery_data['energy_export_kwh']} kWh"
            )

            return battery_data

        except requests.exceptions.Timeout:
            logger.warning(f"Battery [{self.name}]: Request timed out.")
            return None
        except requests.exceptions.ConnectionError:
            logger.warning(f"Battery [{self.name}]: Connection error.")
            return None
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Battery [{self.name}]: HTTP error: {e}")
            return None
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            logger.error(f"Battery [{self.name}]: Error while fetching data: {e}", exc_info=True)
            return None
