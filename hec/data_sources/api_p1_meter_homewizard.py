# hec/data_sources/api_p1_meter_homewizard.py
import json
import logging
import requests
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class P1MeterHomewizardClient:
    def __init__(self, host: str = None, token: str = None, request_timeout: int = 10):
        """
        Initializes the HomeWizard P1 Meter client.
        Tries to use host if provided, otherwise attempts discovery via MAC (if func get_ip_by_mac is available).

        Args:
            host (Optional[str]): The IP address or hostname of the P1 meter.
            request_timeout (int): Timeout in seconds for HTTP requests.
        """
        self.meter_ip: str = host
        self.data_url: Optional[str] = None
        self.battery_url: Optional[str] = None
        self.request_timeout: int = request_timeout
        self.is_initialized: bool = False
        self.token: str = token

        self._initialize_connection()

    def _initialize_connection(self):
        """Attempts to establish the base URL for the P1 meter."""
        if self.meter_ip:
            self.data_url = f"http://{self.meter_ip}/api/v1/data"
            self.battery_url = f"https://{self.meter_ip}/api/batteries"
            logger.info(f"P1 Meter: Configured with host {self.meter_ip}. URL: {self.data_url}")
            try:
                # Check if the URL is reachable
                response = requests.get(self.data_url, timeout=5)
                if response.status_code == 200:
                    logger.info(f"Successfully initialized P1 Meter.")
                    self.is_initialized = True
                    return
                else:
                    logger.warning(f"Failed to connect. HTTP Status: {response.status_code}")
            except requests.RequestException as e:
                print(f"Connection attempt failed: {e}")

        # Fallback to MAC discovery if IP does not work
        # Not implemented

        if not self.meter_ip:
            logger.error("P1 Meter: Not initialized. No host IP provided and MAC discovery failed.")
            self.is_initialized = False

    def refresh_data(self) -> Optional[Dict[str, Any]]:
        """
        Fetches the latest data from the HomeWizard P1 meter API.

        Returns:
            A dictionary containing the P1 meter data if successful, None otherwise.
            The dictionary includes an added 'timestamp_utc_iso' field.
        """
        if not self.is_initialized or not self.data_url:
            logger.error("P1 Meter: Cannot refresh data, client not initialized or URL not set.")
            return None

        response = ''
        try:
            response = requests.get(self.data_url, timeout=self.request_timeout)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            data = response.json()

            data['timestamp_utc_iso'] = datetime.now(timezone.utc).isoformat()

            logger.debug(f"P1 Meter: Successfully fetched data. Active power: {data.get('active_power_w')} W")
            return data
        except requests.exceptions.Timeout:
            logger.warning(f"P1 Meter: Request to {self.data_url} timed out after {self.request_timeout}s.")
            return None
        except requests.exceptions.ConnectionError:
            logger.warning(f"P1 Meter: Connection error when trying to reach {self.data_url}.")
            return None
        except requests.exceptions.HTTPError as e:
            logger.warning(f"P1 Meter: HTTP error from {self.data_url}: {e.response.status_code} {e.response.reason}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"P1 Meter: General request error for {self.data_url}: {e}", exc_info=True)
            return None
        except json.JSONDecodeError as e:
            logger.error(f"P1 Meter: Failed to decode JSON response from {self.data_url}: {e}", exc_info=True)
            logger.debug(f"P1 Meter: Raw response content: {response.text if response else 'N/A'}")
            return None

    def refresh_batteries_data(self) -> Optional[Dict[str, Any]]:
        if not self.is_initialized or not self.battery_url:
            logger.error("P1 Meter: Cannot refresh battery data, client not initialized or URL not set.")
            return None

        response = ''
        try:
            response = requests.get(
                self.battery_url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "X-Api-Version": "2",
                },
                verify=False,
                timeout=self.request_timeout,
            )
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            data = response.json()

            data['timestamp_utc_iso'] = datetime.now(timezone.utc).isoformat()

            logger.debug(f"P1 Meter: Successfully fetched battery data. Active power: {data.get('power_w')} W")
            return data
        except requests.exceptions.Timeout:
            logger.warning(f"P1 Meter: Request to {self.battery_url} timed out after {self.request_timeout}s.")
            return None
        except requests.exceptions.ConnectionError:
            logger.warning(f"P1 Meter: Connection error when trying to reach {self.battery_url}.")
            return None
        except requests.exceptions.HTTPError as e:
            logger.warning(f"P1 Meter: HTTP error from {self.battery_url}: {e.response.status_code} {e.response.reason}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"P1 Meter: General request error for {self.battery_url}: {e}", exc_info=True)
            return None
        except json.JSONDecodeError as e:
            logger.error(f"P1 Meter: Failed to decode JSON response from {self.battery_url}: {e}", exc_info=True)
            logger.debug(f"P1 Meter: Raw response content: {response.text if response else 'N/A'}")
            return None


# Example standalone test (if needed, but better to test via scheduled task)
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test_p1_host = "192.168.0.150"  # Actual IP for testing

    if test_p1_host:
        print(f"--- Testing P1MeterHomeWizard with host: {test_p1_host} ---")
        p1_meter = P1MeterHomewizardClient(host=test_p1_host)

        if p1_meter.is_initialized:
            for _ in range(20):
                test_data = p1_meter.refresh_data()
                if test_data:
                    print(f"Fetched at {test_data['timestamp_utc_iso']}:\n"
                          f"   Active Power: {test_data.get('active_power_w')} W\n"
                          f"   Total Import: {test_data.get('total_power_import_kwh')} kWh\n"
                          f"   L1 Voltage: {test_data.get('active_voltage_l1_v')} V")
                    print(test_data)
                else:
                    print("Failed to fetch P1 data in this attempt.")
                time.sleep(15)
        else:
            print(f"P1 Meter client could not be initialized with host {test_p1_host}. Check IP and network.")
