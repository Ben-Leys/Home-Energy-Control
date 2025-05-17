# hec/controllers/api_evcc.py
import json
import logging
from datetime import datetime, timezone

import requests
import time
from typing import Optional, Dict, Any, Union

from hec.core import constants as c

logger = logging.getLogger(__name__)


class EvccApiClient:
    def __init__(self, base_api_url: str, default_loadpoint_id: int = 1,
                 max_current: int = 32, request_timeout: int = 10):
        """
        Client for interacting with the EVCC API.

        Args:
            base_api_url (str): The base URL of the EVCC API (e.g., "http://localhost:7070/api").
            default_loadpoint_id (int): The default loadpoint ID to interact with.
            max_current (int): The maximum current the loadpoint accepts (currently only 1 loadpoint).
            request_timeout (int): Timeout in seconds for HTTP requests.
        """
        if not base_api_url.endswith('/'):
            base_api_url += '/'
        self.base_url = base_api_url
        self.state_url = f"{self.base_url}state"
        # Loadpoint URLs are constructed dynamically
        self.default_loadpoint_id = default_loadpoint_id
        self.min_current = 6  # Usually the same for any loadpoint
        self.max_current = max_current
        self.request_timeout = max(request_timeout, 2)
        self.is_available: bool = self._check_availability()  # Check on init

        logger.info(f"EVCC client initialized. URL: {self.base_url}, Default Loadpoint: {self.default_loadpoint_id}")
        if not self.is_available:
            logger.warning("EVCC Client: instance may not be available at the configured URL.")

    def _check_availability(self) -> bool:
        """Performs a quick check to see if the API is responsive."""
        try:
            response = requests.get(self.state_url, timeout=self.request_timeout / 2)
            response.raise_for_status()
            logger.debug("EVCC API availability check: OK")
            return True
        except requests.exceptions.RequestException as e:
            logger.warning(f"EVCC API availability check failed: {e}")
            return False

    def _construct_loadpoint_url(self, loadpoint_id: Optional[int] = None) -> str:
        """Constructs the URL for a specific loadpoint."""
        return f"{self.base_url}loadpoints/{loadpoint_id or self.default_loadpoint_id}"

    def get_current_state(self) -> Optional[Dict[str, Any]]:
        """
        Fetches the full current state from EVCC's /api/state endpoint.

        Returns:
            A dictionary containing the EVCC state if successful, None otherwise.
        """
        if not self.is_available:
            if not self._check_availability():
                logger.warning("EVCC API: Still unavailable, cannot get current state.")
                return None
            self.is_available = True

        response = ''
        try:
            response = requests.get(self.state_url, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()

            # Add a timestamp to the data for AppState consistency
            if isinstance(data.get("result"), dict):
                data["result"]["timestamp_utc_iso"] = datetime.now(timezone.utc).isoformat()
                logger.debug(f"EVCC API: Successfully fetched state. Grid Power: {data['result'].get('gridPower')}W")
                return data["result"]
            else:
                logger.warning(f"EVCC API: 'result' key not found or not a dict in state response: {data}")
                return None
        except requests.exceptions.Timeout:
            logger.warning(f"EVCC API: Request to {self.state_url} timed out.")
            self.is_available = False
            return None
        except requests.exceptions.ConnectionError:
            logger.warning(f"EVCC API: Connection error for {self.state_url}.")
            self.is_available = False
            return None
        except requests.exceptions.HTTPError as e:
            logger.warning(f"EVCC API: HTTP error from {self.state_url}: {e.response.status_code} {e.response.reason}")
            self.is_available = False
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"EVCC API: General request error for {self.state_url}: {e}", exc_info=True)
            self.is_available = False
            return None
        except json.JSONDecodeError as e:
            logger.error(f"EVCC API: Failed to decode JSON response from {self.state_url}: {e}", exc_info=True)
            logger.debug(f"EVCC API: Raw response: {response.text if response else 'N/A'}")
            self.is_available = False
            return None

    def _send_command(self, endpoint_suffix: str, method: str = "POST", data: Optional[Any] = None,
                      loadpoint_id: Optional[int] = None) -> bool:
        """
        Helper function to send a command to a loadpoint endpoint.
        `endpoint_suffix` is like "/mode/pv" or "/maxcurrent/16".
        """
        if not self.is_available:
            logger.warning(f"EVCC API: Unavailable, cannot send command '{endpoint_suffix}'.")
            return False

        try:
            response = requests.request(
                method=method.upper(),
                url=f"{self._construct_loadpoint_url(loadpoint_id)}{endpoint_suffix}",
                json=data,
                timeout=self.request_timeout
            )
            response.raise_for_status()
            logger.debug(f"EVCC API: Command '{endpoint_suffix}' successful. Response: {response.text[:200]}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"EVCC API: Error sending command '{endpoint_suffix}': {e}", exc_info=True)
            if isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
                self.is_available = False
            return False

    def set_charge_mode(self, mode: str, loadpoint_id: Optional[int] = None) -> bool:
        """Sets the charging mode (e.g., 'off', 'pv', 'minpv', 'now')."""
        valid_modes = [c.EVCCManualState.EVCC_CMD_STATE_OFF.value, c.EVCCManualState.EVCC_CMD_STATE_PV.value,
                       c.EVCCManualState.EVCC_CMD_STATE_MINPV.value, c.EVCCManualState.EVCC_CMD_STATE_NOW.value]
        if mode not in valid_modes:
            logger.warning(f"EVCC API: Invalid charge mode '{mode}' requested.")
            return False
        return self._send_command(f"/mode/{mode}", loadpoint_id=loadpoint_id)

    def set_target_soc(self, soc_percent: int, loadpoint_id: Optional[int] = None) -> bool:
        """Sets the target State of Charge."""
        if not (0 <= soc_percent <= 100):
            logger.warning(f"EVCC API: Invalid target SOC {soc_percent}%. Must be 0-100.")
            return False
        return self._send_command(f"/targetsoc/{soc_percent}", loadpoint_id=loadpoint_id)

    def set_min_soc(self, soc_percent: int, loadpoint_id: Optional[int] = None) -> bool:
        """Sets the minimum State of Charge."""
        if not (0 <= soc_percent <= 100):
            logger.warning(f"EVCC API: Invalid min SOC {soc_percent}%. Must be 0-100.")
            return False
        return self._send_command(f"/minsoc/{soc_percent}", loadpoint_id=loadpoint_id)

    def set_max_current(self, current_amps: int, loadpoint_id: Optional[int] = None) -> bool:
        """Sets the maximum charging current in Amps."""
        if not (0 <= current_amps <= self.max_current):
            logger.warning(f"EVCC API: Invalid max current {current_amps}. Must be 0-{self.max_current}.")
            return False
        return self._send_command(f"/maxcurrent/{current_amps}", loadpoint_id=loadpoint_id)

    def set_min_current(self, current_amps: int, loadpoint_id: Optional[int] = None) -> bool:
        """Sets the minimum charging current in Amps."""
        if not (self.min_current <= current_amps <= self.max_current):
            logger.warning(f"EVCC API: Invalid min current {current_amps}. "
                           f"Must be {self.min_current}-{self.max_current}.")
        return self._send_command(f"/mincurrent/{current_amps}", loadpoint_id=loadpoint_id)

    def set_smart_cost_limit(self, cost_limit_eur_kwh: float, loadpoint_id: Optional[int] = None) -> bool:
        """Sets the smart cost limit (price per kWh). 0 to disable."""
        # EVCC expects this as a number
        return self._send_command(f"/smartcostlimit/{cost_limit_eur_kwh}", loadpoint_id=loadpoint_id)

    def sequence_force_pv_charging(self, loadpoint_id: Optional[int] = None) -> bool:
        """Attempts a sequence to start PV charging immediately instead of waiting for min_time."""
        logger.info("EVCC API: Initiating sequence for PV charging (minpv -> pv).")
        if not self.set_charge_mode(c.EVCCManualState.EVCC_CMD_STATE_MINPV.name, loadpoint_id):
            return False
        time.sleep(1)  # Short delay for EVCC to process mode change
        return self.set_charge_mode(c.EVCCManualState.EVCC_CMD_STATE_PV.name, loadpoint_id)


# Example Usage (for testing this module directly)
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    TEST_EVCC_URL = "http://192.168.0.247:7070/api"

    print(f"--- Testing EvccApiClient with base URL: {TEST_EVCC_URL} ---")
    evcc = EvccApiClient(base_api_url=TEST_EVCC_URL)

    if evcc.is_available:
        print("\n--- Getting Current State ---")
        state = evcc.get_current_state()
        if state:
            print(f"EVCC Grid Power: {state.get('gridPower')} W")
            if state.get('loadpoints') and len(state['loadpoints']) > 0:
                lp0 = state['loadpoints'][0]
                print(f"Loadpoint 1 Mode: {lp0.get('mode')}")
                print(f"Loadpoint 1 Charging: {lp0.get('charging')}")
                print(f"Loadpoint 1 SoC: {lp0.get('vehicleSoc')} %")
            print("Full state:", state)
        else:
            print("Failed to get current EVCC state.")

        print("\n--- Testing Commands ---")
        print("Setting mode to 'pv'...")
        if evcc.set_charge_mode(c.EVCCManualState.EVCC_CMD_STATE_PV.value):
            time.sleep(3)
            print("Setting mode back to 'off'...")
            evcc.set_charge_mode(c.EVCCManualState.EVCC_CMD_STATE_OFF.value)
        else:
            print("Failed to set mode to 'pv'.")

        print("\nSetting max current to 10A...")
        if evcc.set_max_current(10):
            time.sleep(3)
            print("Setting max current back to 30A...")
            evcc.set_max_current(30)
        else:
            print("Failed to set max current to 10A.")
    else:
        print(f"EVCC API client could not connect or EVCC is not available at {TEST_EVCC_URL}.")
