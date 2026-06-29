import json
import logging
from typing import Any, Dict, Optional

import requests

from hec.core import constants as c
from hec.utils.http_client import HttpClient, build_http_client
from hec.utils.time_utils import utc_now

logger = logging.getLogger(__name__)


class HomeWizardBatteryGateway:
    """Controls the HomeWizard battery group endpoint exposed by the gateway device."""

    def __init__(
            self,
            host: str,
            token: str,
            request_timeout: int = 10,
            http_client: Optional[HttpClient] = None,
            app_config: Optional[dict] = None,
            verify_tls: bool = False,
    ):
        self.host = host
        self.token = token
        self.request_timeout = request_timeout
        self.verify_tls = verify_tls
        self.battery_url = f"https://{self.host}/api/batteries"
        self.http = http_client or build_http_client(
            app_config,
            default_timeout_seconds=request_timeout,
            verify_tls=verify_tls,
        )
        self.is_initialized = False
        self._initialize_connection()

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-Api-Version": "2",
        }

    def _initialize_connection(self) -> None:
        if not self.host or not self.token:
            logger.warning("HomeWizard battery gateway not initialized: host or token missing.")
            return

        try:
            response = self.http.get(
                self.battery_url,
                headers=self._headers(),
                timeout=self.request_timeout,
                verify=self.verify_tls,
            )
            if response.status_code == 200:
                self.is_initialized = True
                logger.info("HomeWizard battery gateway initialized at %s.", self.battery_url)
            else:
                logger.warning(
                    "HomeWizard battery gateway initialization failed with HTTP %s: %s",
                    response.status_code,
                    response.reason,
                )
        except requests.RequestException as e:
            logger.warning("HomeWizard battery gateway connection attempt failed: %s", e)

    def refresh_group_data(self) -> Optional[Dict[str, Any]]:
        if not self.is_initialized:
            logger.warning("HomeWizard battery gateway unavailable. Skipping group refresh.")
            return None

        response = ""
        try:
            response = self.http.get(
                self.battery_url,
                headers=self._headers(),
                timeout=self.request_timeout,
                verify=self.verify_tls,
            )
            response.raise_for_status()
            data = response.json()
            data["timestamp_utc_iso"] = utc_now().isoformat()
            return data
        except requests.exceptions.Timeout:
            logger.warning("HomeWizard battery gateway request to %s timed out.", self.battery_url)
            return None
        except requests.exceptions.ConnectionError:
            logger.warning("HomeWizard battery gateway connection error for %s.", self.battery_url)
            return None
        except requests.exceptions.HTTPError as e:
            logger.warning("HomeWizard battery gateway HTTP error for %s: %s", self.battery_url, e)
            return None
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            logger.error("HomeWizard battery gateway refresh failed: %s", e, exc_info=True)
            logger.debug("HomeWizard battery gateway raw response: %s", response.text if response else "N/A")
            return None

    def set_battery_mode(self, mode: c.BatteryState) -> bool:
        if not self.is_initialized:
            logger.warning("HomeWizard battery gateway unavailable. Skipping battery mode update.")
            return False

        payload = _battery_mode_payload(mode)
        if payload is None:
            logger.error("No HomeWizard battery payload mapping found for state %s", mode)
            return False

        headers = self._headers()
        headers["Content-Type"] = "application/json"

        try:
            logger.info("HomeWizard battery gateway: setting battery mode to %s.", mode.name)
            response = self.http.put(
                self.battery_url,
                headers=headers,
                json=payload,
                timeout=self.request_timeout,
                verify=self.verify_tls,
            )
            response.raise_for_status()
            return True
        except requests.exceptions.Timeout:
            logger.warning("HomeWizard battery gateway timed out while setting mode %s.", mode.name)
            return False
        except requests.exceptions.ConnectionError:
            logger.warning("HomeWizard battery gateway connection error while setting mode %s.", mode.name)
            return False
        except requests.exceptions.HTTPError as e:
            logger.warning("HomeWizard battery gateway HTTP error while setting mode %s: %s", mode.name, e)
            return False
        except requests.exceptions.RequestException as e:
            logger.error("HomeWizard battery gateway command failed: %s", e, exc_info=True)
            return False


def _battery_mode_payload(mode: c.BatteryState) -> Optional[Dict[str, Any]]:
    state_map = {
        c.BatteryState.BATTERY_OFF: {"mode": "standby"},
        c.BatteryState.BATTERY_ON: {"mode": "zero", "permissions": ["charge_allowed", "discharge_allowed"]},
        c.BatteryState.BATTERY_FORCE_CHARGE: {"mode": "to_full"},
        c.BatteryState.BATTERY_BLOCK_CHARGE: {"mode": "zero", "permissions": ["discharge_allowed"]},
        c.BatteryState.BATTERY_BLOCK_DISCHARGE: {"mode": "zero", "permissions": ["charge_allowed"]},
    }
    return state_map.get(mode)
