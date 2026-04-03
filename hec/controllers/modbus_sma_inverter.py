# hec/data_sources/inverter_sma_modbus.py
import logging
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

import pytz
# Pymodbus v3.x imports
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusIOException, ConnectionException

from hec.core import constants as c

logger = logging.getLogger(__name__)

# SMA Modbus Registers
SMA_REG_CURRENT_PV_POWER = 30775  # INT32, Watts, Total current power generated
SMA_REG_DAILY_ENERGY_YIELD = 30517  # UINT64, Wh, Energy yield for the current day
SMA_REG_TOTAL_ENERGY_YIELD = 30513  # UINT64, Wh, Total lifetime energy yield
SMA_REG_POWER_LIMIT_SETPOINT = 40212  # INT32, Watts, Current active power limit
SMA_REG_GRID_GUARD_STATUS = 43090  # UINT32, 1=logged in (Installer), 2=logged in (User), 0=not logged in
SMA_REG_GRID_GUARD_LOGIN = 43090  # Same register for writing login code
SMA_REG_DEVICE_STATUS = 30201  # UINT32, Overall device status code
SMA_DEVICE_STATUS_MAP = {
    35: c.InverterStatus.FAULT,  # Fault
    303: c.InverterStatus.OFF,  # Off
    307: c.InverterStatus.NORMAL,  # Normal
    455: c.InverterStatus.WARNING,  # Warning
}


class InverterSmaModbusClient:
    def __init__(self, host: str, port: int = 502, modbus_unit_id: int = 3, grid_guard_code: Optional[int] = None,
                 standard_power_limit: int = 7000, timeout_sec: int = 5):
        self.host = host
        self.port = port
        self.unit_id = modbus_unit_id
        self.grid_guard_code = grid_guard_code
        self.standard_power_limit = standard_power_limit  # Watts
        self.timeout = timeout_sec
        self.client: Optional[ModbusTcpClient] = None
        self.is_grid_guard_logged_in: bool = False  # Track login state
        self.last_grid_guard_login: Optional[datetime] = None  # Track last login time
        self.power_limit_timestamps = deque(maxlen=4)  # Track power limit change timestamps

        self._connect()  # Attempt initial connection

    def _connect(self) -> bool:
        """Establishes or re-establishes the Modbus TCP connection."""
        if self.client and self.client.is_socket_open():
            return True
        try:
            logger.debug(f"InverterSMA: Attempting to connect to {self.host}:{self.port} (Unit ID: {self.unit_id})")
            self.client = ModbusTcpClient(self.host, port=self.port, timeout=self.timeout)
            if self.client.connect():
                logger.info(f"InverterSMA: Successfully connected to {self.host}:{self.port}")
                return True
            else:
                logger.warning(
                    f"InverterSMA: Failed to connect to {self.host}:{self.port} (client.connect() returned False)")
                self.client = None  # Clear client on failure
                return False
        except ConnectionException as e:  # Pymodbus specific connection exception
            logger.warning(f"InverterSMA: ConnectionException to {self.host}:{self.port} - {e}")
            self.client = None
            return False
        except Exception as e:
            logger.error(f"InverterSMA: Unexpected error connecting to {self.host}:{self.port} - {e}", exc_info=True)
            self.client = None
            return False

    def disconnect(self):
        if self.client:
            self.client.close()
            self.client = None
            logger.info("InverterSMA: Disconnected.")
            self.is_grid_guard_logged_in = False

    def _read_registers(self, address: int, count: int) -> Optional[list[int]]:
        """Helper to read holding registers with connection check and error handling."""
        if not self._connect():  # Ensures connection is active or tries to reconnect
            return None
        try:
            # Ensure client is not None after _connect attempt
            if not self.client:
                return None

            rr = self.client.read_holding_registers(address=address, count=count, slave=self.unit_id)
            if rr.isError():
                logger.warning(f"InverterSMA: Modbus Error reading {count} register(s) at {address}: {rr}")
                if isinstance(rr, ModbusIOException):  # Indicates connection lost
                    self.disconnect()  # Force disconnect to trigger reconnect on next call
                return None
            return rr.registers
        except ModbusIOException as e:
            logger.warning(f"InverterSMA: ModbusIOException during read at {address}: {e}. Disconnecting.")
            self.disconnect()  # Ensure client is closed to attempt a reconnect next time
            return None
        except Exception as e:
            logger.error(f"InverterSMA: Unexpected error reading register {address}: {e}", exc_info=True)
            return None

    def get_operational_status(self) -> c.InverterStatus:
        """Reads the device status register and maps it to an InverterStatus Enum."""
        regs = self._read_registers(SMA_REG_DEVICE_STATUS, 2)
        if not regs or not self.client:
            return c.InverterStatus.OFFLINE

        raw_status_val = ""
        try:
            raw_status_val = self.client.convert_from_registers(regs, data_type=self.client.DATATYPE.UINT32)

            status_enum = SMA_DEVICE_STATUS_MAP.get(raw_status_val, c.InverterStatus.UNKNOWN)
            logger.debug(f"InverterSMA: Raw device status code {raw_status_val} -> Mapped to {status_enum.name}")

            return status_enum
        except Exception as e:
            logger.error(f"InverterSMA: Error converting device status registers {regs} raw value {raw_status_val}: {e}")
            return c.InverterStatus.UNKNOWN

    def get_live_data(self) -> Optional[Dict[str, Any]]:
        """Fetches key live data points from the inverter."""
        status = self.get_operational_status()  # This tries to connect
        if status == c.InverterStatus.OFFLINE and not (self.client and self.client.is_socket_open()):
            logger.warning("InverterSMA: Offline, cannot fetch live data.")
            return None

        def read_and_convert(reg_addr, reg_count, data_type):
            if not self.client:
                return None
            raw_regs = self._read_registers(reg_addr, reg_count)
            if raw_regs:
                try:
                    return self.client.convert_from_registers(raw_regs, data_type=data_type)
                except Exception as e:
                    logger.error(f"InverterSMA: Error converting registers for addr {reg_addr}: {e}")
            return None

        pv_power_w = read_and_convert(SMA_REG_CURRENT_PV_POWER, 2, self.client.DATATYPE.INT32)
        daily_yield_wh = read_and_convert(SMA_REG_DAILY_ENERGY_YIELD, 4, self.client.DATATYPE.UINT64)
        total_yield_wh = read_and_convert(SMA_REG_TOTAL_ENERGY_YIELD, 4, self.client.DATATYPE.UINT64)
        current_limit_w = self.get_current_power_limit_setpoint()

        # Check if read failed
        if pv_power_w is None:
            logger.warning("InverterSMA: Failed to read PV power.")
            return None

        return {
            "timestamp_utc_iso": datetime.now(timezone.utc).isoformat(),
            "operational_status": status.name,
            "pv_power_watts": pv_power_w,
            "daily_yield_wh": daily_yield_wh,
            "total_yield_wh": total_yield_wh,
            "active_power_limit_watts": current_limit_w,
        }

    def get_current_power_limit_setpoint(self) -> Optional[int]:
        """Gets the currently active power limit setpoint."""
        if not self.client:
            return None
        regs = self._read_registers(SMA_REG_POWER_LIMIT_SETPOINT, 2)
        if regs:
            try:
                return self.client.convert_from_registers(regs, data_type=self.client.DATATYPE.INT32)
            except Exception as e:
                logger.error(f"InverterSMA: Error converting power limit registers: {e}")
        return None

    def _get_grid_guard_login_status_code(self) -> Optional[int]:
        """Reads the raw Grid Guard status code."""
        if not self.client:
            return None
        regs = self._read_registers(SMA_REG_GRID_GUARD_STATUS, 2)
        if regs:
            try:
                return self.client.convert_from_registers(regs, data_type=self.client.DATATYPE.UINT32)
            except Exception as e:
                logger.error(f"InverterSMA: Error converting Grid Guard status registers: {e}")
        return None

    def _logout_grid_guard(self):
        """Performs a logout action on the Grid Guard system."""
        logger.debug("InverterSMA: Performing Grid Guard logout.")
        try:
            payload_regs = self.client.convert_to_registers(0, data_type=self.client.DATATYPE.UINT32)
            logout_result = self.client.write_registers(SMA_REG_GRID_GUARD_LOGIN, payload_regs, slave=self.unit_id)
            time.sleep(1)
            if logout_result.isError():
                logger.warning(f"InverterSMA: Modbus Error during Grid Guard logout: {logout_result}")
                if isinstance(logout_result, ModbusIOException):
                    self.disconnect()
        except ModbusIOException as e:
            logger.error(f"InverterSMA: ModbusIOException during Grid Guard logout: {e}")
            self.disconnect()
        except Exception as e:
            logger.error(f"InverterSMA: Unexpected error during Grid Guard logout: {e}", exc_info=True)

    def check_and_perform_grid_guard_login_if_needed(self, force_login: bool = False) -> bool:
        """
        Checks Grid Guard login status and attempts to log in if not already, or if force_login is True.
        Logs out and logs in again if the last login was more than an hour ago.
        Required before writing to protected registers (like power limit).
        Returns True if logged in (or login successful), False otherwise.
        """
        if not self.grid_guard_code:
            logger.warning("InverterSMA: Grid Guard code not configured. Cannot perform login.")
            return False

        if not self._connect() or not self.client:
            return False

        now = datetime.now()
        if not self.last_grid_guard_login or now - self.last_grid_guard_login > timedelta(hours=3):
            logger.info("InverterSMA: Last Grid Guard login expired. Logging out.")
            try:
                self._logout_grid_guard()
            except Exception as e:
                logger.error(f"InverterSMA: Error during Grid Guard logout: {e}", exc_info=True)
            self.is_grid_guard_logged_in = False  # Force re-login

        if not force_login and self.is_grid_guard_logged_in:
            logger.debug("InverterSMA: Grid Guard already marked as logged in.")
            return True

        # Attempt login up to n times
        max_login_attempts = 3
        for attempt in range(max_login_attempts):
            status_code = self._get_grid_guard_login_status_code()
            logger.debug(f"InverterSMA: Grid Guard status code: {status_code} (Attempt {attempt + 1})")

            if status_code == 1:  # Installer level login
                logger.info("InverterSMA: Grid Guard login successful.")
                self.is_grid_guard_logged_in = True
                self.last_grid_guard_login = now
                return True
            elif status_code == 2:  # User level login
                logger.info("InverterSMA: Grid Guard logged in at 'User' level. Insufficient for power limit.")
            else:  # Not logged in (0) or unknown status
                logger.info("InverterSMA: Grid Guard not logged in. Attempting login...")

            self.is_grid_guard_logged_in = False
            try:
                payload_regs = self.client.convert_to_registers(
                    self.grid_guard_code,
                    data_type=self.client.DATATYPE.UINT32
                )

                wr = self.client.write_registers(SMA_REG_GRID_GUARD_LOGIN, payload_regs, slave=self.unit_id)
                if wr.isError():
                    logger.warning(f"InverterSMA: Modbus Error writing Grid Guard code: {wr}")
                    if isinstance(wr, ModbusIOException):
                        self.disconnect()
                else:
                    logger.info("InverterSMA: Grid Guard code sent. Re-checking status shortly...")
                    time.sleep(2)
                    continue
            except ModbusIOException as e:
                logger.warning(f"InverterSMA: ModbusIOException during Grid Guard login write: {e}. Disconnecting.")
                self.disconnect()
                return False
            except Exception as e:
                logger.error(f"InverterSMA: Unexpected error writing Grid Guard login: {e}", exc_info=True)
                return False

            if attempt < max_login_attempts - 1:
                time.sleep(2)  # Short delay before retrying

        logger.error(f"InverterSMA: Failed to log in to Grid Guard after {max_login_attempts} attempts.")
        self.is_grid_guard_logged_in = False
        return False

    def set_active_power_limit(self, limit_watts: int) -> bool:
        """
        Sets the active power limit on the inverter.
        Ensures Grid Guard is logged in before attempting to write.

        Args:
            limit_watts (int): The power limit to set, in Watts.

        Returns:
            bool: True if limit was set successfully, False otherwise.
        """
        if not (0 <= limit_watts <= self.standard_power_limit):
            logger.error(f"InverterSMA: Invalid power limit {limit_watts} W.")
            return False

        # Enforce rate limiting
        now = datetime.now(tz=pytz.UTC)
        if len(self.power_limit_timestamps) >= 4 and now - self.power_limit_timestamps[0] < timedelta(minutes=2):
            logger.error("InverterSMA: Rate limit exceeded for setting power limit. Try again later.")
            return False

        if not self.check_and_perform_grid_guard_login_if_needed():
            logger.error("InverterSMA: Cannot set power limit, Grid Guard login failed.")
            return False

        if not self.client:
            logger.error("InverterSMA: Client not connected after Grid Guard check. Cannot set power limit.")
            return False

        logger.info(f"InverterSMA: Attempting to set power limit to {limit_watts} W.")

        try:
            payload_regs = self.client.convert_to_registers(
                int(limit_watts),
                data_type=self.client.DATATYPE.INT32
            )

            wr = self.client.write_registers(SMA_REG_POWER_LIMIT_SETPOINT, payload_regs, slave=self.unit_id)
            if wr.isError():
                logger.warning(f"InverterSMA: Modbus Error writing power limit ({limit_watts} W): {wr}")
                if isinstance(wr, ModbusIOException):
                    self.disconnect()
                self.is_grid_guard_logged_in = False
                return False

            logger.info(f"InverterSMA: Successfully sent command to set power limit to {limit_watts}W.")
            self.power_limit_timestamps.append(now)
            return True
        except ModbusIOException as e:
            logger.warning(f"InverterSMA: ModbusIOException during power limit write: {e}. Disconnecting.")
            self.disconnect()
            return False
        except Exception as e:
            logger.error(f"InverterSMA: Unexpected error setting power limit: {e}", exc_info=True)
            return False


# Standalone Test
# if __name__ == '__main__':
#     logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#
#     TEST_INV_HOST = "192.168.0.141"
#     TEST_INV_PORT = 502
#     TEST_INV_UNIT_ID = 3
#     TEST_GRID_GUARD_CODE = 1285929600
#     TEST_STANDARD_POWER_LIMIT = 7000
#
#     print(f"--- Testing InverterSmaModbusClient with host: {TEST_INV_HOST} ---")
#     inv_client = InverterSmaModbusClient(
#         host=TEST_INV_HOST,
#         port=TEST_INV_PORT,
#         modbus_unit_id=TEST_INV_UNIT_ID,
#         grid_guard_code=TEST_GRID_GUARD_CODE
#     )
#
#     if inv_client.client and inv_client.client.is_socket_open():
#         print("\n--- Reading Live Data ---")
#         live_data = inv_client.get_live_data()
#         if live_data:
#             print(f"Live Data: {live_data}")
#         else:
#             print("Failed to get live data.")
#
#         print("\n--- Testing Power Limit (will attempt Grid Guard Login) ---")
#         test_limit_value = 6900  # Watts
#         print(f"Attempting to set limit to: {test_limit_value}W")
#         success_set = inv_client.set_active_power_limit(test_limit_value)
#         if success_set:
#             time.sleep(3)
#             print(f"Set limit command sent. Current limit read: {inv_client.get_current_power_limit_setpoint()}W")
#
#             time.sleep(10)
#             print(f"\nAttempting to set limit to: {TEST_STANDARD_POWER_LIMIT}W (remove limit)")
#             success_remove = inv_client.set_active_power_limit(TEST_STANDARD_POWER_LIMIT)
#             if success_remove:
#                 print(f"Remove limit command sent. Current limit: {inv_client.get_current_power_limit_setpoint()}W")
#             else:
#                 print(f"Failed to set limit to {TEST_STANDARD_POWER_LIMIT}W.")
#         else:
#             print(f"Failed to set limit to {test_limit_value}W.")
#
#         inv_client.disconnect()
#     else:
#         print(f"Failed to connect to inverter at {TEST_INV_HOST} during initialization.")
