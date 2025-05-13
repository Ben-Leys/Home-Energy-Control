# constants.py
from enum import Enum, auto


# Controller modes
MODE_AUTO = "auto"
MODE_MANUAL = "manual"

# Inverter control states
INV_CMD_LIMIT_STANDARD = "limit_standard"
INV_CMD_LIMIT_ZERO = "limit_zero"
INV_CMD_LIMIT_FIXED = "limit_fixed_watts"
INV_CMD_LIMIT_TO_USE = "limit_to_home_consumption"

# EVCC control states
EVCC_CMD_STATE_OFF = "off"
EVCC_CMD_STATE_PV = "pv"        # Charge with PV only
EVCC_CMD_STATE_MINPV = "minpv"  # Charge with PV, supplement with grid if PV not enough to start/maintain
EVCC_CMD_STATE_NOW = "now"      # Charge with max power from any source


class AppStatus(Enum):
    NORMAL = auto()     # Everything is running as expected
    STARTING = auto()   # Application is in its startup sequence
    DEGRADED = auto()   # Some non-critical functionality might be impaired
    WARNING = auto()    # A condition that might lead to an error or degraded state if not addressed
    ALARM = auto()      # A significant error has occurred, critical functions might be affected
    SHUTDOWN = auto()   # Application is in the process of shutting down


class InverterStatus(Enum):
    OFFLINE = auto()
    NORMAL = auto()
    LIMITED_PRODUCTION = auto()


class EVChargeStatus(Enum):
    OFFLINE = auto()
    DISCONNECTED = auto()
    CONNECTED_NOT_CHARGING = auto()
    CHARGING = auto()
