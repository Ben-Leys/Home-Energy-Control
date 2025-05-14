# constants.py
from enum import Enum, auto


class AppStatus(Enum):
    STARTING = "starting"   # Application is in its startup sequence
    NORMAL = "normal"       # Everything is running as expected
    WARNING = "warning"     # A condition that might lead to an error or degraded state if not addressed
    DEGRADED = "degraded"   # Some non-critical functionality might be impaired
    ALARM = "alarm"         # A significant error has occurred, critical functions might be affected
    SHUTDOWN = "shutdown"   # Application is in the process of shutting down


class OperatingMode(Enum):
    MODE_AUTO = "auto"      # System makes decisions
    MODE_MANUAL = "manual"  # User manually picks state


class AppManualState(Enum):
    NO_CHARGING = "no charging"
    CHARGE_WHEN_ANY_SOLAR_POWER = "charge when any solar power"
    CHARGE_WITH_ONLY_EXCESS_SOLAR_POWER = "charge with excess solar power"
    CHARGE_WHEN_SELL_PRICE_NEGATIVE = "charge when sell price negative"
    CHARGE_WHEN_BUY_PRICE_NEGATIVE = "charge when buy price negative"
    CHARGE_NOW_WITH_CAPACITY_RATE = "charge now with capacity rate"
    CHARGE_NOW_NO_CAPACITY_RATE = "charge now no capacity rate"


class InverterStatus(Enum):
    UNKNOWN = "unknown"
    OFFLINE = "offline"
    NORMAL = "normal"
    LIMITED_PRODUCTION = "limited production"


class InverterManualState(Enum):
    INV_CMD_LIMIT_STANDARD = "limit standard"
    INV_CMD_LIMIT_ZERO = "limit zero"
    INV_CMD_LIMIT_FIXED = "limit fixed watts"
    INV_CMD_LIMIT_TO_USE = "limit to home consumption"


class EVChargeStatus(Enum):
    UNKNOWN = "unknown"
    OFFLINE = "offline"
    DISCONNECTED = "disconnected"
    CONNECTED_NOT_CHARGING = "connected not charging"
    CHARGING = "charging"


class EVCCManualState(Enum):
    EVCC_CMD_STATE_OFF = "off"
    EVCC_CMD_STATE_PV = "pv"        # Charge with PV only
    EVCC_CMD_STATE_MINPV = "minpv"  # Charge with PV, supplement with grid if PV not enough to start/maintain
    EVCC_CMD_STATE_NOW = "now"      # Charge with max power from any source
