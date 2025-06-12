# constants.py
from enum import Enum


class AppStatus(Enum):
    STARTING = "Starting"   # Application is in its startup sequence
    NORMAL = "Normal"       # Everything is running as expected
    WARNING = "Warning"     # A condition that might lead to an error or degraded state if not addressed
    DEGRADED = "Degraded"   # Some non-critical functionality might be impaired
    ALARM = "Alarm"         # A significant error has occurred, critical functions might be affected
    SHUTDOWN = "Shutdown"   # Application is in the process of shutting down


class MediatorGoal(Enum):
    NO_CHARGING = "No charging"
    CHARGE_WITH_MINIMUM_SOLAR_POWER = "Charge when any solar power"
    CHARGE_WITH_ONLY_EXCESS_SOLAR_POWER = "Charge with excess solar power"
    CHARGE_WHEN_SELL_PRICE_NEGATIVE = "Charge when sell price negative"
    CHARGE_WHEN_BUY_PRICE_NEGATIVE = "Charge when buy price negative"
    CHARGE_NOW_WITH_CAPACITY_RATE = "Charge now with capacity rate"
    CHARGE_NOW_NO_CAPACITY_RATE = "Charge now no capacity rate"


class OperatingMode(Enum):
    MODE_AUTO = "Auto"      # System makes decisions
    MODE_MANUAL = "Manual"  # User manually picks state


class InverterStatus(Enum):
    UNKNOWN = "Unknown"
    OFFLINE = "Offline"
    NORMAL = "Normal"
    WARNING = "Warning"
    OFF = "Off"
    FAULT = "Fault"
    STANDBY = "Standby"


class InverterManualState(Enum):
    INV_CMD_LIMIT_STANDARD = "Standard power limit"
    INV_CMD_LIMIT_ZERO = "Power limit zero"
    INV_CMD_LIMIT_MANUAL = "Manual power limit"
    INV_CMD_LIMIT_TO_USE = "Power limited to home consumption"


class EVChargeStatus(Enum):
    UNKNOWN = "Unknown"
    OFFLINE = "Offline"
    DISCONNECTED = "Disconnected"
    CONNECTED_NOT_CHARGING = "Connected not charging"
    CHARGING = "Charging"


class EVCCManualState(Enum):
    EVCC_CMD_STATE_OFF = "off"
    EVCC_CMD_STATE_PV = "pv"        # Charge with PV only
    EVCC_CMD_STATE_MINPV = "minpv"  # Charge with PV, supplement with grid if PV not enough to start/maintain
    EVCC_CMD_STATE_NOW = "now"      # Charge with max power from any source
