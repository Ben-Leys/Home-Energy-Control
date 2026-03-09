import logging
from collections import deque
from datetime import datetime
from enum import Enum
from typing import Any

from flask import Flask, jsonify, request

from hec.core import constants as c
from hec.core.app_state import GLOBAL_APP_STATE

api_app = Flask(__name__)
logger = logging.getLogger(__name__)


@api_app.route('/api/v1/state', methods=['GET'])
def get_app_state_api():
    """API endpoint to get the current application state."""
    # GLOBAL_APP_STATE.get_all() returns a JSON-serializable dict
    # datetime objects need to be converted to ISO strings, Enums to their values/names
    current_raw_state = GLOBAL_APP_STATE.get_all()

    # Make a copy to modify for serialization
    serializable_state = {}
    for key, value in current_raw_state.items():
        if isinstance(value, datetime):
            serializable_state[key] = value.isoformat()
        elif isinstance(value, c.AppStatus) or \
                isinstance(value, c.MediatorGoal) or \
                isinstance(value, c.OperatingMode) or \
                isinstance(value, c.InverterStatus) or \
                isinstance(value, c.InverterManualState) or \
                isinstance(value, c.EVChargeStatus) or \
                isinstance(value, c.EVCCManualState) or \
                isinstance(value, c.BatteryState):
            serializable_state[key] = value.value
        elif isinstance(value, list):
            new_list = []
            for item in value:
                if isinstance(item, dict):
                    new_dict_item = {}
                    for k_item, v_item in item.items():
                        if isinstance(v_item, datetime):
                            new_dict_item[k_item] = v_item.isoformat()
                        elif isinstance(v_item, Enum):
                            new_dict_item[k_item] = v_item.name
                        else:
                            new_dict_item[k_item] = v_item
                    new_list.append(new_dict_item)
                else:
                    new_list.append(str(item))
            serializable_state[key] = new_list
        elif isinstance(value, deque):
            serializable_state[key] = list(value)
        elif isinstance(value, dict):
            new_dict = {}
            for k_nested, v_nested in value.items():
                if isinstance(v_nested, Enum):
                    new_dict[k_nested] = v_nested.name
                else:
                    new_dict[k_nested] = v_nested
            serializable_state[key] = new_dict
        else:
            serializable_state[key] = value

    return jsonify(serializable_state)


@api_app.route('/api/v1/settings/update', methods=['POST'])
def update_app_setting_api():
    """
    Generic API endpoint to update a specific setting in AppState.
    Expects JSON body: {"key": "app_state_key_name", "value": "new_value"}
    """
    try:
        data = request.json
        if not data or 'key' not in data or 'value' not in data:  # 'value' can be None
            logger.warning("API /settings/update: Missing 'key' or 'value' in request JSON.")
            return jsonify({"error": "Missing 'key' or 'value' in request body"}), 400

        key_to_update = data['key']
        raw_value_from_ui = data['value']  # This value might be a string from UI toggles/inputs

        logger.info(f"API /settings/update: Received request to update '{key_to_update}' to '{raw_value_from_ui}'")

        # --- Type Conversion and Validation ---
        final_value_to_set: Any = None
        conversion_successful = True

        # Check if the key is even valid in AppState
        if key_to_update not in GLOBAL_APP_STATE.current_values:
            logger.warning(f"API /settings/update: Attempt to update unknown AppState key '{key_to_update}'.")
            return jsonify({"error": f"Unknown setting key: {key_to_update}"}), 400

        # Get the expected type from the default value in AppState if possible
        default_value = GLOBAL_APP_STATE.current_values[key_to_update]  # Initial value as type hint

        if key_to_update == "app_operating_mode":
            try:
                if raw_value_from_ui is None:
                    final_value_to_set = None
                else:
                    final_value_to_set = c.OperatingMode[raw_value_from_ui]
            except KeyError:
                conversion_successful = False
                logger.warning(
                    f"API /settings/update: Invalid value '{raw_value_from_ui}' for Application OperatingMode.")

        elif key_to_update == "app_mediator_goal":
            try:
                if raw_value_from_ui is None:
                    final_value_to_set = None
                else:
                    final_value_to_set = c.MediatorGoal[raw_value_from_ui]
            except KeyError:
                conversion_successful = False
                logger.warning(f"API /settings/update: Invalid value '{raw_value_from_ui}' for MediatorGoal.")

        elif key_to_update == "inverter_manual_state":
            try:
                if raw_value_from_ui is None:
                    final_value_to_set = None
                else:
                    final_value_to_set = c.InverterManualState[raw_value_from_ui]
            except KeyError:
                conversion_successful = False
                logger.warning(
                    f"API /settings/update: Invalid value '{raw_value_from_ui}' for inverter_manual_state.")

        elif key_to_update == "evcc_manual_state":
            try:
                if raw_value_from_ui is None:
                    final_value_to_set = None
                else:
                    final_value_to_set = c.EVCCManualState[raw_value_from_ui]
            except KeyError:
                conversion_successful = False
                logger.warning(
                    f"API /settings/update: Invalid value '{raw_value_from_ui}' for evcc_manual_state.")

        elif key_to_update == "battery_manual_mode":
            try:
                if raw_value_from_ui is None:
                    final_value_to_set = None
                else:
                    final_value_to_set = c.BatteryState[raw_value_from_ui]
            except KeyError:
                conversion_successful = False
                logger.warning(
                    f"API /settings/update: Invalid value '{raw_value_from_ui}' for battery_manual_mode.")

        elif key_to_update in ["inverter_manual_limit", "evcc_manual_limit_amps"]:
            if raw_value_from_ui is None:  # Allow None
                final_value_to_set = None
            else:
                try:
                    final_value_to_set = int(raw_value_from_ui)
                    # Add range validation
                    if key_to_update == "inverter_manual_limit" and not (0 <= final_value_to_set <= 7000):
                        raise ValueError(f"Inverter limit out of range 0-7000")
                    if key_to_update == "evcc_manual_limit_amps" and not (6 <= final_value_to_set <= 32):
                        raise ValueError("Amps out of range 0-32")
                except (ValueError, TypeError):
                    conversion_successful = False
                    logger.warning(
                        f"API /settings/update: Invalid integer value '{raw_value_from_ui}' for '{key_to_update}'.")

        # Add more specific type conversions when developed
        # Default if no specific conversion matched
        elif isinstance(default_value, str) and isinstance(raw_value_from_ui, str):
            final_value_to_set = raw_value_from_ui
        elif isinstance(default_value, bool) and isinstance(raw_value_from_ui, bool):
            final_value_to_set = raw_value_from_ui
        else:  # Fallback or unknown key type for conversion
            if key_to_update in GLOBAL_APP_STATE.persisted_keys:
                logger.warning(f"API /settings/update: No specific type conversion defined for persisted key "
                               f"'{key_to_update}' with value '{raw_value_from_ui}'. Assuming string or direct type "
                               f"if matches default.")
                if isinstance(raw_value_from_ui, type(default_value)) or default_value is None:
                    final_value_to_set = raw_value_from_ui
                else:
                    conversion_successful = False  # Reject if types don't match and no rule
            else:
                final_value_to_set = raw_value_from_ui

        if not conversion_successful:
            return jsonify(
                {"error": f"Invalid value or type for setting '{key_to_update}': '{raw_value_from_ui}'"}), 400

        # --- Update AppState (which will also save to DB if key is in persisted_keys) ---
        GLOBAL_APP_STATE.set(key_to_update, final_value_to_set)

        # Read back
        confirmed_value = GLOBAL_APP_STATE.get(key_to_update)
        if isinstance(confirmed_value, Enum):
            confirmed_value_for_json = confirmed_value.name
        else:
            confirmed_value_for_json = confirmed_value

        logger.info(
            f"API /settings/update: Setting '{key_to_update}' successfully updated to '{final_value_to_set}' "
            f"(confirmed: {confirmed_value_for_json}).")
        return jsonify({
            "success": True,
            "key": key_to_update,
            "new_value_stored": confirmed_value_for_json
        })

    except Exception as e:
        logger.error(f"API /settings/update: Error processing request: {e}", exc_info=True)
        return jsonify({"error": "Internal server error processing update"}), 500


def run_api_server(app_config: dict):
    """Runs the Flask API server in a separate thread."""
    api_config = app_config.get('api_server', {})
    host = api_config.get('host', '0.0.0.0')
    port = api_config.get('port', 8123)
    debug_mode = api_config.get('debug', False)

    logger.info(f"Starting API server on http://{host}:{port}")
    try:
        api_app.run(host=host, port=port, debug=debug_mode, use_reloader=False)
    except Exception as e:
        logger.error(f"API server failed to start or crashed: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.DEGRADED)
