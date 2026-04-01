import logging
import os
from collections import deque
from datetime import datetime
from enum import Enum
from typing import Optional

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, send_from_directory

from hec.core import constants as c
from hec.core.app_state import GLOBAL_APP_STATE
from hec.database_ops import db_handler

api_app = Flask(__name__)
logger = logging.getLogger(__name__)

_DB_INSTANCE: Optional[db_handler] = None


@api_app.route('/api/v1/state', methods=['GET'])
def get_app_state_api():
    """API endpoint to get the current application state."""
    # GLOBAL_APP_STATE.get_all() returns a JSON-serializable dict
    # datetime objects need to be converted to ISO strings, Enums to their names
    current_raw_state = GLOBAL_APP_STATE.get_all()

    # Make a copy to modify for serialization
    clean_state, serializable_state = {}, {}
    for key, value in current_raw_state.items():
        if isinstance(value, datetime):
            serializable_state[key] = value.isoformat()
        elif isinstance(value, c.AppStatus) or \
                isinstance(value, c.MediatorGoal) or \
                isinstance(value, c.OperatingMode) or \
                isinstance(value, c.InverterStatus) or \
                isinstance(value, c.InverterManualState) or \
                isinstance(value, c.EVCCManualState) or \
                isinstance(value, c.BatteryState):
            serializable_state[key] = value.name
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

        clean_state = clean_nas(serializable_state)

    return jsonify(clean_state)


@api_app.route("/api/v1/logs", methods=['GET'])
def get_logs():
    if _DB_INSTANCE is None:
        return jsonify({"error": "Database not initialized in API"}), 500

    try:
        limit = request.args.get('limit', default=1000, type=int)
        limit = min(limit, 20000)
    except ValueError:
        limit = 1000

    logs = _DB_INSTANCE.get_latest_logs(limit)
    return jsonify({"logs": logs})


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

        key = data['key']
        raw_val = data['value']

        logger.info(f"API /settings/update: Received request to update '{key}' to '{raw_val}'")

        # Check if the key is even valid in AppState
        if key not in GLOBAL_APP_STATE.current_values:
            logger.warning(f"API /settings/update: Attempt to update unknown AppState key '{key}'.")
            return jsonify({"error": f"Unknown setting key: {key}"}), 400

        # Key map to Enum
        TYPE_MAP = {
            "app_operating_mode": c.OperatingMode,
            "app_mediator_goal": c.MediatorGoal,
            "inverter_manual_state": c.InverterManualState,
            "evcc_manual_state": c.EVCCManualState,
            "battery_manual_mode": c.BatteryState,
            "inverter_manual_limit": int,
            "evcc_manual_limit_amps": int,
        }

        final_value = raw_val

        if raw_val is not None:
            target_type = TYPE_MAP.get(key)

            try:
                if target_type is None:
                    default_val = GLOBAL_APP_STATE.current_values[key]
                    if isinstance(default_val, bool) and not isinstance(raw_val, bool):
                        final_value = str(raw_val).lower() in ['true', '1', 'yes']
                    else:
                        final_value = raw_val

                elif issubclass(target_type, Enum):
                    # Enum via Name (eg. 'BATTERY_ON')
                    final_value = target_type[raw_val]

                elif target_type == int:
                    final_value = int(raw_val)
                    # Range validation
                    if key == "inverter_manual_limit" and not (0 <= final_value <= 7000):
                        return jsonify({"error": "Inverter limit 0-7000"}), 400
                    if key == "evcc_manual_limit_amps" and not (6 <= final_value <= 32):
                        return jsonify({"error": "EVCC amps 6-32"}), 400

            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Conversion failed for {key} with value {raw_val}: {e}")
                return jsonify({"error": f"Invalid value '{raw_val}' for {key}"}), 400

        # Update and confirm
        GLOBAL_APP_STATE.set(key, final_value)
        confirmed = GLOBAL_APP_STATE.get(key)

        json_val = confirmed.name if isinstance(confirmed, Enum) else confirmed

        logger.info(f"API Update: {key} -> {json_val}")
        return jsonify({"success": True, "key": key, "new_value_stored": json_val})

    except Exception as e:
        logger.error(f"API /settings/update: Error processing request: {e}", exc_info=True)
        return jsonify({"error": "Internal server error processing update"}), 500


def clean_nas(obj):
    """
    Recursively replaces NaN/Inf with None so JSON serialization works.
    """
    if isinstance(obj, dict):
        return {k: clean_nas(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nas(x) for x in obj]
    elif isinstance(obj, float):
        # Check for NaN or Infinity
        if np.isnan(obj) or np.isinf(obj):
            return None
    return obj


@api_app.route('/')
def serve_dashboard():
    base_dir = os.path.dirname(os.path.realpath(__file__))
    return send_from_directory(base_dir, 'vue_dashboard.html')


def run_api_server(app_config: dict, db_handler):
    """Runs the Flask API server in a separate thread."""
    global _DB_INSTANCE

    api_config = app_config.get('api_server', {})
    host = api_config.get('host', '0.0.0.0')
    port = api_config.get('port', 8123)
    debug_mode = api_config.get('debug', False)
    _DB_INSTANCE = db_handler

    logger.info(f"Starting API server on http://{host}:{port}")
    try:
        api_app.run(host=host, port=port, debug=debug_mode, use_reloader=False)
    except Exception as e:
        logger.error(f"API server failed to start or crashed: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.DEGRADED)
