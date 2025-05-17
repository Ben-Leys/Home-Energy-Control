import logging
from datetime import datetime
from enum import Enum
from flask import Flask, jsonify

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
                isinstance(value, c.AppManualState) or \
                isinstance(value, c.OperatingMode) or \
                isinstance(value, c.InverterStatus) or \
                isinstance(value, c.InverterManualState) or \
                isinstance(value, c.EVChargeStatus) or \
                isinstance(value, c.EVCCManualState):
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


def run_api_server(app_config: dict):
    """Runs the Flask API server in a separate thread."""
    api_config = app_config.get('api_server', {})
    host = api_config.get('host', '0.0.0.0')
    port = api_config.get('port', 5000)
    debug_mode = api_config.get('debug', False)

    logger.info(f"Starting API server on http://{host}:{port}")
    try:
        api_app.run(host=host, port=port, debug=debug_mode, use_reloader=False)
        return
    except Exception as e:
        logger.error(f"API server failed to start or crashed: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.DEGRADED)
