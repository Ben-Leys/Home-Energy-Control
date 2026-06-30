import logging
import os
import hmac
import secrets
from collections import deque
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import Optional
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from flask import Flask, jsonify, make_response, request, send_from_directory, session
from werkzeug.security import check_password_hash

from hec.core import constants as c
from hec.core.app_logging import sync_app_status_from_incidents
from hec.core.app_state import GLOBAL_APP_STATE
from hec.database_ops import db_handler

api_app = Flask(__name__)
api_app.secret_key = "hec-auth-disabled-session-secret"
logger = logging.getLogger(__name__)

_DB_INSTANCE: Optional[db_handler] = None
_CSRF_SESSION_KEY = "csrf_token"
_AUTH_CONFIG = {
    "enabled": False,
    "password": None,
    "password_hash": None,
    "csrf_enabled": True,
    "same_origin_enabled": True,
}

_SETTING_TYPE_MAP = {
    "app_operating_mode": c.OperatingMode,
    "app_mediator_goal": c.MediatorGoal,
    "inverter_manual_state": c.InverterManualState,
    "evcc_manual_state": c.EVCCManualState,
    "battery_manual_mode": c.BatteryState,
    "inverter_manual_limit": int,
    "evcc_manual_limit": int,
}

_COMMAND_TYPE_MAP = {
    "summary_request": bool,
    "reboot_request": bool,
}

_ALLOWED_UPDATE_TYPE_MAP = {**_SETTING_TYPE_MAP, **_COMMAND_TYPE_MAP}
_RANGE_LIMITS = {
    "inverter_manual_limit": (0, 7000, "Inverter limit must be between 0 and 7000 W"),
    "evcc_manual_limit": (6, 32, "EVCC amps must be between 6 and 32"),
}
_CONTENT_SECURITY_POLICY = (
    "default-src 'self'; "
    "script-src 'self' https://unpkg.com 'unsafe-inline'; "
    "style-src 'self' 'unsafe-inline'; "
    "connect-src 'self'; "
    "img-src 'self' data:; "
    "object-src 'none'; "
    "base-uri 'self'; "
    "frame-ancestors 'none'"
)


@api_app.after_request
def add_security_headers(response):
    response.headers.setdefault("Content-Security-Policy", _CONTENT_SECURITY_POLICY)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    return response


def configure_api_security(app_config: dict):
    """Configures lightweight local dashboard authentication."""
    global _AUTH_CONFIG

    api_config = app_config.get("api_server", app_config) if app_config else {}
    auth_config = api_config.get("auth", {})

    password_env = auth_config.get("password_env", "HEC_AUTH_PASSWORD")
    password_hash_env = auth_config.get("password_hash_env", "HEC_AUTH_PASSWORD_HASH")
    cookie_secret_env = auth_config.get("cookie_secret_env", "HEC_AUTH_COOKIE_SECRET")

    password = auth_config.get("password") or os.getenv(password_env)
    password_hash = auth_config.get("password_hash") or os.getenv(password_hash_env)
    cookie_secret = auth_config.get("cookie_secret") or os.getenv(cookie_secret_env)
    enabled = bool(auth_config.get("enabled", bool(password or password_hash)))

    if enabled and not (password or password_hash):
        logger.warning("API auth is enabled but no password or password hash is configured. Disabling API auth.")
        enabled = False

    if enabled and not cookie_secret:
        cookie_secret = secrets.token_urlsafe(32)
        logger.info(
            "API auth is enabled without a configured cookie secret. "
            "Generated a transient secret; sessions will not survive app restarts."
        )

    cookie_max_age_days = int(auth_config.get("cookie_max_age_days", 365))
    api_app.secret_key = cookie_secret or "hec-auth-disabled-session-secret"
    api_app.permanent_session_lifetime = timedelta(days=max(1, cookie_max_age_days))
    api_app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE=auth_config.get("cookie_samesite", "Lax"),
        SESSION_COOKIE_SECURE=bool(auth_config.get("secure_cookie", False)),
    )

    _AUTH_CONFIG = {
        "enabled": enabled,
        "password": password,
        "password_hash": password_hash,
        "csrf_enabled": bool(auth_config.get("csrf_enabled", True)),
        "same_origin_enabled": bool(auth_config.get("same_origin_enabled", True)),
    }


def _auth_enabled() -> bool:
    return bool(_AUTH_CONFIG.get("enabled"))


def _is_authenticated() -> bool:
    if not _auth_enabled():
        return True
    return bool(session.get("authenticated"))


def _verify_password(candidate: str) -> bool:
    if not candidate:
        return False

    configured_hash = _AUTH_CONFIG.get("password_hash")
    if configured_hash:
        return check_password_hash(configured_hash, candidate)

    configured_password = _AUTH_CONFIG.get("password")
    if configured_password:
        return hmac.compare_digest(str(configured_password), str(candidate))

    return False


def _ensure_csrf_token() -> str:
    token = session.get(_CSRF_SESSION_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        session[_CSRF_SESSION_KEY] = token
    return token


def _json_error(message: str, status_code: int):
    return jsonify({"error": message}), status_code


def _parse_state_version(raw_value) -> Optional[int]:
    if raw_value is None:
        return None
    candidate = str(raw_value).strip()
    if candidate.startswith("W/"):
        candidate = candidate[2:].strip()
    candidate = candidate.strip('"')
    try:
        return int(candidate)
    except (TypeError, ValueError):
        return None


def _serialize_app_state():
    current_raw_state = GLOBAL_APP_STATE.get_all()
    serializable_state = _serialize_for_json(current_raw_state)
    return clean_nas(serializable_state)


def _state_etag(state_version: int) -> str:
    return f'"{int(state_version)}"'


def require_auth(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not _is_authenticated():
            return _json_error("Authentication required", 401)
        return view_func(*args, **kwargs)

    return wrapper


def _same_origin_request() -> bool:
    origin = request.headers.get("Origin")
    referer = request.headers.get("Referer")
    candidate = origin or referer
    if not candidate:
        return True

    parsed = urlparse(candidate)
    host_url = urlparse(request.host_url)
    return parsed.scheme == host_url.scheme and parsed.netloc == host_url.netloc


def require_csrf(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not _auth_enabled():
            return view_func(*args, **kwargs)

        if _AUTH_CONFIG.get("same_origin_enabled", True) and not _same_origin_request():
            return _json_error("Cross-origin request rejected", 403)

        if _AUTH_CONFIG.get("csrf_enabled", True):
            expected_token = session.get(_CSRF_SESSION_KEY)
            provided_token = request.headers.get("X-CSRF-Token")
            if not expected_token or not hmac.compare_digest(str(expected_token), str(provided_token or "")):
                return _json_error("Invalid CSRF token", 403)

        return view_func(*args, **kwargs)

    return wrapper


def _serialize_for_json(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, list):
        new_list = []
        for item in value:
            if isinstance(item, dict):
                new_list.append({k: _serialize_for_json(v) for k, v in item.items()})
            else:
                new_list.append(_serialize_for_json(item))
        return new_list
    if isinstance(value, deque):
        return list(value)
    if isinstance(value, dict):
        return {k: _serialize_for_json(v) for k, v in value.items()}
    return value


def _coerce_bool(raw_value):
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, str):
        normalized = raw_value.strip().lower()
        if normalized in ("true", "1", "yes", "on"):
            return True
        if normalized in ("false", "0", "no", "off"):
            return False
    raise ValueError("Expected a boolean value")


def _coerce_update_value(key, raw_value):
    target_type = _ALLOWED_UPDATE_TYPE_MAP[key]

    if target_type is bool:
        return _coerce_bool(raw_value)

    if target_type is int:
        if isinstance(raw_value, bool):
            raise ValueError("Expected an integer value")
        final_value = int(raw_value)
        if key in _RANGE_LIMITS:
            low, high, message = _RANGE_LIMITS[key]
            if not (low <= final_value <= high):
                raise ValueError(message)
        return final_value

    if isinstance(target_type, type) and issubclass(target_type, Enum):
        if isinstance(raw_value, target_type):
            return raw_value
        if not isinstance(raw_value, str):
            raise ValueError("Expected enum name")
        return target_type[raw_value]

    return raw_value


def _audit_update(key, value):
    remote_addr = request.remote_addr or "unknown"
    if key in _COMMAND_TYPE_MAP:
        logger.info("AUDIT command_request %s remote=%s", key, remote_addr)
    else:
        safe_value = value.name if isinstance(value, Enum) else value
        logger.info("AUDIT setting_change %s=%s remote=%s", key, safe_value, remote_addr)


def _apply_command_side_effects(key, value):
    if key == "reboot_request" and value:
        GLOBAL_APP_STATE.set("restart_status", "requested")
        GLOBAL_APP_STATE.set(
            "restart_message",
            "Restart requested. The runtime will stop safely so the supervisor can restart the app.",
        )


@api_app.route('/api/v1/auth/status', methods=['GET'])
def get_auth_status():
    authenticated = _is_authenticated()
    response = {
        "auth_enabled": _auth_enabled(),
        "authenticated": authenticated,
    }
    if authenticated:
        response["csrf_token"] = _ensure_csrf_token()
    return jsonify(response)


@api_app.route('/api/v1/auth/login', methods=['POST'])
def login_api():
    if not _auth_enabled():
        session.permanent = True
        session["authenticated"] = True
        return jsonify({"success": True, "csrf_token": _ensure_csrf_token()})

    data = request.get_json(silent=True) or {}
    if not _verify_password(data.get("password", "")):
        logger.info("AUDIT auth_login failed remote=%s", request.remote_addr or "unknown")
        return _json_error("Invalid password", 401)

    session.clear()
    session.permanent = True
    session["authenticated"] = True
    logger.info("AUDIT auth_login success remote=%s", request.remote_addr or "unknown")
    return jsonify({"success": True, "csrf_token": _ensure_csrf_token()})


@api_app.route('/api/v1/auth/logout', methods=['POST'])
@require_auth
@require_csrf
def logout_api():
    logger.info("AUDIT auth_logout remote=%s", request.remote_addr or "unknown")
    session.clear()
    return jsonify({"success": True})


@api_app.route('/api/v1/state', methods=['GET'])
@require_auth
def get_app_state_api():
    """API endpoint to get the current application state."""
    state_version = GLOBAL_APP_STATE.get_state_version()
    requested_version = _parse_state_version(request.args.get("since_version"))
    if requested_version is None:
        requested_version = _parse_state_version(request.headers.get("If-None-Match"))

    if requested_version is not None and requested_version >= state_version:
        response = make_response("", 304)
        response.headers["ETag"] = _state_etag(state_version)
        return response

    state_payload = _serialize_app_state()
    response = jsonify(state_payload)
    response.headers["ETag"] = _state_etag(state_payload.get("state_version", state_version))
    return response


@api_app.route("/api/v1/logs", methods=['GET'])
@require_auth
def get_logs():
    if _DB_INSTANCE is None:
        return jsonify({"error": "Database not initialized in API"}), 500

    try:
        limit = request.args.get('limit', default=1000, type=int)
        if limit is None:
            limit = 1000
        limit = max(1, min(limit, 20000))
    except (TypeError, ValueError):
        limit = 1000

    logs = _DB_INSTANCE.get_latest_logs(limit)
    return jsonify({"logs": logs})


@api_app.route("/api/v1/incidents", methods=["GET"])
@require_auth
def get_incidents_api():
    if _DB_INSTANCE is None:
        return jsonify({"error": "Database not initialized in API"}), 500

    return jsonify(_DB_INSTANCE.get_dashboard_incidents())


@api_app.route("/api/v1/incidents/<int:incident_id>/acknowledge", methods=["POST"])
@require_auth
@require_csrf
def acknowledge_incident_api(incident_id: int):
    if _DB_INSTANCE is None:
        return jsonify({"error": "Database not initialized in API"}), 500

    data = request.get_json(silent=True) or {}
    acknowledged_by = data.get("acknowledged_by") or request.remote_addr or "dashboard"
    incident = _DB_INSTANCE.acknowledge_incident(incident_id, acknowledged_by=acknowledged_by)
    if not incident:
        return _json_error("Incident not found", 404)

    logger.info("AUDIT incident_acknowledged id=%s remote=%s", incident_id, request.remote_addr or "unknown")
    sync_app_status_from_incidents(GLOBAL_APP_STATE, _DB_INSTANCE)
    state_payload = _serialize_app_state()
    return jsonify({
        "success": True,
        "incident": incident,
        "state_version": state_payload.get("state_version"),
        "state": state_payload,
    })


@api_app.route("/api/v1/incidents/acknowledge-all", methods=["POST"])
@require_auth
@require_csrf
def acknowledge_all_incidents_api():
    if _DB_INSTANCE is None:
        return jsonify({"error": "Database not initialized in API"}), 500

    data = request.get_json(silent=True) or {}
    acknowledged_by = data.get("acknowledged_by") or request.remote_addr or "dashboard"
    incidents = _DB_INSTANCE.acknowledge_all_active_incidents(acknowledged_by=acknowledged_by)

    logger.info(
        "AUDIT incident_acknowledged_all count=%s remote=%s",
        len(incidents),
        request.remote_addr or "unknown",
    )
    sync_app_status_from_incidents(GLOBAL_APP_STATE, _DB_INSTANCE)
    state_payload = _serialize_app_state()
    return jsonify({
        "success": True,
        "incidents": incidents,
        "state_version": state_payload.get("state_version"),
        "state": state_payload,
    })


@api_app.route("/api/v1/incidents/<int:incident_id>/resolve", methods=["POST"])
@require_auth
@require_csrf
def resolve_incident_api(incident_id: int):
    if _DB_INSTANCE is None:
        return jsonify({"error": "Database not initialized in API"}), 500

    incident = _DB_INSTANCE.resolve_incident(incident_id)
    if not incident:
        return _json_error("Incident not found", 404)

    logger.info("AUDIT incident_resolved id=%s remote=%s", incident_id, request.remote_addr or "unknown")
    sync_app_status_from_incidents(GLOBAL_APP_STATE, _DB_INSTANCE)
    state_payload = _serialize_app_state()
    return jsonify({
        "success": True,
        "incident": incident,
        "state_version": state_payload.get("state_version"),
        "state": state_payload,
    })


@api_app.route("/api/v1/notifications/devices", methods=["POST"])
@require_auth
@require_csrf
def register_notification_device_api():
    if _DB_INSTANCE is None:
        return jsonify({"error": "Database not initialized in API"}), 500

    data = request.get_json(silent=True) or {}
    try:
        device = _DB_INSTANCE.register_notification_device(
            device_token=data.get("device_token"),
            label=data.get("label") or "Dashboard device",
            notification_types=data.get("notification_types") or [],
            enabled=bool(data.get("enabled", True)),
        )
    except ValueError as e:
        return _json_error(str(e), 400)

    logger.info("AUDIT notification_device_registered remote=%s", request.remote_addr or "unknown")
    return jsonify({"success": True, "device": device})


@api_app.route("/api/v1/notifications/pending", methods=["GET"])
@require_auth
def get_pending_notifications_api():
    if _DB_INSTANCE is None:
        return jsonify({"error": "Database not initialized in API"}), 500

    device_token = request.args.get("device_token", "")
    notifications = _DB_INSTANCE.take_pending_notifications(device_token)
    return jsonify({"notifications": notifications})


@api_app.route('/api/v1/settings/update', methods=['POST'])
@require_auth
@require_csrf
def update_app_setting_api():
    """
    API endpoint to update a specifically allowed setting or command in AppState.
    Expects JSON body: {"key": "app_state_key_name", "value": "new_value"}
    """
    try:
        data = request.get_json(silent=True)
        if not data or 'key' not in data or 'value' not in data:  # 'value' can be None
            logger.warning("API /settings/update: Missing 'key' or 'value' in request JSON.")
            return jsonify({"error": "Missing 'key' or 'value' in request body"}), 400

        key = data['key']
        raw_val = data['value']

        logger.info(f"API /settings/update: Received request to update '{key}' to '{raw_val}'")

        if key not in _ALLOWED_UPDATE_TYPE_MAP:
            logger.warning(f"API /settings/update: Attempt to update non-allowlisted key '{key}'.")
            return jsonify({"error": f"Setting key '{key}' is not allowed for API updates"}), 400

        if not GLOBAL_APP_STATE.has_key(key):
            logger.warning(f"API /settings/update: Allowlisted key '{key}' is not present in AppState.")
            return jsonify({"error": f"Unknown setting key: {key}"}), 400

        try:
            final_value = _coerce_update_value(key, raw_val)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Conversion failed for {key} with value {raw_val}: {e}")
            return jsonify({"error": f"Invalid value '{raw_val}' for {key}: {e}"}), 400

        GLOBAL_APP_STATE.set(key, final_value)
        _apply_command_side_effects(key, final_value)
        confirmed = GLOBAL_APP_STATE.get(key)

        json_val = confirmed.name if isinstance(confirmed, Enum) else confirmed

        _audit_update(key, confirmed)
        state_payload = _serialize_app_state()
        return jsonify({
            "success": True,
            "key": key,
            "new_value_stored": json_val,
            "state_version": state_payload.get("state_version"),
            "state": state_payload,
        })

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
    configure_api_security(app_config)

    logger.info(f"Starting API server on http://{host}:{port}")
    try:
        api_app.run(host=host, port=port, debug=debug_mode, use_reloader=False)
    except Exception as e:
        logger.error(f"API server failed to start or crashed: {e}", exc_info=True)
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.DEGRADED)
