import copy
import logging
import re
import shutil
import subprocess
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from hec.core import api_server
from hec.core import constants as c
from hec.core.app_logging import GlobalStateHandler, sync_app_status_from_incidents
from hec.core.app_state import AppState, GLOBAL_APP_STATE
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.system_mediator import SystemMediator


class Phase6DatabaseTestCase(unittest.TestCase):
    def setUp(self):
        self.scratch_root = Path.cwd() / "_scratch"
        self.scratch_root.mkdir(exist_ok=True)
        safe_test_name = self._testMethodName.replace("test_", "")
        self.db_path = self.scratch_root / f"phase6-{safe_test_name}.sqlite"
        self._remove_sqlite_artifacts()
        self.handlers = []

    def tearDown(self):
        for handler in self.handlers:
            handler.close_connection()
        self._remove_sqlite_artifacts()

    def _remove_sqlite_artifacts(self):
        paths = [
            self.db_path,
            self.db_path.with_name(f"{self.db_path.name}-journal"),
            self.db_path.with_name(f"{self.db_path.name}-wal"),
            self.db_path.with_name(f"{self.db_path.name}-shm"),
        ]
        for attempt in range(10):
            try:
                for path in paths:
                    path.unlink(missing_ok=True)
                return
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.05)

    def make_handler(self, **config_overrides):
        config = {
            "path": str(self.db_path),
            "busy_timeout_ms": 750,
            **config_overrides,
        }
        handler = DatabaseHandler(config)
        handler.initialize_database()
        self.handlers.append(handler)
        return handler


class TestDatabasePhase6Incidents(Phase6DatabaseTestCase):
    def test_record_incident_persists_and_deduplicates_active_occurrences(self):
        handler = self.make_handler()
        first_seen = datetime(2026, 6, 29, 9, 0, tzinfo=timezone.utc)

        first = handler.record_incident(
            severity="error",
            source="inverter",
            message="API rejected the limit update",
            occurred_at_utc=first_seen,
        )
        second = handler.record_incident(
            severity="error",
            source="inverter",
            message="API rejected the limit update",
            occurred_at_utc=first_seen + timedelta(minutes=1),
        )

        self.assertEqual(first["id"], second["id"])
        self.assertEqual(2, second["occurrence_count"])
        self.assertEqual("active", second["status"])
        self.assertEqual(first_seen.isoformat(), second["first_seen_utc"])
        self.assertEqual((first_seen + timedelta(minutes=1)).isoformat(), second["last_seen_utc"])
        self.assertTrue(first["should_notify"])
        self.assertFalse(second["should_notify"])

        active = handler.get_incidents(status="active")
        self.assertEqual(1, len(active))
        self.assertEqual(2, active[0]["occurrence_count"])

    def test_acknowledged_incident_reopens_on_recurrence_and_can_resolve(self):
        handler = self.make_handler()
        first_seen = datetime(2026, 6, 29, 9, 0, tzinfo=timezone.utc)
        incident = handler.record_incident(
            severity="warning",
            source="evcc",
            message="EVCC polling task failed",
            occurred_at_utc=first_seen,
        )

        acknowledged = handler.acknowledge_incident(
            incident["id"],
            acknowledged_by="dashboard",
            acknowledged_at_utc=first_seen + timedelta(minutes=2),
        )
        self.assertEqual("acknowledged", acknowledged["status"])
        self.assertEqual([], handler.get_incidents(status="active"))

        reopened = handler.record_incident(
            severity="warning",
            source="evcc",
            message="EVCC polling task failed",
            occurred_at_utc=first_seen + timedelta(minutes=3),
        )
        self.assertEqual(incident["id"], reopened["id"])
        self.assertEqual("active", reopened["status"])
        self.assertIsNone(reopened["acknowledged_at_utc"])
        self.assertEqual(2, reopened["occurrence_count"])
        self.assertTrue(reopened["should_notify"])

        resolved = handler.resolve_incident(
            incident["id"],
            resolved_at_utc=first_seen + timedelta(minutes=5),
        )
        self.assertEqual("resolved", resolved["status"])

        groups = handler.get_dashboard_incidents()
        self.assertEqual([], groups["active"])
        self.assertEqual([], groups["acknowledged"])
        self.assertEqual([incident["id"]], [row["id"] for row in groups["resolved"]])

    def test_notification_devices_filter_types_and_pending_notifications_are_one_shot(self):
        handler = self.make_handler()
        handler.register_notification_device(
            device_token="browser-1",
            label="Kitchen tablet",
            notification_types=["warning", "peak_consumption"],
        )
        handler.register_notification_device(
            device_token="browser-2",
            label="Work phone",
            notification_types=["error"],
        )

        handler.queue_notification(
            notification_type="warning",
            title="Warning incident",
            message="EVCC polling failed",
            incident_id=1,
            dedupe_key="incident:1:warning",
        )
        handler.queue_notification(
            notification_type="error",
            title="Error incident",
            message="Inverter rejected command",
            incident_id=2,
            dedupe_key="incident:2:error",
        )
        handler.queue_notification(
            notification_type="peak_consumption",
            title="Peak consumption",
            message="15m average exceeded the peak limit",
            incident_id=3,
            dedupe_key="incident:3:peak",
        )
        handler.queue_notification(
            notification_type="warning",
            title="Warning incident",
            message="EVCC polling failed again",
            incident_id=1,
            dedupe_key="incident:1:warning",
        )

        browser_1_pending = handler.take_pending_notifications("browser-1")
        browser_2_pending = handler.take_pending_notifications("browser-2")
        browser_1_second_poll = handler.take_pending_notifications("browser-1")

        self.assertEqual(["warning", "peak_consumption"], [row["notification_type"] for row in browser_1_pending])
        self.assertEqual(["error"], [row["notification_type"] for row in browser_2_pending])
        self.assertEqual([], browser_1_second_poll)


class TestLoggingPhase6Incidents(Phase6DatabaseTestCase):
    def test_warning_and_error_logs_create_incidents_and_status_is_derived_from_active_incidents(self):
        handler = self.make_handler()
        app_state = AppState()
        global_state_handler = GlobalStateHandler(app_state)
        global_state_handler.set_db_handler(handler)
        global_state_handler.setFormatter(logging.Formatter("%(message)s"))

        error_record = logging.LogRecord(
            name="hec.controllers.inverter",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="Inverter API rejected the limit update",
            args=(),
            exc_info=None,
        )
        warning_record = logging.LogRecord(
            name="hec.logic_engine.evcc",
            level=logging.WARNING,
            pathname=__file__,
            lineno=2,
            msg="EVCC polling task failed",
            args=(),
            exc_info=None,
        )
        info_record = logging.LogRecord(
            name="hec.core.runtime",
            level=logging.INFO,
            pathname=__file__,
            lineno=3,
            msg="Runtime heartbeat",
            args=(),
            exc_info=None,
        )

        global_state_handler.emit(error_record)
        self.assertIs(c.AppStatus.ALARM, app_state.get("app_state"))

        global_state_handler.emit(warning_record)
        global_state_handler.emit(info_record)
        self.assertIs(c.AppStatus.ALARM, app_state.get("app_state"))

        incidents = handler.get_incidents(status="active")
        self.assertEqual(["error", "warning"], [row["severity"] for row in incidents])

        handler.acknowledge_incident(incidents[0]["id"], acknowledged_by="dashboard")
        sync_app_status_from_incidents(app_state, handler)
        self.assertIs(c.AppStatus.WARNING, app_state.get("app_state"))

        handler.acknowledge_incident(incidents[1]["id"], acknowledged_by="dashboard")
        sync_app_status_from_incidents(app_state, handler)
        self.assertIs(c.AppStatus.NORMAL, app_state.get("app_state"))


class TestApiPhase6IncidentsAndNotifications(Phase6DatabaseTestCase):
    def setUp(self):
        super().setUp()
        self.original_values = copy.deepcopy(GLOBAL_APP_STATE.current_values)
        self.original_prediction_plan_df = GLOBAL_APP_STATE.prediction_plan_df
        self.original_db_handler = GLOBAL_APP_STATE.db_handler
        self.original_api_db = api_server._DB_INSTANCE
        GLOBAL_APP_STATE.current_values = AppState().current_values.copy()
        GLOBAL_APP_STATE.prediction_plan_df = None
        GLOBAL_APP_STATE.db_handler = None
        api_server.configure_api_security({"api_server": {"auth": {"enabled": False}}})
        self.handler = self.make_handler()
        api_server._DB_INSTANCE = self.handler
        self.client = api_server.api_app.test_client()

    def tearDown(self):
        GLOBAL_APP_STATE.current_values = self.original_values
        GLOBAL_APP_STATE.prediction_plan_df = self.original_prediction_plan_df
        GLOBAL_APP_STATE.db_handler = self.original_db_handler
        api_server._DB_INSTANCE = self.original_api_db
        api_server.configure_api_security({"api_server": {"auth": {"enabled": False}}})
        super().tearDown()

    def test_incident_api_lists_and_acknowledges_incidents_with_canonical_status(self):
        incident = self.handler.record_incident(
            severity="error",
            source="daily_summary",
            message="Forecasting failed",
        )
        GLOBAL_APP_STATE.set("app_state", c.AppStatus.ALARM)

        list_response = self.client.get("/api/v1/incidents")
        self.assertEqual(200, list_response.status_code)
        self.assertEqual([incident["id"]], [row["id"] for row in list_response.get_json()["active"]])

        ack_response = self.client.post(
            f"/api/v1/incidents/{incident['id']}/acknowledge",
            json={"acknowledged_by": "dashboard"},
        )
        self.assertEqual(200, ack_response.status_code)
        body = ack_response.get_json()
        self.assertEqual("acknowledged", body["incident"]["status"])
        self.assertEqual("NORMAL", body["state"]["app_state"])

        after_ack_response = self.client.get("/api/v1/incidents")
        self.assertEqual([], after_ack_response.get_json()["active"])
        self.assertEqual([incident["id"]], [row["id"] for row in after_ack_response.get_json()["acknowledged"]])

    def test_notification_device_api_registers_preferences_and_returns_pending_notifications(self):
        register_response = self.client.post(
            "/api/v1/notifications/devices",
            json={
                "device_token": "browser-1",
                "label": "Dashboard phone",
                "notification_types": ["warning", "peak_consumption"],
            },
        )
        self.assertEqual(200, register_response.status_code)
        registered = register_response.get_json()["device"]
        self.assertEqual(["warning", "peak_consumption"], registered["notification_types"])

        self.handler.queue_notification(
            notification_type="warning",
            title="Warning incident",
            message="Battery data missing",
            dedupe_key="warning:1",
        )
        self.handler.queue_notification(
            notification_type="error",
            title="Error incident",
            message="Inverter failed",
            dedupe_key="error:1",
        )
        self.handler.queue_notification(
            notification_type="peak_consumption",
            title="Peak consumption",
            message="Peak threshold exceeded",
            dedupe_key="peak:1",
        )

        pending_response = self.client.get("/api/v1/notifications/pending?device_token=browser-1")
        self.assertEqual(200, pending_response.status_code)
        self.assertEqual(
            ["warning", "peak_consumption"],
            [row["notification_type"] for row in pending_response.get_json()["notifications"]],
        )

    def test_dashboard_contains_incident_views_and_device_activation_preferences(self):
        response = self.client.get("/")
        try:
            self.assertEqual(200, response.status_code)
            html = response.get_data(as_text=True)
            self.assertIn("currentTab === 'incidents'", html)
            self.assertIn("/api/v1/incidents", html)
            self.assertIn("acknowledgeIncident", html)
            self.assertIn("Activate this device", html)
            self.assertIn("Notification.requestPermission", html)
            self.assertIn("/api/v1/notifications/devices", html)
            self.assertIn("/api/v1/notifications/pending", html)
            self.assertIn("notificationPreferences.warning", html)
            self.assertIn("notificationPreferences.error", html)
            self.assertIn("notificationPreferences.peak_consumption", html)
        finally:
            response.close()


class TestDashboardPhase6JavaScript(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("node") is None:
            raise unittest.SkipTest("node is required to execute dashboard JavaScript behavior tests")

        dashboard_path = Path(__file__).resolve().parents[1] / "core" / "vue_dashboard.html"
        html = dashboard_path.read_text(encoding="utf-8")
        match = re.search(
            r"<script>\s*const \{ createApp, ref, computed, watch, nextTick, onMounted, onUnmounted \} = Vue"
            r"[\s\S]*?</script>",
            html,
        )
        if not match:
            raise AssertionError("Could not find dashboard Vue application script")
        cls.dashboard_script = match.group(0).replace("<script>", "").replace("</script>", "")

    def _run_dashboard_js(self, scenario: str):
        node_script = f"""
const assert = require('assert');
const dashboardScript = {self.dashboard_script!r};
const storage = {{}};

globalThis.window = {{
  __HEC_DASHBOARD_ENABLE_TEST_HOOKS__: true,
  confirm: () => true,
  localStorage: {{
    getItem: (key) => Object.prototype.hasOwnProperty.call(storage, key) ? storage[key] : null,
    setItem: (key, value) => {{ storage[key] = String(value); }},
    removeItem: (key) => {{ delete storage[key]; }}
  }},
  Notification: {{
    permission: 'granted',
    requestPermission: async () => 'granted'
  }},
  crypto: {{ randomUUID: () => 'browser-token-1' }}
}};
globalThis.Notification = function(title, options) {{
  globalThis.__notifications.push({{ title, body: options && options.body }});
}};
Object.defineProperty(globalThis.Notification, 'permission', {{
  get: () => window.Notification.permission,
  set: (value) => {{ window.Notification.permission = value; }}
}});
globalThis.Notification.requestPermission = window.Notification.requestPermission;
globalThis.__notifications = [];
globalThis.__alerts = [];
globalThis.alert = (message) => globalThis.__alerts.push(message);
globalThis.crypto = {{ randomUUID: () => 'browser-token-1' }};
globalThis.setInterval = () => 1;
globalThis.clearInterval = () => undefined;
globalThis.setTimeout = (callback) => {{
  callback();
  return 1;
}};

globalThis.Vue = {{
  createApp: (options) => ({{
    mount: () => {{
      globalThis.__dashboardComponent = options.setup();
      return globalThis.__dashboardComponent;
    }}
  }}),
  ref: (value) => ({{ value }}),
  computed: (callback) => ({{ get value() {{ return callback(); }} }}),
  watch: () => undefined,
  nextTick: () => Promise.resolve(),
  onMounted: () => undefined,
  onUnmounted: () => undefined
}};

eval(dashboardScript);
const hooks = window.__HEC_DASHBOARD_TEST_HOOKS__;
const component = globalThis.__dashboardComponent;
assert(hooks, 'dashboard test hooks were not exposed');
assert(component, 'dashboard component was not mounted');

(async () => {{
{scenario}
}})().catch((error) => {{
  console.error(error && error.stack ? error.stack : error);
  process.exit(1);
}});
"""
        scratch_root = Path.cwd() / "_scratch"
        scratch_root.mkdir(exist_ok=True)
        script_path = scratch_root / "phase6-dashboard-test.js"
        script_path.write_text(node_script, encoding="utf-8")
        try:
            result = subprocess.run(
                ["node", str(script_path)],
                cwd=Path(__file__).resolve().parents[2],
                text=True,
                capture_output=True,
                check=False,
            )
        finally:
            script_path.unlink(missing_ok=True)
        if result.returncode != 0:
            self.fail(
                "Dashboard JavaScript scenario failed\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )

    def test_device_activation_sends_selected_notification_types_and_polling_displays_one_shot_notifications(self):
        self._run_dashboard_js(
            """
hooks.auth.value.authenticated = true;
const calls = [];
let pendingCalls = 0;
globalThis.fetch = async (url, options = {}) => {
  calls.push({ url, options });
  if (url === '/api/v1/notifications/devices') {
    const body = JSON.parse(options.body);
    assert.deepStrictEqual(body.notification_types, ['warning', 'peak_consumption']);
    return { ok: true, status: 200, json: async () => ({ device: body }) };
  }
  if (String(url).startsWith('/api/v1/notifications/pending')) {
    pendingCalls += 1;
    return {
      ok: true,
      status: 200,
      json: async () => ({ notifications: pendingCalls === 1 ? [
        { title: 'Peak consumption', message: '15m average exceeded', notification_type: 'peak_consumption' }
      ] : [] })
    };
  }
  return { ok: true, status: 200, json: async () => ({}) };
};

hooks.notificationPreferences.value.warning = true;
hooks.notificationPreferences.value.error = false;
hooks.notificationPreferences.value.peak_consumption = true;

await component.activateNotificationDevice();
await component.fetchPendingNotifications();

assert.strictEqual(hooks.notificationDevice.value.active, true);
assert.strictEqual(hooks.notificationDevice.value.token, 'browser-token-1');
assert.strictEqual(globalThis.__notifications.length, 1);
assert.strictEqual(globalThis.__notifications[0].title, 'Peak consumption');
"""
        )


class TestSystemMediatorPhase6PeakNotifications(unittest.TestCase):
    def setUp(self):
        self.original_values = copy.deepcopy(GLOBAL_APP_STATE.current_values)
        self.original_prediction_plan_df = GLOBAL_APP_STATE.prediction_plan_df
        GLOBAL_APP_STATE.current_values = AppState().current_values.copy()
        GLOBAL_APP_STATE.prediction_plan_df = None
        GLOBAL_APP_STATE.set("average_grid_import_watts", {"5m": 3000, "10m": 2600, "15m": 2550})
        GLOBAL_APP_STATE.set("inverter_manual_state", c.InverterManualState.INV_CMD_LIMIT_STANDARD)
        GLOBAL_APP_STATE.set("evcc_manual_state", c.EVCCManualState.EVCC_CMD_STATE_PV)
        GLOBAL_APP_STATE.set("battery_data", {"mode": c.BatteryState.BATTERY_ON})
        GLOBAL_APP_STATE.db_handler = MagicMock()

        self.inverter_client = MagicMock()
        self.inverter_client.standard_power_limit = 7000
        self.mediator = SystemMediator(
            {"smtp": {}, "mediator": {"standard_max_peak_consumption_kw": 2.5}},
            MagicMock(),
            self.inverter_client,
            MagicMock(),
        )
        self.mediator.current_max_peak_kw = 2.5
        self.mediator.ignore_start = (datetime.now() - timedelta(hours=2)).time()
        self.mediator.ignore_end = (datetime.now() - timedelta(hours=1)).time()
        self.mediator.last_email_sent_time = None

    def tearDown(self):
        GLOBAL_APP_STATE.current_values = self.original_values
        GLOBAL_APP_STATE.prediction_plan_df = self.original_prediction_plan_df
        GLOBAL_APP_STATE.db_handler = None

    def test_peak_exceedance_records_peak_incident_for_notification_dispatch(self):
        with (
            patch("hec.logic_engine.system_mediator.send_email_with_attachments"),
            patch("hec.logic_engine.system_mediator.record_peak_consumption_incident") as record_peak,
        ):
            self.mediator._handle_peak_consumption()

        record_peak.assert_called_once()
        _, kwargs = record_peak.call_args
        self.assertEqual(GLOBAL_APP_STATE.db_handler, kwargs["db_handler"])
        self.assertEqual(2.5, kwargs["limit_kw"])
        self.assertEqual("peak_consumption", kwargs["notification_type"])


if __name__ == "__main__":
    unittest.main()
