import copy
import json
import re
import shutil
import subprocess
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytz

from hec.core import api_server
from hec.core import constants as c
from hec.core.app_state import AppState, GLOBAL_APP_STATE
from hec.logic_engine import data_processors
from hec.logic_engine import scheduled_tasks


class TestAppStatePhase3(unittest.TestCase):
    def test_set_increments_state_version_and_get_all_returns_copy(self):
        app_state = AppState()
        initial_version = app_state.get_state_version()

        app_state.set("app_state", c.AppStatus.NORMAL)
        state = app_state.get_all()
        state["app_state"] = c.AppStatus.ALARM

        self.assertEqual(initial_version + 1, app_state.get_state_version())
        self.assertIs(c.AppStatus.NORMAL, app_state.get("app_state"))
        self.assertEqual(app_state.get_state_version(), state["state_version"])

    def test_get_returns_mutable_copy_without_changing_live_state_or_version(self):
        app_state = AppState()
        app_state.set("p1_meter_data", {"active_power_w": 100})
        version_after_set = app_state.get_state_version()

        returned_state = app_state.get("p1_meter_data")
        returned_state["active_power_w"] = 900

        self.assertEqual(100, app_state.get("p1_meter_data")["active_power_w"])
        self.assertEqual(version_after_set, app_state.get_state_version())

    def test_set_copies_mutable_values_so_caller_mutations_do_not_bypass_versioning(self):
        app_state = AppState()
        source_state = {"active_power_w": 100}

        app_state.set("p1_meter_data", source_state)
        version_after_set = app_state.get_state_version()
        source_state["active_power_w"] = 900

        self.assertEqual(100, app_state.get("p1_meter_data")["active_power_w"])
        self.assertEqual(version_after_set, app_state.get_state_version())

    def test_mutate_updates_mutable_state_through_versioned_lock_path(self):
        app_state = AppState()
        app_state.set("p1_meter_data", {"active_power_w": 100})
        version_after_set = app_state.get_state_version()

        updated = app_state.mutate(
            "p1_meter_data",
            lambda current: current.update({"active_power_w": 250}),
        )

        self.assertEqual({"active_power_w": 250}, updated)
        self.assertEqual(250, app_state.get("p1_meter_data")["active_power_w"])
        self.assertGreater(app_state.get_state_version(), version_after_set)

    def test_snapshot_context_reads_one_consistent_version_while_live_state_changes(self):
        app_state = AppState()
        app_state.db_handler = MagicMock()
        app_state.set("evcc_manual_limit", 6)

        with app_state.snapshot_context():
            snapshot_version = app_state.get("state_version")
            app_state.set("evcc_manual_limit", 12)

            self.assertEqual(6, app_state.get("evcc_manual_limit"))
            self.assertEqual(snapshot_version, app_state.get("state_version"))

        self.assertEqual(12, app_state.get("evcc_manual_limit"))
        self.assertGreater(app_state.get_state_version(), snapshot_version)


class TestApiPhase3StateResponses(unittest.TestCase):
    def setUp(self):
        self.original_values = copy.deepcopy(GLOBAL_APP_STATE.current_values)
        self.original_prediction_plan_df = GLOBAL_APP_STATE.prediction_plan_df
        self.original_db_handler = GLOBAL_APP_STATE.db_handler
        GLOBAL_APP_STATE.current_values = AppState().current_values.copy()
        GLOBAL_APP_STATE.prediction_plan_df = None
        GLOBAL_APP_STATE.db_handler = MagicMock()
        api_server.configure_api_security({"api_server": {"auth": {"enabled": False}}})
        self.client = api_server.api_app.test_client()

    def tearDown(self):
        GLOBAL_APP_STATE.current_values = self.original_values
        GLOBAL_APP_STATE.prediction_plan_df = self.original_prediction_plan_df
        GLOBAL_APP_STATE.db_handler = self.original_db_handler
        api_server.configure_api_security({"api_server": {"auth": {"enabled": False}}})

    def test_state_endpoint_includes_version_etag_and_returns_not_modified(self):
        GLOBAL_APP_STATE.set("app_operating_mode", c.OperatingMode.MODE_MANUAL)

        first_response = self.client.get("/api/v1/state")
        self.assertEqual(200, first_response.status_code)
        first_body = first_response.get_json()
        first_version = first_body["state_version"]

        self.assertIn("ETag", first_response.headers)
        self.assertEqual(f'"{first_version}"', first_response.headers["ETag"])

        unchanged_response = self.client.get(f"/api/v1/state?since_version={first_version}")
        self.assertEqual(304, unchanged_response.status_code)

        GLOBAL_APP_STATE.set("evcc_manual_limit", 12)
        changed_response = self.client.get(f"/api/v1/state?since_version={first_version}")
        self.assertEqual(200, changed_response.status_code)
        self.assertGreater(changed_response.get_json()["state_version"], first_version)

    def test_update_endpoint_returns_canonical_state_and_state_version(self):
        response = self.client.post(
            "/api/v1/settings/update",
            json={"key": "app_operating_mode", "value": "MODE_AUTO"},
        )

        self.assertEqual(200, response.status_code)
        body = response.get_json()
        self.assertTrue(body["success"])
        self.assertEqual("MODE_AUTO", body["new_value_stored"])
        self.assertEqual(body["state_version"], body["state"]["state_version"])
        self.assertEqual("MODE_AUTO", body["state"]["app_operating_mode"])

    def test_dashboard_contains_header_pending_edit_and_versioned_polling_hooks(self):
        response = self.client.get("/")
        try:
            self.assertEqual(200, response.status_code)

            html = response.get_data(as_text=True)
            self.assertIn('<div class="header-row">', html)
            self.assertIn('<h3>🏠 Home Energy Control</h3>', html)
            self.assertIn('POLL_INTERVAL_MS = 15000', html)
            self.assertIn('pendingEdits', html)
            self.assertIn('lastConfirmedState', html)
            self.assertIn('since_version', html)
            self.assertIn('If-None-Match', html)
            self.assertIn('restoreLastConfirmedValue', html)
        finally:
            response.close()

    def test_dashboard_current_price_interval_render_is_null_safe(self):
        response = self.client.get("/")
        try:
            self.assertEqual(200, response.status_code)

            html = response.get_data(as_text=True)
            self.assertIn("formatTimeHm(currentPrice?.start)", html)
            self.assertIn("formatTimeHm(currentPrice?.end)", html)
            self.assertNotIn("formatTimeHm(currentPrice.start)", html)
            self.assertNotIn("formatTimeHm(currentPrice.end)", html)
        finally:
            response.close()


class TestDashboardStateSyncJavaScript(unittest.TestCase):
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
const dashboardScript = {json.dumps(self.dashboard_script)};

globalThis.window = {{
  __HEC_DASHBOARD_ENABLE_TEST_HOOKS__: true,
  confirm: () => true
}};
globalThis.__alerts = [];
globalThis.alert = (message) => globalThis.__alerts.push(message);
globalThis.fetch = async () => {{
  throw new Error('fetch was not mocked for this test');
}};
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
        script_path = scratch_root / "phase3-dashboard-test.js"
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

    def test_stale_poll_payload_is_ignored_after_newer_confirmed_state(self):
        self._run_dashboard_js(
            """
hooks.applyStatePayload({ state_version: 3, evcc_manual_limit: 12 });
hooks.applyStatePayload({ state_version: 2, evcc_manual_limit: 6 });

assert.strictEqual(hooks.stateVersion.value, 3);
assert.strictEqual(hooks.state.value.evcc_manual_limit, 12);
assert.strictEqual(hooks.lastConfirmedState.value.evcc_manual_limit, 12);
"""
        )

    def test_pending_edit_is_merged_over_newer_poll_payload(self):
        self._run_dashboard_js(
            """
hooks.applyStatePayload({
  state_version: 1,
  app_operating_mode: 'MODE_MANUAL',
  evcc_manual_limit: 6
});
hooks.state.value.evcc_manual_limit = 16;
hooks.setPending('evcc_manual_limit', true);

hooks.applyStatePayload({
  state_version: 2,
  app_operating_mode: 'MODE_AUTO',
  evcc_manual_limit: 12
});

assert.strictEqual(hooks.stateVersion.value, 2);
assert.strictEqual(hooks.state.value.app_operating_mode, 'MODE_AUTO');
assert.strictEqual(hooks.state.value.evcc_manual_limit, 16);
assert.strictEqual(hooks.lastConfirmedState.value.evcc_manual_limit, 12);
"""
        )

    def test_failed_update_restores_last_confirmed_value_and_clears_pending_flag(self):
        self._run_dashboard_js(
            """
hooks.applyStatePayload({ state_version: 1, evcc_manual_limit: 6 });
hooks.state.value.evcc_manual_limit = 99;
globalThis.fetch = async () => ({
  status: 400,
  ok: false,
  json: async () => ({ success: false, error: 'invalid value' })
});

await component.updateSetting('evcc_manual_limit', 99);

assert.strictEqual(hooks.state.value.evcc_manual_limit, 6);
assert.strictEqual(hooks.isPending('evcc_manual_limit'), false);
assert.strictEqual(Object.keys(hooks.pendingEdits.value).length, 0);
assert.strictEqual(globalThis.__alerts.length, 1);
"""
        )


class TestScheduledTasksPhase3(unittest.TestCase):
    def setUp(self):
        self.original_values = copy.deepcopy(GLOBAL_APP_STATE.current_values)
        self.original_prediction_plan_df = GLOBAL_APP_STATE.prediction_plan_df
        self.original_db_handler = GLOBAL_APP_STATE.db_handler
        GLOBAL_APP_STATE.current_values = AppState().current_values.copy()
        GLOBAL_APP_STATE.prediction_plan_df = None
        GLOBAL_APP_STATE.db_handler = None

    def tearDown(self):
        GLOBAL_APP_STATE.current_values = self.original_values
        GLOBAL_APP_STATE.prediction_plan_df = self.original_prediction_plan_df
        GLOBAL_APP_STATE.db_handler = self.original_db_handler

    def test_mediator_task_uses_one_app_state_snapshot_for_decision_run(self):
        GLOBAL_APP_STATE.set("prediction_plan", [])
        GLOBAL_APP_STATE.set("p1_meter_data", {"active_power_w": 100, "monthly_power_peak_w": 2500})

        class RecordingMediator:
            def run_system_mediation_logic(self):
                self.first_read = GLOBAL_APP_STATE.get("p1_meter_data")["active_power_w"]
                GLOBAL_APP_STATE.set("p1_meter_data", {"active_power_w": 900, "monthly_power_peak_w": 2500})
                self.second_read = GLOBAL_APP_STATE.get("p1_meter_data")["active_power_w"]

        mediator = RecordingMediator()

        scheduled_tasks.task_system_mediator(mediator, {}, MagicMock(), MagicMock())

        self.assertEqual(100, mediator.first_read)
        self.assertEqual(100, mediator.second_read)
        self.assertEqual(900, GLOBAL_APP_STATE.get("p1_meter_data")["active_power_w"])

    def test_rolling_average_sample_deques_survive_copied_app_state_reads(self):
        first_ts = datetime.now(pytz.UTC)
        second_ts = first_ts + pd.Timedelta(seconds=30)
        GLOBAL_APP_STATE.set("p1_meter_data", {
            "timestamp_utc_iso": first_ts.isoformat(),
            "total_power_import_kwh": 10.0,
            "total_power_export_kwh": 1.0,
        })

        data_processors.update_rolling_averages()

        GLOBAL_APP_STATE.set("p1_meter_data", {
            "timestamp_utc_iso": second_ts.isoformat(),
            "total_power_import_kwh": 10.1,
            "total_power_export_kwh": 1.02,
        })
        data_processors.update_rolling_averages()

        self.assertEqual(2, len(GLOBAL_APP_STATE.get("recent_p1_import_kwh_samples")))
        self.assertEqual(2, len(GLOBAL_APP_STATE.get("recent_p1_export_kwh_samples")))
        self.assertEqual(12000.0, GLOBAL_APP_STATE.get("average_grid_import_watts")["60s"])
        self.assertEqual(2400.0, GLOBAL_APP_STATE.get("average_grid_export_watts")["60s"])

    def _seed_existing_prediction_plan(self):
        now_utc = datetime.now(pytz.UTC)
        local_tz = pytz.timezone("Europe/Brussels")
        today_date_str = now_utc.astimezone(local_tz).strftime("%Y-%m-%d")
        base_plan = pd.DataFrame({"cons_kwh": [0.1]}, index=pd.DatetimeIndex([now_utc]))

        GLOBAL_APP_STATE.set("plan_generation_date", today_date_str)
        GLOBAL_APP_STATE.set("prediction_plan_df", base_plan)
        GLOBAL_APP_STATE.set("p1_meter_data", {"monthly_power_peak_w": 2500})
        GLOBAL_APP_STATE.set("sunrise", None)

    def test_battery_predictor_treats_zero_soc_as_valid_and_uses_solar_average_key(self):
        self._seed_existing_prediction_plan()
        GLOBAL_APP_STATE.set("battery_records", [{"state_of_charge_pct": 0}])
        GLOBAL_APP_STATE.set("average_grid_import_watts", {"60s": 900})
        GLOBAL_APP_STATE.set("average_grid_export_watts", {"60s": 100})
        GLOBAL_APP_STATE.set("average_solar_production_watts", {"60s": 500})
        GLOBAL_APP_STATE.set("evcc_loadpoint_state", {"charge_current": 2})

        class CapturingBatteryPredictor:
            instances = []

            def __init__(self, app_config):
                self.calls = []
                CapturingBatteryPredictor.instances.append(self)

            def optimize_plan(self, base_plan_df, cur_dt, actual_soc_pct, state, app_config, db_handler,
                              cur_solar_w, cur_cons_w):
                self.calls.append({
                    "actual_soc_pct": actual_soc_pct,
                    "cur_solar_w": cur_solar_w,
                    "cur_cons_w": cur_cons_w,
                })
                return pd.DataFrame(
                    {"block_c": [False], "new_pct": [actual_soc_pct]},
                    index=pd.DatetimeIndex([cur_dt]),
                )

        with patch("hec.logic_engine.scheduled_tasks.BatteryPredictor", CapturingBatteryPredictor):
            scheduled_tasks.task_run_battery_predictor({}, MagicMock())

        call = CapturingBatteryPredictor.instances[0].calls[0]
        self.assertEqual(0.0, call["actual_soc_pct"])
        self.assertEqual(500, call["cur_solar_w"])
        self.assertEqual(840, call["cur_cons_w"])

    def test_battery_predictor_skips_when_soc_is_missing(self):
        self._seed_existing_prediction_plan()
        GLOBAL_APP_STATE.set("battery_records", [{"state_of_charge_pct": None}])

        class CapturingBatteryPredictor:
            instances = []

            def __init__(self, app_config):
                self.calls = []
                CapturingBatteryPredictor.instances.append(self)

            def optimize_plan(self, *args, **kwargs):
                self.calls.append((args, kwargs))
                raise AssertionError("optimize_plan should not run without SOC")

        with (
            patch("hec.logic_engine.scheduled_tasks.BatteryPredictor", CapturingBatteryPredictor),
            self.assertLogs("hec.logic_engine.scheduled_tasks", level="WARNING") as captured,
        ):
            scheduled_tasks.task_run_battery_predictor({}, MagicMock())

        self.assertFalse(CapturingBatteryPredictor.instances[0].calls)
        self.assertIn("No battery SOC found", "\n".join(captured.output))


if __name__ == "__main__":
    unittest.main()
