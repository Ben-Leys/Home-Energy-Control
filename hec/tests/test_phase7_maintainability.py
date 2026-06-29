import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

from hec.core import api_server
from hec.core.app_state import AppState, GLOBAL_APP_STATE
from hec.core import constants as c
from hec.logic_engine import scheduled_tasks


def valid_config():
    return {
        "application": {
            "log_level": "INFO",
            "tariffs_file_name": "tariffs.yaml",
            "log_to_file": False,
        },
        "database": {
            "path": "home_energy.db",
            "busy_timeout_ms": 1000,
            "history_retention_days": 1095,
            "log_retention_hours": 72,
        },
        "scheduler": {
            "timezone": "Europe/Brussels",
            "thread_pool_max_workers": 4,
            "run_in_background": True,
        },
        "historic_data": {
            "start_date": "2026-01-01",
        },
        "location": {
            "city": "Putte",
            "region_name_for_astral_optional": "Belgium",
            "timezone": "Europe/Brussels",
            "latitude": 51.05483,
            "longitude": 4.62877,
        },
        "api_server": {
            "enabled": True,
            "host": "127.0.0.1",
            "port": 8123,
            "auth": {
                "enabled": False,
                "cookie_max_age_days": 30,
                "csrf_enabled": True,
                "same_origin_enabled": True,
            },
        },
        "runtime": {
            "restart_strategy": "supervised_process",
            "restart_exit_code": 75,
            "main_loop_sleep_seconds": 1,
        },
        "http": {
            "default_timeout_seconds": 10,
            "retries": 2,
            "backoff_factor": 0.2,
            "verify_tls": True,
        },
    }


class TestPhase7ConfigValidation(unittest.TestCase):
    def test_validate_app_config_returns_typed_config_for_required_sections(self):
        from hec.core.config_schema import validate_app_config

        typed_config = validate_app_config(valid_config())

        self.assertEqual("home_energy.db", typed_config.database.path)
        self.assertEqual("Europe/Brussels", typed_config.scheduler.timezone)
        self.assertEqual(8123, typed_config.api_server.port)
        self.assertEqual(10, typed_config.http.default_timeout_seconds)

    def test_validate_app_config_rejects_missing_database_path_and_bad_timezone(self):
        from hec.core.config_schema import ConfigValidationError, validate_app_config

        missing_db_path = valid_config()
        missing_db_path["database"].pop("path")
        with self.assertRaisesRegex(ConfigValidationError, "database.path"):
            validate_app_config(missing_db_path)

        bad_timezone = valid_config()
        bad_timezone["scheduler"]["timezone"] = "Mars/Base"
        with self.assertRaisesRegex(ConfigValidationError, "scheduler.timezone"):
            validate_app_config(bad_timezone)


class TestPhase7HttpAndTimezoneUtilities(unittest.TestCase):
    def test_http_client_applies_default_timeout_and_tls_policy(self):
        from hec.utils.http_client import HttpClient

        class FakeSession:
            def __init__(self):
                self.calls = []
                self.mounts = []

            def mount(self, prefix, adapter):
                self.mounts.append((prefix, adapter))

            def request(self, method, url, **kwargs):
                self.calls.append((method, url, kwargs))
                response = MagicMock()
                response.status_code = 200
                return response

        session = FakeSession()
        client = HttpClient(default_timeout_seconds=7, retries=2, verify_tls=False, session=session)

        response = client.get("https://example.test/api")

        self.assertEqual(200, response.status_code)
        self.assertEqual(("GET", "https://example.test/api"), session.calls[0][:2])
        self.assertEqual(7, session.calls[0][2]["timeout"])
        self.assertFalse(session.calls[0][2]["verify"])
        self.assertEqual(["https://", "http://"], [prefix for prefix, _ in session.mounts])

    def test_local_day_bounds_handle_dst_length_in_utc(self):
        from hec.utils.time_utils import local_day_bounds

        spring_start, spring_end = local_day_bounds(date(2026, 3, 29), "Europe/Brussels")
        fall_start, fall_end = local_day_bounds(date(2026, 10, 25), "Europe/Brussels")

        self.assertEqual(23 * 3600, (spring_end - spring_start).total_seconds())
        self.assertEqual(25 * 3600, (fall_end - fall_start).total_seconds())


class TestPhase7HomeWizardBatteryGateway(unittest.TestCase):
    def test_homewizard_battery_gateway_owns_group_reads_and_commands(self):
        from hec.controllers.homewizard_battery_gateway import HomeWizardBatteryGateway
        from hec.data_sources.api_p1_meter_homewizard import P1MeterHomewizardClient

        class FakeResponse:
            status_code = 200
            reason = "OK"
            text = "{}"

            def __init__(self, payload=None):
                self.payload = payload or {}

            def raise_for_status(self):
                return None

            def json(self):
                return self.payload

        class FakeHttpClient:
            def __init__(self):
                self.calls = []

            def get(self, url, **kwargs):
                self.calls.append(("GET", url, kwargs))
                return FakeResponse({"mode": "zero", "permissions": ["charge_allowed"]})

            def put(self, url, **kwargs):
                self.calls.append(("PUT", url, kwargs))
                return FakeResponse()

        http = FakeHttpClient()
        gateway = HomeWizardBatteryGateway(
            host="battery-gateway.local",
            token="secret-token",
            http_client=http,
            verify_tls=False,
        )

        self.assertTrue(gateway.is_initialized)
        self.assertFalse(hasattr(P1MeterHomewizardClient, "set_battery_mode"))
        self.assertEqual("zero", gateway.refresh_group_data()["mode"])

        self.assertTrue(gateway.set_battery_mode(c.BatteryState.BATTERY_BLOCK_CHARGE))
        put_call = [call for call in http.calls if call[0] == "PUT"][0]
        self.assertEqual({"mode": "zero", "permissions": ["discharge_allowed"]}, put_call[2]["json"])
        self.assertFalse(put_call[2]["verify"])

    def test_p1_poll_uses_battery_gateway_without_battery_methods_on_p1_client(self):
        original_values = GLOBAL_APP_STATE.current_values
        original_prediction_plan_df = GLOBAL_APP_STATE.prediction_plan_df
        original_db_handler = GLOBAL_APP_STATE.db_handler
        try:
            GLOBAL_APP_STATE.current_values = AppState().current_values.copy()
            GLOBAL_APP_STATE.prediction_plan_df = None
            GLOBAL_APP_STATE.db_handler = None

            p1_client = MagicMock()
            p1_client.is_initialized = True
            p1_client.refresh_meter_data.return_value = {
                "timestamp_utc_iso": "2026-06-29T12:00:00+00:00",
                "active_power_w": 125,
                "active_power_average_w": 100,
                "total_power_import_kwh": 10.0,
                "total_power_export_kwh": 1.0,
                "montly_power_peak_w": 2400,
                "montly_power_peak_timestamp": "2026-06-29T11:45:00+00:00",
            }
            del p1_client.refresh_batteries_data

            battery_gateway = MagicMock()
            battery_gateway.refresh_group_data.return_value = {"mode": "zero", "permissions": []}

            db_handler = MagicMock()
            db_handler.store_p1_meter_data.return_value = True

            scheduled_tasks.task_poll_p1_meter(db_handler, p1_client, battery_gateway=battery_gateway, boundary=15)

            battery_gateway.refresh_group_data.assert_called_once()
            self.assertEqual({"mode": "zero", "permissions": []}, GLOBAL_APP_STATE.get("battery_data"))
            self.assertEqual(125, GLOBAL_APP_STATE.get("p1_meter_data")["active_power_w"])
        finally:
            GLOBAL_APP_STATE.current_values = original_values
            GLOBAL_APP_STATE.prediction_plan_df = original_prediction_plan_df
            GLOBAL_APP_STATE.db_handler = original_db_handler


class TestPhase7DashboardAndProjectArtifacts(unittest.TestCase):
    def test_dashboard_uses_pinned_vue_csp_and_accessible_tabs(self):
        api_server.configure_api_security({"api_server": {"auth": {"enabled": False}}})
        client = api_server.api_app.test_client()

        response = client.get("/")
        try:
            html = response.get_data(as_text=True)
            self.assertIn("Content-Security-Policy", response.headers)
            self.assertIn("vue@3.5.16/dist/vue.global.prod.js", html)
            self.assertNotIn("vue@3/dist/vue.global.js", html)
            self.assertIn('role="tablist"', html)
            self.assertIn('role="tab"', html)
            self.assertIn('aria-selected', html)
            self.assertIn('@keydown', html)
        finally:
            response.close()

    def test_streamlit_dashboard_and_dependencies_are_removed(self):
        root = Path.cwd()

        self.assertFalse((root / "hec" / "ui" / "hec_dashboard.py").exists())
        requirements = (root / "requirements.txt").read_text(encoding="utf-8")
        self.assertNotIn("streamlit", requirements.lower())
        self.assertNotIn("bokeh", requirements.lower())
        self.assertNotIn("param", requirements.lower())

    def test_ci_docs_license_and_lint_configuration_exist(self):
        root = Path.cwd()

        expected_paths = [
            ".github/workflows/ci.yml",
            "pyproject.toml",
            "requirements-dev.txt",
            "docs/nas-deployment.md",
            "docs/device-configuration.md",
            "docs/troubleshooting.md",
            "docs/dependency-management.md",
            "docs/privacy-data-retention.md",
            "LICENSE",
        ]

        for relative_path in expected_paths:
            with self.subTest(relative_path=relative_path):
                self.assertTrue((root / relative_path).exists())


if __name__ == "__main__":
    unittest.main()
