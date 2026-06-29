import unittest

from hec.core import constants as c
from hec.core import api_server
from hec.core.app_state import GLOBAL_APP_STATE


class TestApiPhase1Security(unittest.TestCase):
    def setUp(self):
        self.original_values = GLOBAL_APP_STATE.current_values.copy()
        self.original_db_handler = GLOBAL_APP_STATE.db_handler
        GLOBAL_APP_STATE.db_handler = None
        api_server.configure_api_security({
            "api_server": {
                "auth": {
                    "enabled": True,
                    "password": "local-password",
                    "cookie_secret": "test-cookie-secret",
                    "cookie_max_age_days": 30,
                }
            }
        })
        api_server._DB_INSTANCE = None
        self.client = api_server.api_app.test_client()

    def tearDown(self):
        GLOBAL_APP_STATE.current_values = self.original_values
        GLOBAL_APP_STATE.db_handler = self.original_db_handler
        api_server.configure_api_security({"api_server": {"auth": {"enabled": False}}})

    def login(self):
        response = self.client.post(
            "/api/v1/auth/login",
            json={"password": "local-password"},
        )
        self.assertEqual(200, response.status_code)
        body = response.get_json()
        self.assertTrue(body["success"])
        self.assertTrue(body["csrf_token"])
        return body["csrf_token"]

    def test_protected_api_routes_reject_unauthenticated_requests(self):
        state_response = self.client.get("/api/v1/state")
        logs_response = self.client.get("/api/v1/logs")
        update_response = self.client.post(
            "/api/v1/settings/update",
            json={"key": "app_operating_mode", "value": "MODE_AUTO"},
        )

        self.assertEqual(401, state_response.status_code)
        self.assertEqual(401, logs_response.status_code)
        self.assertEqual(401, update_response.status_code)

    def test_login_allows_reads_and_csrf_protects_setting_updates(self):
        csrf_token = self.login()

        state_response = self.client.get("/api/v1/state")
        self.assertEqual(200, state_response.status_code)

        missing_csrf_response = self.client.post(
            "/api/v1/settings/update",
            json={"key": "app_operating_mode", "value": "MODE_AUTO"},
        )
        self.assertEqual(403, missing_csrf_response.status_code)

        update_response = self.client.post(
            "/api/v1/settings/update",
            headers={"X-CSRF-Token": csrf_token},
            json={"key": "app_operating_mode", "value": "MODE_AUTO"},
        )
        self.assertEqual(200, update_response.status_code)
        self.assertEqual("MODE_AUTO", update_response.get_json()["new_value_stored"])
        self.assertIs(c.OperatingMode.MODE_AUTO, GLOBAL_APP_STATE.get("app_operating_mode"))

    def test_update_endpoint_rejects_non_allowlisted_app_state_keys(self):
        csrf_token = self.login()

        response = self.client.post(
            "/api/v1/settings/update",
            headers={"X-CSRF-Token": csrf_token},
            json={"key": "app_state", "value": "ALARM"},
        )

        self.assertEqual(400, response.status_code)
        self.assertIn("not allowed", response.get_json()["error"])

    def test_evcc_manual_limit_uses_real_key_and_rejects_stale_alias(self):
        csrf_token = self.login()

        stale_alias_response = self.client.post(
            "/api/v1/settings/update",
            headers={"X-CSRF-Token": csrf_token},
            json={"key": "evcc_manual_limit_amps", "value": "12"},
        )
        self.assertEqual(400, stale_alias_response.status_code)

        valid_response = self.client.post(
            "/api/v1/settings/update",
            headers={"X-CSRF-Token": csrf_token},
            json={"key": "evcc_manual_limit", "value": "12"},
        )
        self.assertEqual(200, valid_response.status_code)
        self.assertEqual(12, valid_response.get_json()["new_value_stored"])
        self.assertEqual(12, GLOBAL_APP_STATE.get("evcc_manual_limit"))

        low_response = self.client.post(
            "/api/v1/settings/update",
            headers={"X-CSRF-Token": csrf_token},
            json={"key": "evcc_manual_limit", "value": "5"},
        )
        high_response = self.client.post(
            "/api/v1/settings/update",
            headers={"X-CSRF-Token": csrf_token},
            json={"key": "evcc_manual_limit", "value": "33"},
        )
        self.assertEqual(400, low_response.status_code)
        self.assertEqual(400, high_response.status_code)

    def test_command_requests_are_audit_logged(self):
        csrf_token = self.login()

        with self.assertLogs("hec.core.api_server", level="INFO") as captured:
            summary_response = self.client.post(
                "/api/v1/settings/update",
                headers={"X-CSRF-Token": csrf_token},
                json={"key": "summary_request", "value": True},
            )
            reboot_response = self.client.post(
                "/api/v1/settings/update",
                headers={"X-CSRF-Token": csrf_token},
                json={"key": "reboot_request", "value": True},
            )

        self.assertEqual(200, summary_response.status_code)
        self.assertEqual(200, reboot_response.status_code)
        audit_output = "\n".join(captured.output)
        self.assertIn("AUDIT command_request summary_request", audit_output)
        self.assertIn("AUDIT command_request reboot_request", audit_output)

    def test_dashboard_contains_login_csrf_and_confirmed_shutdown_hooks(self):
        response = self.client.get("/")
        try:
            self.assertEqual(200, response.status_code)

            html = response.get_data(as_text=True)
            self.assertIn("/api/v1/auth/status", html)
            self.assertIn("/api/v1/auth/login", html)
            self.assertIn("/api/v1/auth/logout", html)
            self.assertIn("X-CSRF-Token", html)
            self.assertIn("Dashboard Login", html)
            self.assertIn("window.confirm", html)
            self.assertIn("requestShutdown", html)
            self.assertIn("Stop for supervised restart", html)
            self.assertNotIn("Restart system", html)
        finally:
            response.close()


if __name__ == "__main__":
    unittest.main()
