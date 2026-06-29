import copy
import unittest
from unittest.mock import MagicMock, patch

from hec.core import constants as c
from hec.core.app_state import AppState, GLOBAL_APP_STATE
from hec.logic_engine import scheduled_tasks


class TestScheduledTasksPhase5Restart(unittest.TestCase):
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

    def test_restart_request_is_left_for_runtime_without_self_signaling(self):
        GLOBAL_APP_STATE.set("reboot_request", True)
        GLOBAL_APP_STATE.set("prediction_plan", [])
        mediator = MagicMock()

        with patch("os.kill") as kill_process:
            scheduled_tasks.task_system_mediator(mediator, {}, MagicMock(), MagicMock())

        kill_process.assert_not_called()
        mediator.run_system_mediation_logic.assert_not_called()
        self.assertTrue(GLOBAL_APP_STATE.get("reboot_request"))
        self.assertEqual("requested", GLOBAL_APP_STATE.get("restart_status"))


class TestApplicationRuntimePhase5(unittest.TestCase):
    def test_runtime_starts_resources_and_shutdown_closes_them(self):
        from hec.core.runtime import ApplicationRuntime

        app_state = AppState()
        db_handler = MagicMock()
        scheduler = MagicMock()
        scheduler.running = True
        scheduler.is_alive.return_value = True
        initializer = MagicMock()
        initializer.initialize.return_value = {
            "db_handler": db_handler,
            "scheduler": scheduler,
        }

        runtime = ApplicationRuntime(
            {"api_server": {"enabled": False}, "scheduler": {"run_in_background": True}},
            app_state=app_state,
            initializer=initializer,
        )

        runtime.start()
        runtime.shutdown(c.AppStatus.SHUTDOWN)

        initializer.initialize.assert_called_once()
        scheduler.start.assert_called_once()
        scheduler.shutdown.assert_called_once()
        db_handler.close_connection.assert_called_once()
        self.assertIs(c.AppStatus.SHUTDOWN, app_state.get("app_state"))

    def test_runtime_returns_configured_exit_code_for_supervised_restart(self):
        from hec.core.runtime import ApplicationRuntime, RuntimeExitReason

        app_state = AppState()
        scheduler = MagicMock()
        scheduler.running = True
        initializer = MagicMock()
        initializer.initialize.return_value = {
            "db_handler": MagicMock(),
            "scheduler": scheduler,
        }
        sleep_calls = []

        def request_restart_after_first_loop(_seconds):
            sleep_calls.append(_seconds)
            app_state.set("reboot_request", True)

        runtime = ApplicationRuntime(
            {
                "api_server": {"enabled": False},
                "scheduler": {"run_in_background": True},
                "runtime": {
                    "restart_strategy": "supervised_process",
                    "restart_exit_code": 23,
                    "main_loop_sleep_seconds": 0.01,
                },
            },
            app_state=app_state,
            initializer=initializer,
            sleep_func=request_restart_after_first_loop,
        )

        exit_code = runtime.run()

        self.assertEqual(23, exit_code)
        self.assertIs(RuntimeExitReason.RESTART_REQUESTED, runtime.exit_reason)
        self.assertEqual("stopping", app_state.get("restart_status"))
        self.assertTrue(sleep_calls)


if __name__ == "__main__":
    unittest.main()
