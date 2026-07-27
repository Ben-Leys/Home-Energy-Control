import unittest
from unittest.mock import Mock, patch

from hec.core.app_state import GLOBAL_APP_STATE
from hec.logic_engine import scheduled_tasks


class TestScheduledTasks(unittest.TestCase):
    def setUp(self):
        self.previous_today = GLOBAL_APP_STATE.get("electricity_prices_today")
        self.previous_tomorrow = GLOBAL_APP_STATE.get("electricity_prices_tomorrow")
        self.previous_sunrise = GLOBAL_APP_STATE.get("sunrise")
        self.previous_sunset = GLOBAL_APP_STATE.get("sunset")
        self.previous_fetch_prices_attempt_count = scheduled_tasks.fetch_prices_attempt_count

        self.addCleanup(GLOBAL_APP_STATE.set, "electricity_prices_today", self.previous_today)
        self.addCleanup(GLOBAL_APP_STATE.set, "electricity_prices_tomorrow", self.previous_tomorrow)
        self.addCleanup(GLOBAL_APP_STATE.set, "sunrise", self.previous_sunrise)
        self.addCleanup(GLOBAL_APP_STATE.set, "sunset", self.previous_sunset)
        self.addCleanup(
            setattr,
            scheduled_tasks,
            "fetch_prices_attempt_count",
            self.previous_fetch_prices_attempt_count,
        )

    def test_midnight_rollover_requests_sunrise_datetimes_with_keyword(self):
        db_handler = Mock()
        app_config = {"inverter": {}}
        GLOBAL_APP_STATE.set("electricity_prices_tomorrow", ["price"])

        with patch.object(scheduled_tasks, "is_daylight", return_value=(True, "sunrise", "sunset")) as daylight:
            scheduled_tasks.task_midnight_rollover(db_handler, app_config)

        daylight.assert_called_once_with(app_config, return_dt=True)
        self.assertEqual(GLOBAL_APP_STATE.get("sunrise"), "sunrise")
        self.assertEqual(GLOBAL_APP_STATE.get("sunset"), "sunset")

    @patch.object(scheduled_tasks, "register_job")
    @patch.object(scheduled_tasks, "fetch_entsoe_prices", return_value=[])
    def test_empty_day_ahead_response_is_retried_without_sending_summary(
            self,
            _fetch_prices,
            register_job,
    ):
        scheduler = Mock()
        scheduled_tasks.fetch_prices_attempt_count = 0
        app_config = {
            "tasks_schedule": {
                scheduled_tasks.FETCH_PRICES_JOB_ID: {
                    "summary_email": True,
                    "max_retries": 36,
                }
            }
        }

        scheduled_tasks.task_fetch_and_store_day_ahead_prices(
            scheduler,
            Mock(),
            app_config,
            Mock(),
        )

        register_job.assert_not_called()
        scheduler.modify_job.assert_called_once()
        self.assertEqual(
            scheduler.modify_job.call_args.kwargs["next_run_time"].tzinfo,
            scheduled_tasks.timezone.utc,
        )


if __name__ == "__main__":
    unittest.main()
