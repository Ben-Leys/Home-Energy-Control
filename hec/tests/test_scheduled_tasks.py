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

        self.addCleanup(GLOBAL_APP_STATE.set, "electricity_prices_today", self.previous_today)
        self.addCleanup(GLOBAL_APP_STATE.set, "electricity_prices_tomorrow", self.previous_tomorrow)
        self.addCleanup(GLOBAL_APP_STATE.set, "sunrise", self.previous_sunrise)
        self.addCleanup(GLOBAL_APP_STATE.set, "sunset", self.previous_sunset)

    def test_midnight_rollover_requests_sunrise_datetimes_with_keyword(self):
        db_handler = Mock()
        app_config = {"inverter": {}}
        GLOBAL_APP_STATE.set("electricity_prices_tomorrow", ["price"])

        with patch.object(scheduled_tasks, "is_daylight", return_value=(True, "sunrise", "sunset")) as daylight:
            scheduled_tasks.task_midnight_rollover(db_handler, app_config)

        daylight.assert_called_once_with(app_config, return_dt=True)
        self.assertEqual(GLOBAL_APP_STATE.get("sunrise"), "sunrise")
        self.assertEqual(GLOBAL_APP_STATE.get("sunset"), "sunset")


if __name__ == "__main__":
    unittest.main()
