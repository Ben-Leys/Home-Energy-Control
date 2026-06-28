import io
import unittest
from datetime import datetime, time, timedelta
from unittest.mock import MagicMock, patch

from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.models import NetElectricityPriceInterval
from hec.reporting.daily_summary import DailySummaryGenerator
from hec.reporting.summary_timing import DailySummaryTimingProfiler


class ManualClock:
    def __init__(self):
        self.current_time = 0.0

    def __call__(self):
        return self.current_time

    def advance(self, seconds):
        self.current_time += seconds


def make_price_intervals(target_date, tzinfo):
    prices = {
        "dynamic": {"buy": 0.20, "sell": 0.05},
        "fixed": {"buy": 0.30, "sell": 0.04},
    }
    start = datetime.combine(target_date, time.min, tzinfo=tzinfo)
    return [
        NetElectricityPriceInterval(start + timedelta(hours=hour), 60, "dynamic", prices)
        for hour in range(24)
    ]


def make_costs():
    return {
        "active_contract": "dynamic",
        "total_kwh_imported": 12.0,
        "total_kwh_exported": 4.0,
        "dynamic": {
            "total_cost_excl_rev": 3.0,
            "energy_revenue_export": 0.5,
            "total_bill": 2.5,
        },
        "fixed": {
            "total_cost_excl_rev": 4.0,
            "energy_revenue_export": 0.4,
            "total_bill": 3.6,
        },
    }


def make_savings():
    return {
        "total_avoided_cost_eur": 1.0,
        "total_export_kwh": 2.0,
        "total_opportunity_cost_eur": 0.25,
        "total_import_kwh": 0.5,
        "total_savings_eur": 0.75,
    }


class FakePricePredictor:
    def __init__(self, db_handler):
        self.is_trained = True

    def train_model(self, train_start, train_end):
        raise AssertionError("trained predictors should not retrain during this test")

    def predict_prices_for_day(self, predict_date, daily_elia_fc_for_predictor):
        return None


class TestDailySummaryTimingProfiler(unittest.TestCase):
    def test_profiler_records_steps_and_reports_top_two(self):
        clock = ManualClock()
        profiler = DailySummaryTimingProfiler(clock=clock)

        with profiler.step("data_loading"):
            clock.advance(1.25)
        with profiler.step("plotting"):
            clock.advance(2.5)
        with profiler.step("smtp"):
            clock.advance(0.5)

        top_two = profiler.top_steps(2)

        self.assertEqual([step.name for step in top_two], ["plotting", "data_loading"])
        self.assertEqual([step.elapsed_seconds for step in top_two], [2.5, 1.25])
        self.assertIn("status=success", profiler.format_report("success"))
        self.assertIn("plotting=2.500s", profiler.format_report("success"))
        self.assertIn("data_loading=1.250s", profiler.format_report("success"))


class TestDailySummaryGeneratorProfiling(unittest.TestCase):
    def setUp(self):
        self.previous_today = GLOBAL_APP_STATE.get("electricity_prices_today")
        self.previous_tomorrow = GLOBAL_APP_STATE.get("electricity_prices_tomorrow")
        now = datetime.now().astimezone()
        GLOBAL_APP_STATE.set("electricity_prices_today", make_price_intervals(now.date(), now.tzinfo))
        GLOBAL_APP_STATE.set(
            "electricity_prices_tomorrow",
            make_price_intervals((now + timedelta(days=1)).date(), now.tzinfo),
        )

    def tearDown(self):
        GLOBAL_APP_STATE.set("electricity_prices_today", self.previous_today)
        GLOBAL_APP_STATE.set("electricity_prices_tomorrow", self.previous_tomorrow)

    @patch("hec.reporting.daily_summary.EnergyPricePredictor", FakePricePredictor)
    @patch("hec.reporting.daily_summary.send_email_with_attachments", return_value=True)
    @patch("hec.reporting.daily_summary.calculate_battery_saving_for_period", return_value=make_savings())
    @patch("hec.reporting.daily_summary.calculate_total_costs_for_period", return_value=make_costs())
    @patch("hec.reporting.daily_summary.generate_future_price_plot", return_value=None)
    @patch("hec.reporting.daily_summary.generate_price_solar_plot", return_value=io.BytesIO(b"plot"))
    def test_generate_and_send_summary_records_required_timing_steps(self, *_mocks):
        profiler = DailySummaryTimingProfiler()
        db_handler = MagicMock()
        db_handler.get_elia_forecasts.return_value = []
        app_config = {
            "historic_data": {"start_date": "2025-01-01"},
            "inverter": {"standard_power_limit": 7000, "panel_peak_w": 5000},
            "smtp": {
                "sender_email": "sender@example.invalid",
                "default_recipients": ["recipient@example.invalid"],
            },
        }
        generator = DailySummaryGenerator(app_config, db_handler, MagicMock(), {"solar": []})

        self.assertTrue(generator.generate_and_send_summary(app_config, profiler=profiler))

        recorded_steps = {step.name for step in profiler.steps}
        self.assertSetEqual(
            recorded_steps,
            {
                "data_loading",
                "plotting",
                "price_model_training",
                "price_prediction",
                "cost_calculation",
                "smtp",
            },
        )


if __name__ == "__main__":
    unittest.main()
