import io
import unittest
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.models import NetElectricityPriceInterval, PricePoint
from hec.core.tariff_manager import TariffManager
from hec.core import api_server
from hec.logic_engine.cost_calculator import (
    calculate_net_intervals_for_day,
    calculate_total_costs_for_period,
)
from hec.logic_engine import scheduled_tasks
from hec.logic_engine.price_predictor import EnergyPricePredictor
from hec.reporting.daily_summary import DailySummaryGenerator
from hec.reporting.plot_generator import generate_future_price_plot


def write_tariffs(path: Path, fixed_buy_price: float):
    path.write_text(
        f"""
contract_types:
  - fixed
  - dynamic
active_contract:
  - start_date: "2024-01-01"
    value: dynamic
energy_supplier:
  fixed:
    buy_price_per_kwh:
      - start_date: "2024-01-01"
        value: {fixed_buy_price}
    sell_price_per_kwh:
      - start_date: "2024-01-01"
        value: 0.04
    green_certificate_fee_per_kwh:
      - start_date: "2024-01-01"
        value: 0.0
    chp_certificate_fee_per_kwh:
      - start_date: "2024-01-01"
        value: 0.0
  dynamic:
    spot_buy_multiplier:
      - start_date: "2024-01-01"
        value: 1.0
    spot_buy_fixed_fee_per_kwh:
      - start_date: "2024-01-01"
        value: 0.0
    spot_sell_multiplier:
      - start_date: "2024-01-01"
        value: 1.0
    spot_sell_fixed_fee_per_kwh:
      - start_date: "2024-01-01"
        value: 0.0
    green_certificate_fee_per_kwh:
      - start_date: "2024-01-01"
        value: 0.0
    chp_certificate_fee_per_kwh:
      - start_date: "2024-01-01"
        value: 0.0
    subscription_cost:
      - start_date: "2024-01-01"
        value: 0.0
grid_operator:
  grid_usage_fee_per_kwh:
    - start_date: "2024-01-01"
      value: 0.0
  data_management:
    - start_date: "2024-01-01"
      value: 0.0
  capacity_tariff_minimum_kw:
    - start_date: "2024-01-01"
      value: 2.5
  capacity_tariff_per_kw_per_year:
    - start_date: "2024-01-01"
      value: 0.0
government:
  energy_contribution_per_kwh:
    - start_date: "2024-01-01"
      value: 0.0
  rate_per_kwh_below:
    - start_date: "2024-01-01"
      value: 0.0
  rate_per_kwh_above:
    - start_date: "2024-01-01"
      value: 0.0
  excise_duty_tiers:
    - start_date: "2024-01-01"
      value: 1000000
  federal_contribution_fund_per_kwh:
    - start_date: "2024-01-01"
      value: 0.0
  vat:
    - start_date: "2024-01-01"
      value: 1.0
""".lstrip(),
        encoding="utf-8",
    )


class RecordingTariffManager:
    def __init__(self):
        self.requested_dates = []

    def get_all_tariffs(self, target_date):
        self.requested_dates.append(target_date)
        return {
            "active_contract": "dynamic",
            "energy_supplier": {
                "fixed": {
                    "buy_price_per_kwh": 0.30,
                    "sell_price_per_kwh": 0.04,
                    "green_certificate_fee_per_kwh": 0.0,
                    "chp_certificate_fee_per_kwh": 0.0,
                },
                "dynamic": {
                    "spot_buy_multiplier": 1.0,
                    "spot_buy_fixed_fee_per_kwh": 0.0,
                    "spot_sell_multiplier": 1.0,
                    "spot_sell_fixed_fee_per_kwh": 0.0,
                    "green_certificate_fee_per_kwh": 0.0,
                    "chp_certificate_fee_per_kwh": 0.0,
                    "subscription_cost": 0.0,
                },
            },
            "grid_operator": {
                "grid_usage_fee_per_kwh": 0.0,
                "data_management": 0.0,
                "capacity_tariff_minimum_kw": 2.5,
                "capacity_tariff_per_kw_per_year": 0.0,
            },
            "government": {
                "energy_contribution_per_kwh": 0.0,
                "rate_per_kwh_below": 0.0,
                "rate_per_kwh_above": 0.0,
                "excise_duty_tiers": 1000000,
                "federal_contribution_fund_per_kwh": 0.0,
                "vat": 1.0,
            },
        }


def make_price_interval(target_datetime):
    return NetElectricityPriceInterval(
        interval_start_local=target_datetime,
        resolution_minutes=60,
        active_contract_type="dynamic",
        net_prices_eur_per_kwh={
            "dynamic": {"buy": 0.20, "sell": 0.05},
            "fixed": {"buy": 0.30, "sell": 0.04},
        },
    )


def make_costs():
    return {
        "active_contract": "dynamic",
        "total_kwh_imported": 1.0,
        "total_kwh_exported": 0.0,
        "dynamic": {
            "total_cost_excl_rev": 0.2,
            "energy_revenue_export": 0.0,
            "total_bill": 0.2,
        },
        "fixed": {
            "total_cost_excl_rev": 0.3,
            "energy_revenue_export": 0.0,
            "total_bill": 0.3,
        },
    }


def make_savings():
    return {
        "total_avoided_cost_eur": 0.0,
        "total_export_kwh": 0.0,
        "total_opportunity_cost_eur": 0.0,
        "total_import_kwh": 0.0,
        "total_savings_eur": 0.0,
    }


class UntrainedPricePredictor:
    def __init__(self, db_handler, app_config=None):
        self.is_trained = False

    def train_model(self, *args, **kwargs):
        raise AssertionError("Daily summary must not train the price model")

    def predict_prices_for_day(self, *args, **kwargs):
        raise AssertionError("Daily summary must use cached predictions")


class TestPhase4TariffsAndCosts(unittest.TestCase):
    def setUp(self):
        self.scratch_root = Path.cwd() / "_scratch"
        self.scratch_root.mkdir(exist_ok=True)

    def test_tariff_manager_reloads_tariffs_after_daily_staleness_window(self):
        tariff_path = self.scratch_root / "phase4-tariffs.yaml"
        write_tariffs(tariff_path, fixed_buy_price=0.11)
        config = {"application": {"tariffs_file_name": str(tariff_path)}}
        manager = TariffManager(config, reload_interval=timedelta(days=1))

        first = manager.get_all_tariffs(date(2026, 6, 1))
        self.assertEqual(0.11, first["energy_supplier"]["fixed"]["buy_price_per_kwh"])

        write_tariffs(tariff_path, fixed_buy_price=0.22)
        manager._last_loaded_at_utc = datetime.now(timezone.utc) - timedelta(days=1, seconds=1)

        reloaded = manager.get_all_tariffs(date(2026, 6, 1))

        self.assertEqual(0.22, reloaded["energy_supplier"]["fixed"]["buy_price_per_kwh"])

    def test_net_interval_calculation_uses_passed_tariff_manager_without_reparsing_file(self):
        db_handler = MagicMock()
        tariff_manager = RecordingTariffManager()
        target_datetime = datetime(2026, 6, 1, tzinfo=timezone.utc)
        price_points = [
            PricePoint(
                timestamp_utc=target_datetime,
                price_eur_per_mwh=100,
                position=1,
                resolution_minutes=60,
            )
        ]

        with patch(
            "hec.logic_engine.cost_calculator.initialize_tariff_manager",
            side_effect=AssertionError("should not parse tariffs inside interval calculation"),
        ):
            intervals = calculate_net_intervals_for_day(
                db_handler,
                {},
                target_datetime,
                price_points,
                tariff_manager=tariff_manager,
            )

        self.assertEqual([date(2026, 6, 1)], tariff_manager.requested_dates)
        self.assertEqual(0.30, intervals[0].net_prices_eur_per_kwh["fixed"]["buy"])
        self.assertEqual(0.10, intervals[0].net_prices_eur_per_kwh["dynamic"]["buy"])

    def test_period_cost_calculation_uses_each_day_for_tariff_lookup(self):
        db_handler = MagicMock()
        db_handler.get_energy_deltas_for_intervals.return_value = {}
        db_handler.get_avg_monthly_peak_w_last_12m.return_value = None
        tariff_manager = RecordingTariffManager()
        start = date(2026, 6, 1)
        end = date(2026, 6, 2)

        with patch(
            "hec.logic_engine.cost_calculator.calculate_net_intervals_for_day",
            side_effect=lambda _db, _cfg, target_dt, **_kwargs: [make_price_interval(target_dt)],
        ):
            calculate_total_costs_for_period(start, end, {}, db_handler, tariff_manager)

        self.assertEqual([start, end], tariff_manager.requested_dates)


class TestPhase4PredictionAndSummary(unittest.TestCase):
    def setUp(self):
        self.previous_today = GLOBAL_APP_STATE.get("electricity_prices_today")
        self.previous_tomorrow = GLOBAL_APP_STATE.get("electricity_prices_tomorrow")
        now = datetime.now().astimezone()
        prices = {
            "dynamic": {"buy": 0.20, "sell": 0.05},
            "fixed": {"buy": 0.30, "sell": 0.04},
        }
        GLOBAL_APP_STATE.set(
            "electricity_prices_today",
            [
                NetElectricityPriceInterval(
                    now.replace(hour=hour, minute=0, second=0, microsecond=0),
                    60,
                    "dynamic",
                    prices,
                )
                for hour in range(24)
            ],
        )
        tomorrow = now + timedelta(days=1)
        GLOBAL_APP_STATE.set(
            "electricity_prices_tomorrow",
            [
                NetElectricityPriceInterval(
                    tomorrow.replace(hour=hour, minute=0, second=0, microsecond=0),
                    60,
                    "dynamic",
                    prices,
                )
                for hour in range(24)
            ],
        )

    def tearDown(self):
        GLOBAL_APP_STATE.set("electricity_prices_today", self.previous_today)
        GLOBAL_APP_STATE.set("electricity_prices_tomorrow", self.previous_tomorrow)

    def test_price_predictor_defaults_to_bounded_single_core_training_window(self):
        predictor = EnergyPricePredictor(
            MagicMock(),
            {
                "price_prediction": {
                    "training_window_days": 400,
                    "random_forest_estimators": 17,
                    "random_forest_n_jobs": 1,
                }
            },
        )

        bounded_start, bounded_end = predictor.resolve_training_window(
            date(2024, 1, 1),
            date(2026, 6, 29),
        )

        self.assertEqual(date(2025, 5, 26), bounded_start)
        self.assertEqual(date(2026, 6, 29), bounded_end)
        self.assertEqual(17, predictor.model.n_estimators)
        self.assertEqual(1, predictor.model.n_jobs)

    @patch("hec.reporting.daily_summary.EnergyPricePredictor", UntrainedPricePredictor)
    @patch("hec.reporting.daily_summary.send_email_with_attachments", return_value=True)
    @patch("hec.reporting.daily_summary.calculate_battery_saving_for_period", return_value=make_savings())
    @patch("hec.reporting.daily_summary.calculate_total_costs_for_period", return_value=make_costs())
    @patch("hec.reporting.daily_summary.generate_price_solar_plot", return_value=io.BytesIO(b"plot"))
    def test_daily_summary_uses_cached_four_day_predictions_without_training(
            self,
            _price_plot,
            _costs,
            _savings,
            _send_email,
    ):
        db_handler = MagicMock()
        db_handler.get_elia_forecasts.return_value = []

        def cached_predictions_for(target_date):
            timestamp = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
            return [
                {
                    "timestamp_utc": timestamp.isoformat(),
                    "predicted_gross_price_kwh": 0.10,
                    "solar_factor": 0.25,
                    "wind_factor": 0.50,
                    "grid_load_mwh": 100.0,
                }
            ]

        db_handler.get_predicted_prices_for_date.side_effect = cached_predictions_for
        captured_future_dfs = []

        def fake_future_plot(_db, _app_config, future_dfs, _future_date, _inverter_kw):
            captured_future_dfs.extend(future_dfs)
            return io.BytesIO(b"future")

        app_config = {
            "historic_data": {"start_date": "2025-01-01"},
            "inverter": {"standard_power_limit": 7000, "panel_peak_w": 5000},
            "smtp": {
                "sender_email": "sender@example.invalid",
                "default_recipients": ["recipient@example.invalid"],
            },
        }
        generator = DailySummaryGenerator(app_config, db_handler, MagicMock(), {"solar": []})

        with patch("hec.reporting.daily_summary.generate_future_price_plot", side_effect=fake_future_plot):
            self.assertTrue(generator.generate_and_send_summary(app_config))

        self.assertEqual(4, db_handler.get_predicted_prices_for_date.call_count)
        self.assertEqual(4, len(captured_future_dfs))

    def test_future_price_plot_title_and_day_markers_follow_four_cached_days(self):
        start_date = date(2026, 7, 1)
        future_dfs = []
        for day_offset in range(4):
            day_start = datetime.combine(start_date + timedelta(days=day_offset), time.min, tzinfo=timezone.utc)
            future_dfs.append(pd.DataFrame({
                "timestamp_utc": [day_start + timedelta(hours=i) for i in range(4)],
                "predicted_gross_price_kwh": [0.10, 0.12, 0.11, 0.09],
                "solar_factor": [0.0, 0.2, 0.4, 0.1],
                "wind_factor": [0.4, 0.3, 0.2, 0.1],
                "grid_load_mwh": [100.0, 110.0, 90.0, 95.0],
            }))

        def intervals_from_price_points(_db, _app_config, _target_date, price_points, **_kwargs):
            return [
                NetElectricityPriceInterval(
                    interval_start_local=price_point.timestamp_utc,
                    resolution_minutes=15,
                    active_contract_type="dynamic",
                    net_prices_eur_per_kwh={
                        "dynamic": {"buy": 0.20, "sell": 0.05},
                        "fixed": {"buy": 0.30, "sell": 0.04},
                    },
                )
                for price_point in price_points
            ]

        with (
            patch(
                "hec.reporting.plot_generator.calculate_net_intervals_for_day",
                side_effect=intervals_from_price_points,
            ),
            patch("hec.reporting.plot_generator.plt.title") as title_mock,
        ):
            plot_buffer = generate_future_price_plot(MagicMock(), {}, future_dfs, start_date, inverter_kw=7.0)

        self.assertIsNotNone(plot_buffer)
        title = title_mock.call_args.args[0]
        self.assertIn("04-07-2026", title)
        self.assertNotIn("05-07-2026", title)

    def test_prediction_cache_refresh_trains_when_needed_and_predicts_four_days_only(self):
        db_handler = MagicMock()
        db_handler.get_elia_forecasts.return_value = []
        instances = []

        class FakeScheduledPredictor:
            def __init__(self, db_handler_arg, app_config_arg):
                self.db_handler = db_handler_arg
                self.app_config = app_config_arg
                self.is_trained = False
                self.train_calls = []
                self.predicted_dates = []
                instances.append(self)

            def needs_training(self):
                return True

            def train_model(self, train_start, train_end):
                self.train_calls.append((train_start, train_end))
                self.is_trained = True

            def predict_prices_for_day(self, predict_date, daily_elia_fc_for_predictor):
                self.predicted_dates.append(predict_date)
                return pd.DataFrame({
                    "timestamp_utc": [datetime.combine(predict_date, time.min, tzinfo=timezone.utc)],
                    "predicted_gross_price_kwh": [0.10],
                    "solar_factor": [0.1],
                    "wind_factor": [0.2],
                    "grid_load_mwh": [100.0],
                })

        app_config = {"historic_data": {"start_date": "2024-01-01"}}

        with patch("hec.logic_engine.scheduled_tasks.EnergyPricePredictor", FakeScheduledPredictor):
            refreshed_dates = scheduled_tasks.task_refresh_price_predictions(app_config, db_handler)

        predictor = instances[0]
        tomorrow = datetime.now().astimezone().date() + timedelta(days=1)
        self.assertEqual([(date(2024, 1, 1), tomorrow - timedelta(days=2))], predictor.train_calls)
        self.assertEqual([tomorrow + timedelta(days=i) for i in range(4)], predictor.predicted_dates)
        self.assertEqual(predictor.predicted_dates, refreshed_dates)

    def test_tariff_reload_task_forces_tariff_manager_reload(self):
        tariff_manager = MagicMock()

        scheduled_tasks.task_reload_tariffs(tariff_manager)

        tariff_manager.reload_if_stale.assert_called_once_with(force=True)

    def test_manual_summary_request_starts_background_job_without_blocking_mediator(self):
        previous_values = GLOBAL_APP_STATE.current_values.copy()
        previous_prediction_plan_df = GLOBAL_APP_STATE.prediction_plan_df
        previous_summary_thread = scheduled_tasks._summary_job_thread
        try:
            scheduled_tasks._summary_job_thread = None
            GLOBAL_APP_STATE.current_values["summary_request"] = True
            GLOBAL_APP_STATE.current_values["prediction_plan"] = []

            class RecordingMediator:
                def __init__(self):
                    self.ran = False

                def run_system_mediation_logic(self):
                    self.ran = True

            class FakeThread:
                def __init__(self, target, args=(), kwargs=None, daemon=None, name=None):
                    self.target = target
                    self.args = args
                    self.kwargs = kwargs or {}
                    self.daemon = daemon
                    self.name = name
                    self.started = False

                def is_alive(self):
                    return False

                def start(self):
                    self.started = True

            mediator = RecordingMediator()
            with (
                patch("hec.logic_engine.scheduled_tasks.threading.Thread", FakeThread, create=True),
                patch("hec.logic_engine.scheduled_tasks.task_send_daily_energy_summary_email") as send_summary,
            ):
                scheduled_tasks.task_system_mediator(mediator, {}, MagicMock(), MagicMock())

            send_summary.assert_not_called()
            self.assertFalse(GLOBAL_APP_STATE.get("summary_request"))
            self.assertTrue(mediator.ran)
            self.assertEqual("queued", GLOBAL_APP_STATE.get("summary_job_status")["state"])
        finally:
            GLOBAL_APP_STATE.current_values = previous_values
            GLOBAL_APP_STATE.prediction_plan_df = previous_prediction_plan_df
            scheduled_tasks._summary_job_thread = previous_summary_thread

    def test_dashboard_exposes_daily_summary_job_status(self):
        api_server.configure_api_security({"api_server": {"auth": {"enabled": False}}})
        client = api_server.api_app.test_client()

        response = client.get("/")
        try:
            html = response.get_data(as_text=True)
        finally:
            response.close()

        self.assertIn("summaryJobStatusText", html)
        self.assertIn("summary_job_status", html)


if __name__ == "__main__":
    unittest.main()
