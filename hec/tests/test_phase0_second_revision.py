import math
import time as time_module
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from hec.controllers.api_evcc import EvccApiClient
from hec.core import api_server
from hec.core.app_state import AppState, GLOBAL_APP_STATE
from hec.core.config_schema import validate_app_config
from hec.core.market_prices import MarketContext
from hec.core.models import EVCCOverallState, NetElectricityPriceInterval, PricePoint
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine import scheduled_tasks
from hec.reporting.daily_summary import DailySummaryGenerator
from hec.reporting import plot_generator


def _price_interval(start, minutes=15, active_contract_type="dynamic"):
    return NetElectricityPriceInterval(
        interval_start_local=start,
        resolution_minutes=minutes,
        active_contract_type=active_contract_type,
        net_prices_eur_per_kwh={
            "dynamic": {"buy": 0.20, "sell": 0.05},
            "fixed": {"buy": 0.31, "sell": 0.04},
        },
    )


def _valid_config():
    return {
        "database": {"path": "home_energy.db"},
        "scheduler": {"timezone": "Europe/Brussels"},
        "historic_data": {"start_date": "2026-01-01"},
        "inverter": {
            "location": {
                "city": "Putte",
                "region_name_for_astral_optional": "",
                "timezone": "Europe/Brussels",
                "latitude": 51.05483,
                "longitude": 4.62877,
            },
        },
    }


class AppStateRestoreMixin:
    def setUp(self):
        self.original_values = GLOBAL_APP_STATE.current_values
        self.original_prediction_plan_df = GLOBAL_APP_STATE.prediction_plan_df
        self.original_db_handler = GLOBAL_APP_STATE.db_handler
        GLOBAL_APP_STATE.current_values = AppState().current_values.copy()
        GLOBAL_APP_STATE.prediction_plan_df = None
        GLOBAL_APP_STATE.db_handler = None

    def tearDown(self):
        GLOBAL_APP_STATE.current_values = self.original_values
        GLOBAL_APP_STATE.prediction_plan_df = self.original_prediction_plan_df
        GLOBAL_APP_STATE.db_handler = self.original_db_handler


class TestPhase0MarketContext(AppStateRestoreMixin, unittest.TestCase):
    @patch("hec.core.market_prices.datetime")
    def test_fixed_contract_refresh_sets_prices_expiry_and_returns_true(self, mock_datetime):
        start = datetime(2026, 6, 30, 10, 0, tzinfo=timezone.utc)
        now = start + timedelta(minutes=5)
        mock_datetime.now.return_value = now
        GLOBAL_APP_STATE.set("electricity_prices_today", [_price_interval(start, active_contract_type="fixed")])

        market = MarketContext()

        self.assertTrue(market.refresh_if_needed())
        self.assertTrue(market.is_fixed_contract)
        self.assertEqual(0.31, market.buy_price)
        self.assertEqual(0.04, market.sell_price)
        self.assertEqual(
            (start.astimezone() + timedelta(minutes=15)).replace(second=0),
            market.next_update_at,
        )


class TestPhase0Evcc(AppStateRestoreMixin, unittest.TestCase):
    def test_set_min_current_rejects_invalid_current_without_sending_command(self):
        client = EvccApiClient.__new__(EvccApiClient)
        client.min_current = 6
        client.max_current = 32
        client._send_command = MagicMock(return_value=True)

        self.assertFalse(client.set_min_current(5))
        self.assertFalse(client.set_min_current(33))
        client._send_command.assert_not_called()

    def test_evcc_overall_state_default_timestamp_is_per_instance(self):
        first = EVCCOverallState()
        time_module.sleep(0.001)
        second = EVCCOverallState()

        self.assertNotEqual(first.timestamp_utc_iso, second.timestamp_utc_iso)

    def test_evcc_poll_handles_payload_with_no_loadpoints_as_degraded_data(self):
        evcc_client = MagicMock()
        evcc_client.get_current_state_data.return_value = {
            "timestamp_utc_iso": "2026-06-30T10:00:00+00:00",
            "residualPower": 42,
            "loadpoints": [],
        }
        db_handler = MagicMock()

        with self.assertLogs("hec.logic_engine.scheduled_tasks", level="WARNING") as captured:
            scheduled_tasks.task_poll_evcc_state(evcc_client, db_handler)

        self.assertIn("no loadpoints", "\n".join(captured.output).lower())
        self.assertEqual(42, GLOBAL_APP_STATE.get("evcc_overall_state")["residual_power"])
        self.assertIsNone(GLOBAL_APP_STATE.get("evcc_loadpoint_state"))
        db_handler.store_evcc_session.assert_not_called()

    def test_evcc_poll_handles_missing_currents_and_session_energy(self):
        evcc_client = MagicMock()
        evcc_client.get_current_state_data.return_value = {
            "timestamp_utc_iso": "2026-06-30T10:00:00+00:00",
            "residualPower": 0,
            "loadpoints": [
                {
                    "connected": True,
                    "charging": False,
                    "mode": "pv",
                }
            ],
        }
        db_handler = MagicMock()

        with self.assertLogs("hec.logic_engine.scheduled_tasks", level="WARNING") as captured:
            scheduled_tasks.task_poll_evcc_state(evcc_client, db_handler)

        self.assertIn("sessionenergy", "\n".join(captured.output).lower())
        loadpoint = GLOBAL_APP_STATE.get("evcc_loadpoint_state")
        self.assertIsNone(loadpoint["charge_current"])
        self.assertIsNone(loadpoint["session_energy"])
        db_handler.store_evcc_session.assert_not_called()


class TestPhase0DailySummaryHelpers(unittest.TestCase):
    def test_format_hours_static_helper_uses_normal_call_shape(self):
        self.assertEqual("1 - 3, 4 - 5 h", DailySummaryGenerator._format_hours([1, 2, 4]))

    def test_format_hours_summary_static_helper_uses_normal_call_shape(self):
        start = datetime(2026, 7, 1, tzinfo=timezone.utc)
        intervals = [
            _price_interval(start + timedelta(hours=hour), minutes=60)
            for hour in range(6)
        ]

        summary_html, negative_html = DailySummaryGenerator._format_hours_summary(
            intervals,
            adjusted_solar=[1.0] * 6,
            res_min=60,
            solar_income=0.30,
        )

        self.assertIn("Expensive", summary_html)
        self.assertIn("Cheap", summary_html)
        self.assertEqual("<td></td>", negative_html)

    def test_format_hours_summary_handles_negative_sell_prices_and_truncated_solar(self):
        start = datetime(2026, 7, 1, tzinfo=timezone.utc)
        intervals = [
            NetElectricityPriceInterval(
                interval_start_local=start + timedelta(hours=hour),
                resolution_minutes=60,
                active_contract_type="dynamic",
                net_prices_eur_per_kwh={
                    "dynamic": {"buy": 0.10, "sell": -0.05 if hour in (12, 13) else 0.05}
                },
            )
            for hour in range(24)
        ]

        # Truncated adjusted_solar list (e.g. only 2 entries for 24 intervals)
        summary_html, negative_html = DailySummaryGenerator._format_hours_summary(
            intervals,
            adjusted_solar=[1.0, 1.0],
            res_min=60,
            solar_income=0.0,
        )

        self.assertIn("Expensive", summary_html)
        self.assertIn("12 - 14 h", negative_html)
        self.assertIn("Shut off panels saves: € 0.00", negative_html)

    def test_format_hours_summary_computes_saved_negative_income_accurately(self):
        start = datetime(2026, 7, 1, tzinfo=timezone.utc)
        intervals = [
            NetElectricityPriceInterval(
                interval_start_local=start + timedelta(hours=hour),
                resolution_minutes=60,
                active_contract_type="dynamic",
                net_prices_eur_per_kwh={
                    "dynamic": {"buy": 0.10, "sell": -0.05 if hour in (12, 13) else 0.05}
                },
            )
            for hour in range(24)
        ]

        # 2 kWh in hour 12, 3 kWh in hour 13 -> (-(-0.05) * 2) + (-(-0.05) * 3) = 0.10 + 0.15 = 0.25
        adjusted_solar = [0.0] * 24
        adjusted_solar[12] = 2.0
        adjusted_solar[13] = 3.0

        summary_html, negative_html = DailySummaryGenerator._format_hours_summary(
            intervals,
            adjusted_solar=adjusted_solar,
            res_min=60,
            solar_income=0.50,
        )

        self.assertIn("12 - 14 h", negative_html)
        self.assertIn("Shut off panels saves: € 0.25", negative_html)

    def test_format_hours_summary_handles_empty_intervals_and_none_solar(self):
        summary_html, negative_html = DailySummaryGenerator._format_hours_summary(
            [],
            adjusted_solar=None,
            res_min=60,
            solar_income=0.0,
        )

        self.assertIn("Expensive: none", summary_html)
        self.assertIn("Cheap: none", summary_html)
        self.assertEqual("<td></td>", negative_html)



class TestPhase0PlotGeneration(unittest.TestCase):
    def tearDown(self):
        plot_generator.plt.close("all")

    def test_plot_generator_forces_agg_backend_before_pyplot_import(self):
        source = (Path.cwd() / "hec" / "reporting" / "plot_generator.py").read_text(encoding="utf-8")

        self.assertLess(
            source.index("matplotlib.use(\"Agg\")"),
            source.index("import matplotlib.pyplot as plt"),
        )

    def test_spring_dst_price_padding_uses_missing_values_not_fake_zeroes(self):
        start = datetime(2026, 3, 29, tzinfo=timezone.utc)
        intervals = [
            _price_interval(start + timedelta(minutes=15 * index))
            for index in range(92)
        ]

        buy, sell, _timestamps = plot_generator._prepare_price_data_for_plot(intervals, 96)

        self.assertEqual(96, len(buy))
        self.assertTrue(all(math.isnan(value) for value in buy[8:12]))
        self.assertTrue(all(math.isnan(value) for value in sell[8:12]))

    def test_price_solar_plot_closes_figure_when_save_fails(self):
        start = datetime(2026, 7, 1, tzinfo=timezone.utc)
        today = [_price_interval(start + timedelta(hours=hour), minutes=60) for hour in range(24)]
        tomorrow = [
            _price_interval(start + timedelta(days=1, hours=hour), minutes=60)
            for hour in range(24)
        ]

        with patch("hec.reporting.plot_generator.plt.savefig", side_effect=RuntimeError("disk full")):
            result = plot_generator.generate_price_solar_plot(
                t_date_local=(start + timedelta(days=1)).date(),
                n_date_nepi=today[-10:],
                t_date_nepi=tomorrow,
                t_date_solar=[0.2] * 24,
                fixed_buy_price=0.31,
                fixed_sell_price=0.04,
                forecast_resolution=60,
                inverter_kw=7.0,
            )

        self.assertIsNone(result)
        self.assertEqual([], plot_generator.plt.get_fignums())


class TestPhase0DashboardAndApi(unittest.TestCase):
    def test_dashboard_current_price_card_is_labelled_market_price(self):
        api_server.configure_api_security({"api_server": {"auth": {"enabled": False}}})
        client = api_server.api_app.test_client()

        response = client.get("/")
        try:
            html = response.get_data(as_text=True)
        finally:
            response.close()

        self.assertIn("Market price", html)

    def test_logs_limit_is_clamped_to_lower_and_upper_bounds(self):
        api_server.configure_api_security({"api_server": {"auth": {"enabled": False}}})
        original_db = api_server._DB_INSTANCE
        captured_limits = []

        class FakeDb:
            def get_latest_logs(self, limit):
                captured_limits.append(limit)
                return []

        api_server._DB_INSTANCE = FakeDb()
        client = api_server.api_app.test_client()
        try:
            self.assertEqual(200, client.get("/api/v1/logs?limit=-5").status_code)
            self.assertEqual(200, client.get("/api/v1/logs?limit=50000").status_code)
        finally:
            api_server._DB_INSTANCE = original_db

        self.assertEqual([1, 20000], captured_limits)


class TestPhase0ConfigValidation(unittest.TestCase):
    def test_location_is_allowed_under_inverter_without_standalone_location(self):
        typed_config = validate_app_config(_valid_config())

        self.assertEqual("home_energy.db", typed_config.database.path)


class TestPhase0DatabaseFixes(unittest.TestCase):
    def setUp(self):
        self.scratch_root = Path.cwd() / "_scratch"
        self.scratch_root.mkdir(exist_ok=True)
        self.db_path = self.scratch_root / f"phase0-{self._testMethodName}.sqlite"
        self._remove_sqlite_artifacts()
        self.handler = DatabaseHandler({"path": str(self.db_path), "busy_timeout_ms": 750})
        self.handler.initialize_database()

    def tearDown(self):
        self.handler.close_connection()
        self._remove_sqlite_artifacts()

    def _remove_sqlite_artifacts(self):
        for suffix in ("", "-journal", "-wal", "-shm"):
            (self.db_path.with_name(f"{self.db_path.name}{suffix}")).unlink(missing_ok=True)

    def test_store_da_prices_returns_number_of_inserted_or_replaced_rows(self):
        start = datetime(2026, 7, 1, tzinfo=timezone.utc)
        price_points = [
            PricePoint(start, 100.0, 1, 60),
            PricePoint(start + timedelta(hours=1), 110.0, 2, 60),
        ]

        self.assertEqual(2, self.handler.store_da_prices(price_points))
        self.assertEqual(2, self.handler.store_da_prices(price_points))

    def test_battery_delta_calculation_treats_zero_counters_as_valid_values(self):
        start = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
        with self.handler.transaction() as conn:
            conn.execute(
                """
                INSERT INTO battery_log
                (timestamp_utc, battery_name, energy_import_kwh, energy_export_kwh)
                VALUES (?, ?, ?, ?)
                """,
                (start.isoformat(), "battery-a", 0.0, 0.0),
            )
            conn.execute(
                """
                INSERT INTO battery_log
                (timestamp_utc, battery_name, energy_import_kwh, energy_export_kwh)
                VALUES (?, ?, ?, ?)
                """,
                ((start + timedelta(minutes=15)).isoformat(), "battery-a", 0.2, 0.1),
            )

        interval = _price_interval(start, minutes=15)
        result = self.handler.get_battery_deltas_for_intervals([interval])

        self.assertEqual(
            {"imported_kwh": 0.2, "exported_kwh": 0.1},
            result["battery-a"][start.isoformat()],
        )


if __name__ == "__main__":
    unittest.main()
