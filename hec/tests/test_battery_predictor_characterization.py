import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd

from hec.core.models import NetElectricityPriceInterval
from hec.logic_engine.battery_predictor import BatteryPredictor


def get_predictor_config():
    return {
        "batteries": [
            {
                "capacity_kwh": 5.0,
                "max_charge_W": 2000,
                "max_discharge_W": 1000,
            }
        ],
        "inverter": {
            "panel_peak_w": 4000,
            "standard_power_limit": 7000,
        },
    }


def make_price_intervals(index, buy_prices, sell_prices):
    return [
        NetElectricityPriceInterval(
            interval_start_local=ts,
            resolution_minutes=15,
            active_contract_type="dynamic",
            net_prices_eur_per_kwh={
                "dynamic": {
                    "buy": buy_price,
                    "sell": sell_price,
                }
            },
        )
        for ts, buy_price, sell_price in zip(index, buy_prices, sell_prices)
    ]


def make_plan_frame(index):
    return pd.DataFrame(
        {
            "cons_kwh": [0.25, 0.25, 0.90, 0.20],
            "solar_kwh": [1.00, 0.00, 0.00, 0.00],
            "net_kwh": [0.75, -0.25, -0.90, -0.20],
            "charge_kwh": [0.50, -0.25, -0.50, -0.20],
            "soc_pct": [59.0, 52.75, 40.25, 35.25],
            "grid_in": [0.00, 0.00, 0.40, 0.00],
            "grid_out": [0.25, 0.00, 0.00, 0.00],
        },
        index=index,
    )


class TestBatteryPredictorCharacterization(unittest.TestCase):
    def setUp(self):
        self.predictor = BatteryPredictor(get_predictor_config())

    def test_generate_plan_preserves_current_output_shape_and_discharge_curve(self):
        start = datetime(2026, 6, 28, 8, 0, tzinfo=timezone.utc)
        index = pd.date_range(start=start, periods=4, freq="15min")
        consumption = pd.Series([0.20, 0.20, 0.20, 0.20], index=index)
        db = MagicMock()
        db.get_elia_forecasts.return_value = []

        plan = self.predictor.generate_plan(
            start,
            start + timedelta(minutes=45),
            consumption,
            db,
            max_peak_kw=2.5,
            initial_soc_pct=50,
        )

        self.assertEqual(
            list(plan.columns),
            ["cons_kwh", "solar_kwh", "net_kwh", "charge_kwh", "soc_pct", "grid_in", "grid_out"],
        )
        self.assertEqual(len(plan), 4)
        self.assertEqual(plan.index.tolist(), index.tolist())
        self.assertEqual(plan["charge_kwh"].round(3).tolist(), [-0.2, -0.2, -0.2, -0.2])
        self.assertEqual(plan["soc_pct"].round(1).tolist(), [45.0, 40.0, 35.0, 30.0])
        self.assertEqual(plan["grid_in"].round(3).tolist(), [0.0, 0.0, 0.0, 0.0])

    def test_calculate_impact_characterizes_block_and_force_signals(self):
        start = datetime(2026, 6, 28, 8, 0, tzinfo=timezone.utc)
        index = pd.date_range(start=start, periods=3, freq="15min")
        df = pd.DataFrame(
            {
                "cons_kwh": [0.25, 0.25, 0.20],
                "solar_kwh": [1.00, 0.00, 0.00],
                "net_kwh": [0.75, -0.25, -0.20],
                "charge_kwh": [0.50, -0.25, -0.20],
                "grid_out": [0.25, 0.00, 0.00],
                "buy_price": [0.20, 0.20, 0.05],
                "sell_price": [0.05, 0.05, 0.02],
                "block_c": [True, False, False],
                "block_d": [False, True, False],
                "force_c": [False, False, True],
                "force_time": [0, 0, 15],
                "limit_i": [7000.0, 7000.0, 7000.0],
                "new_c": [0.0, 0.0, 0.0],
                "new_pct": [0.0, 0.0, 0.0],
                "new_grid": [0.0, 0.0, 0.0],
            },
            index=index,
        )
        self.predictor.max_peak_kw = 2.5

        impacted = self.predictor.calculate_impact(df, current_soc_kwh=2.5)

        self.assertEqual(impacted["new_c"].round(3).tolist(), [0.0, 0.0, 0.225])
        self.assertEqual(impacted["new_grid"].round(3).tolist(), [0.75, -0.25, -0.425])
        self.assertEqual(impacted["new_pct"].round(2).tolist(), [50.0, 50.0, 54.05])

    def test_calculate_impact_characterizes_negative_price_inverter_limits(self):
        start = datetime(2026, 6, 28, 8, 0, tzinfo=timezone.utc)
        index = pd.date_range(start=start, periods=2, freq="15min")
        df = pd.DataFrame(
            {
                "cons_kwh": [0.25, 0.25],
                "solar_kwh": [1.00, 1.00],
                "net_kwh": [0.75, 0.75],
                "charge_kwh": [0.50, 0.50],
                "grid_out": [0.25, 0.25],
                "buy_price": [0.20, -0.05],
                "sell_price": [-0.05, 0.05],
                "block_c": [False, False],
                "block_d": [False, False],
                "force_c": [False, False],
                "force_time": [0, 0],
                "limit_i": [7000.0, 7000.0],
                "new_c": [0.0, 0.0],
                "new_pct": [0.0, 0.0],
                "new_grid": [0.0, 0.0],
            },
            index=index,
        )

        impacted = self.predictor.calculate_impact(df, current_soc_kwh=1.0)

        self.assertEqual(impacted["limit_i"].round(0).tolist(), [3000.0, 0.0])
        self.assertEqual(impacted["new_grid"].round(3).tolist(), [0.0, 0.25])

    def test_optimize_plan_preserves_plan_columns_and_live_first_row(self):
        start = datetime(2026, 6, 28, 8, 0, tzinfo=timezone.utc)
        index = pd.date_range(start=start, periods=4, freq="15min")
        base_plan = make_plan_frame(index)
        state = {
            "electricity_prices_today": make_price_intervals(
                index,
                buy_prices=[0.20, 0.12, 0.38, -0.05],
                sell_prices=[-0.05, 0.04, 0.08, 0.03],
            ),
            "electricity_prices_tomorrow": [],
        }

        optimized = self.predictor.optimize_plan(
            base_plan,
            cur_dt=start + timedelta(minutes=5),
            actual_soc_pct=50,
            state=state,
            app_config=get_predictor_config(),
            db_handler=None,
            cur_solar_w=1200,
            cur_cons_w=800,
        )

        expected_columns = [
            "cons_kwh",
            "solar_kwh",
            "net_kwh",
            "charge_kwh",
            "soc_pct",
            "grid_in",
            "grid_out",
            "buy_price",
            "sell_price",
            "block_d",
            "block_c",
            "force_c",
            "force_time",
            "limit_i",
            "new_c",
            "new_pct",
            "new_grid",
        ]
        self.assertEqual(list(optimized.columns), expected_columns)
        self.assertEqual(optimized.index.tolist(), index.tolist())
        self.assertAlmostEqual(optimized.iloc[0]["solar_kwh"], 0.3)
        self.assertAlmostEqual(optimized.iloc[0]["cons_kwh"], 0.2)
        self.assertAlmostEqual(optimized.iloc[0]["net_kwh"], 0.1)
        self.assertEqual(optimized.iloc[0]["limit_i"], 2800.0)
        self.assertEqual(optimized.iloc[-1]["limit_i"], 0.0)


if __name__ == "__main__":
    unittest.main()
